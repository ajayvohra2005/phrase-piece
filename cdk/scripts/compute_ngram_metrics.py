import argparse
import  logging
import json
import time
import boto3
import os
from   urllib.parse import urlparse
import re
import gzip
from tempfile import NamedTemporaryFile
from sys import getsizeof
import signal
from collections import OrderedDict
import random
from uuid import uuid4
import numpy as np
import torch
from sentence_transformers import SentenceTransformer


from transformers import (
    AutoTokenizer,
    AutoModelForTokenClassification,
    TokenClassificationPipeline
)

MAX_ATTEMPTS = 5

def retry(attempt=None, error=None):
    print(f"{error}; will retry")
    if attempt <= MAX_ATTEMPTS:
        attempt += 1
    else:
        raise error
    
    interval = random.uniform(2**(attempt - 1), 2**attempt)
    time.sleep(interval)
    return attempt

def s3_bucket_keys(s3_client, bucket_name, bucket_prefix):
    """Generator for listing S3 bucket keys matching prefix"""

    kwargs = {'Bucket': bucket_name, 'Prefix': bucket_prefix}
    while True:
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            yield obj['Key']

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
             
class LRUCache(OrderedDict):
 
    def __init__(self, capacity):
        super(OrderedDict, self).__init__()
        self.capacity = capacity
        self.__hit = 0
        self.__miss = 0
 
    def hit_ratio(self):
        total = self.__hit + self.__miss
        hit_ratio = None
        if total:
            hit_ratio = float(self.__hit/total)
        
        return hit_ratio

    def get(self, key, default=None):
        if key not in self:
            self.__miss += 1
            return default
        else:
            self.__hit += 1
            self.move_to_end(key)
            return self[key]
 
    def put(self, key, value, evict=True):
          
        if evict:
            self[key] = value
            self.move_to_end(key)
            if len(self) > self.capacity:
                return self.popitem(last = False)
            else:
                return None
        elif key in self or len(self) < self.capacity:
            self[key] = value
            return None
        else:
            return (key, value)


class ComputeNgramMetrics:
    MAX_LENGTH = 10
    MAX_WORD_SEQ_LEN = 256
    POS_PATTERN = re.compile(r"^(((PUNCT)((PROPN)|(NOUN)|(NUM)|(PUNCT))+(PUNCT))|(((PROPN)|(NOUN)|(NUM)|(ADJ)|(VERB))*((PROPN)|(NOUN)|(NUM))+))$")

    MAX_ATTEMPTS = 4
    NGRAM_CACHE_CAPACITY = 2**24
    LOG_INTERVAL = 100000

    OPEN = ["[","{","("]
    CLOSE = ["]","}",")"]


    def __init__(self, args):
        logging.basicConfig(
            format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("compute_ngram_metrics")

        with open(args.config) as json_file:
            config = json.load(json_file)
            self.__corpora_bucket = config['corpora_bucket']
            self.__corpus_id = config['corpus_id']
            self.__batch_job_queue = config['batch_job_queue']
            self.__cpu_inference_batch_job_definition = config['cpu_inference_batch_job_definition']
            self.__max_length = config.get('max_length', self.MAX_LENGTH)
            self.__pos_pattern = config.get('pos_pattern', None)
            self.__language_code = config.get("language_code", "en")
           
        if args.manifest:
            self.__manifest = args.manifest
        else:
            self.__manifest = None

        self.s3_client = boto3.client('s3')
        self.athena_client = boto3.client('athena')

        self.__n_values = set()
        self.__n_values.add(1)

        self.__freq_cache = LRUCache(self.NGRAM_CACHE_CAPACITY)
        self.__contrib_cache = LRUCache(self.NGRAM_CACHE_CAPACITY)

        self.__files = { "freq": dict(), "contrib": dict()}
        self.__gzips = { "freq": dict(), "contrib": dict()}
        
        if self.__pos_pattern:
            self.__pos_pattern = re.compile(self.__pos_pattern)
        else:
            self.__pos_pattern = self.POS_PATTERN

        self.__create_models()

        signal.signal(signal.SIGINT, self.__exit_gracefully)
        signal.signal(signal.SIGTERM, self.__exit_gracefully)
        
    def __get_gzip(self, name, n):
        gzip_file = self.__gzips[name].get(n, None)
        if gzip_file is None:
            file = NamedTemporaryFile(mode='w+b', delete=True)
            gzip_file = gzip.open(file, mode="wb")
            self.__files[name][n] = file
            self.__gzips[name][n] = gzip_file
           
        return gzip_file

    def __close_gzip(self, name, n):
        gzip_archive = self.__gzips[name].get(n, None)
        if gzip_archive:
            gzip_archive.close()

    def __close_file(self, name, n):
        file = self.__files[name].get(n, None)
        if file:
            file.close()
    
    def __create_models(self):
        attempt = 0
        self.logger.info("Initializing models")
        while True:
            try:
                self.__pos_tokenizer = AutoTokenizer.from_pretrained(f"wietsedv/xlm-roberta-base-ft-udpos28-{self.__language_code}")
                self.__pos_model = AutoModelForTokenClassification.from_pretrained(f"wietsedv/xlm-roberta-base-ft-udpos28-{self.__language_code}")
                self.__pos_pipeline = TokenClassificationPipeline(model=self.__pos_model, tokenizer=self.__pos_tokenizer)
                self.__st_model = SentenceTransformer('distiluse-base-multilingual-cased-v2')

                break
            except Exception as error:
                self.logger.warning(error)
                attempt = retry(attempt=attempt, error=error)
        self.logger.info("All models initalized")

    @classmethod
    def get_text_segments(cls, contexts):
        segments = []
        segment = ""
        segment_word_count = 0

        for context in contexts:
            context_words = context.split()
            context_word_count = len(context_words)
            if context_word_count + segment_word_count <= cls.MAX_WORD_SEQ_LEN:
                segment = f"{segment}{context}. "
                segment_word_count += context_word_count
            else:
                segments.append(segment)
                segment = f"{context}. "
                segment_word_count = context_word_count

        if segment:
            segments.append(segment)

        return segments
    
    def __compute_segment_embeddings(self, segments=None):
        self.__segment_embeddings = self.__st_model.encode(sentences=segments)
        self.__segment_embeddings  =  self.__segment_embeddings/np.linalg.norm(self.__segment_embeddings, axis=-1, keepdims=True)
        
                
    def __get_candidate_contrib(self, candidate=None):
        contrib = 0.0

        candidate_embedding = self.__st_model.encode([candidate])
        candidate_embedding  =  candidate_embedding/np.linalg.norm(candidate_embedding, axis=-1, keepdims=True)
        
        contrib = max(float(np.mean(np.dot( self.__segment_embeddings, candidate_embedding.T))), 0.0)

        return contrib
    
    @classmethod
    def mean_embeddings(cls, output, attention_mask):
        final_hidden_states = output['hidden_states'][-1]
        attention_mask = attention_mask.unsqueeze(-1).expand(final_hidden_states.size()).float()
        embeddings = torch.sum(final_hidden_states * attention_mask, 1) / torch.clamp(attention_mask.sum(1), min=1e-8)
        return embeddings.detach()

    def __matches_pos_pattern(self, s):
        pos = self.__encode_pos(s)
        m = self.__pos_pattern.match(pos)
        return m

    def __encode_pos(self, text):
        text=text.encode('utf-8', 'ignore').decode('utf-8')
        outputs = self.__pos_pipeline(text)

        pos = []
        for i in range(len(outputs)):
            entity = outputs[i]['entity']
            pos.append(f"{entity}")
        
        return "".join(pos)

    def __find_candidates(self, context):
        
        context = re.sub(r"http[s]?://[^\s]+", "", context)
        candidates = list()
        _1grams = list()
        context_words = context.split()
        num_context_words = len(context_words)
        for start in range(num_context_words):
            sub_context_words = context_words[start: min(start + self.__max_length, num_context_words)]
           
            num_sub_context_words = len(sub_context_words)
            for n in range(num_sub_context_words, 0, -1):
                phrase_words = sub_context_words[0: n]
                phrase = " ".join(phrase_words) 
                if self.is_wellformed(phrase) and self.__matches_pos_pattern(phrase):
                    candidates.append( (phrase, n))

            _1grams.append(sub_context_words[0])

        return _1grams, candidates
    
    def __upload_gzip(self, name=None, n=None):
        attempt = 0
        file_name = str(uuid4())
        key = f"{self.__corpus_id}/ngrams/{name}/map/ngram_len={n}/{file_name}.gz"
        
        while True:
            try:
                self.logger.info(f"upload gzip: s3://{self.__corpora_bucket}/{key}")
                file = self.__files[name].get(n, None)
                if file:
                    file.seek(0)
                    self.s3_client.put_object(Body=file, Bucket=self.__corpora_bucket, Key=key)
                break
            except Exception as e:
                attempt = retry(attempt=attempt, error=e)

    def __write_ngram_freq(self, ngram=None, freq=None, n=None):
        json_obj = {
            "ngram": ngram,
            "freq": freq
        }
        json_str = json.dumps(json_obj) + "\n"
        self.__get_gzip("freq", n).write(json_str.encode("utf-8"))

    def __write_ngram_contrib(self, ngram=None, contrib=None, n=None):
        json_obj = {
            "ngram": ngram,
            "contrib": contrib
        }
        json_str = json.dumps(json_obj) + "\n"
        self.__get_gzip("contrib", n).write(json_str.encode("utf-8"))

    def __flush_freq_cache(self):
        total = len(self.__freq_cache)
        count = 0
        
        self.logger.info(f"flushing freq cache: {total} items, {getsizeof(self.__freq_cache)} bytes")
        start = time.time()
        
        for key, value in self.__freq_cache.items():
            freq, n = value
            self.__write_ngram_freq(ngram=key, freq=freq, n=n)

            count += 1
            if count % self.LOG_INTERVAL == 0:
                throughput = count/(time.time() - start)
                eta = (total - count)/throughput
                self.logger.info(f"flushed {count} ngrams, throughput: {throughput} ngrams/sec, eta: {eta} secs")
        self.__freq_cache.clear()
        self.logger.info(f"Flushed freq cache in {time.time() - start} seconds")
        self.logger.info(f"Freq cache hit ratio: {self.__freq_cache.hit_ratio()}")

        for n in self.__n_values:
            self.__close_gzip("freq", n)
            self.__upload_gzip("freq", n)
            self.__close_file("freq", n)
  

    def __flush_contrib_cache(self):
        total = len(self.__contrib_cache)
        count = 0
        
        self.logger.info(f"flushing contrib cache: {total} items, {getsizeof(self.__contrib_cache)} bytes")
        start = time.time()

        for key, value in self.__contrib_cache.items():
            contrib, n = value
            self.__write_ngram_contrib(ngram=key, contrib=contrib, n=n)

            count += 1
            if count % self.LOG_INTERVAL == 0:
                throughput = count/(time.time() - start)
                eta = (total - count)/throughput
                self.logger.info(f"flushed {count} ngrams, throughput: {throughput} ngrams/sec, eta: {eta} secs")
        self.__contrib_cache.clear()
        self.logger.info(f"Flushed contrib cache in {time.time() - start} seconds")
        self.logger.info(f"Contrib cache hit ratio: {self.__contrib_cache.hit_ratio()}")

        for n in self.__n_values:
            self.__close_gzip("contrib", n)
            self.__upload_gzip("contrib", n)
            self.__close_file("contrib", n)

    def __increment_ngram_freq(self, n=None, ngram=None):
        freq, n = self.__freq_cache.get(ngram, (0, n))
        freq += 1

        flush_item = self.__freq_cache.put(ngram, (freq, n))
        if flush_item:
            key, value = flush_item
            freq, n = value
            self.__write_ngram_freq(ngram=key, freq=freq, n=n)

    def __increment_ngram_contrib(self, n=None, ngram=None, score=None):
        contrib, n = self.__contrib_cache.get(ngram, (0.0, n))
        contrib += score

        flush_item = self.__contrib_cache.put(ngram, (contrib, n))
        if flush_item:
            key, value = flush_item
            contrib, n = value
            self.__write_ngram_contrib(ngram=key, contrib=contrib, n=n)


    @staticmethod
    def get_contexts(text):
        contexts=[]
        lines = text.splitlines()
        for line in lines:
            contexts.extend(re.split("[.]\s+", line))
        
        contexts = list(filter(lambda x: x, contexts))
        contexts = [ re.sub(r'[.]$', '', st) for st in contexts ]
        return contexts

    def __process_manifest(self):    
        self.logger.info(f"processing manifest file: {self.__manifest}")  
        start = time.time()
    
        manifest = self.__download_manifest()

        self.__scored_candidates = dict()

        with open(manifest, "rb") as file_obj:
            file_obj.seek(0)
            with gzip.open(file_obj, mode = "r") as gzip_obj:
                while (line := gzip_obj.readline()):
                    json_obj=json.loads(line)
                    text = json_obj.get('text')
                    contexts = self.get_contexts(text)
                    segments = self.get_text_segments(contexts=contexts)
                    self.__compute_segment_embeddings(segments=segments)

                    for context in contexts:
                        self.__extract_ngrams(context=context)
                    
                gzip_obj.close()
            file_obj.close()

        self.__flush_freq_cache()
        self.__flush_contrib_cache()

        elapsed = time.time() - start
        self.logger.info(f"process_manifest elapsed time: {elapsed}")
    
    def __extract_ngrams(self, context=None):
        self.logger.info(f"Extract ngrams from: {context}")

        _1grams, candidates = self.__find_candidates(context)
        for _1gram in _1grams:
            self.__increment_ngram_freq(n=1, ngram=_1gram)

        for candidate, n in candidates:
            candidate_scored = True

            if candidate not in self.__scored_candidates:
                score = self.__get_candidate_contrib(candidate=candidate)
                self.__scored_candidates[candidate] = score
                self.logger.info(f"Candidate contribution: {candidate}: {score}")
            else:
                score = self.__scored_candidates[candidate]
                candidate_scored = False

            if score > 0.0:
                self.__n_values.add(n)
                if n > 1: # freq for all 1-grams regardless of score is incremented above
                    self.__increment_ngram_freq(n=n, ngram=candidate)
                if candidate_scored:
                    self.__increment_ngram_contrib(n=n, ngram=candidate, score=score)
    

    @classmethod
    def is_balanced(cls, s, left='(', right=')'):
        count = 0

        for i in range(len(s)):
            if (s[i] == left):
                count += 1
            elif (s[i] == right):
                count -= 1

            if count < 0:
                break
    
        return count == 0

    @classmethod
    def is_wellformed(cls, s):
        well_formed =  cls.is_balanced(s) and cls.is_balanced(s, left='[',  right=']')
        well_formed = well_formed and cls.is_balanced(s, left='\u201c',  right='\u201d')
        well_formed = well_formed and (len(re.findall(r"'", s)) % 2 == 0) and ( len(re.findall(r'"', s)) % 2 == 0)
        well_formed = well_formed and not re.match(r'.+"\s+".+', s) and not re.match(r".+'\s+'.+", s)
        return well_formed
    
    @staticmethod
    def athena_query(client=None, params=None):
        response = client.start_query_execution(
            QueryString=params["query"],
            QueryExecutionContext={
                'Database': params['database']
            },
            ResultConfiguration={
                'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
            }
        )
        return response

    @classmethod
    def athena_sync(cls, client=None, params=None):
        execution = cls.athena_query(client=client, params=params)
        execution_id = execution['QueryExecutionId']
        state = 'RUNNING'

        while ( state in ['RUNNING', 'QUEUED']):
            response = client.get_query_execution(QueryExecutionId = execution_id)
            print(f"{response}")
            if 'QueryExecution' in response and \
                    'Status' in response['QueryExecution'] and \
                    'State' in response['QueryExecution']['Status']:
                state = response['QueryExecution']['Status']['State']
                if state == 'FAILED':
                    raise RuntimeError(str(response))
                elif state == 'SUCCEEDED':
                    return  response['QueryExecution']['ResultConfiguration']['OutputLocation']
            time.sleep(2)


    def __unload_ngram_freq_reduce(self, n=None):
        unload_location =  f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/freq/reduce/ngram_len={n}/' 
        params = dict()
        params['database'] = self.__corpus_id
        params['bucket'] = self.__corpora_bucket
        params['path'] = f"{self.__corpus_id}/athena/output/"
        params['query'] = f"unload (select ngram, sum(freq) as ngram_freq from ngram_freq_map where ngram_len = {n} group by ngram) TO '{unload_location}' WITH (format = 'PARQUET')"
        
        self.athena_sync(client=self.athena_client, params=params)

    
    def __create_ngram_freq_map_table(self):
        s3_storage_location =  f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/freq/map' 
        params = dict()
        params['database'] = self.__corpus_id
        params['query'] = f"""
            CREATE EXTERNAL TABLE ngram_freq_map ( 
            ngram string,
            freq int)
            PARTITIONED BY ( ngram_len int)
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            LOCATION '{s3_storage_location}/' 
            TBLPROPERTIES (
                'storage.location.template'='{s3_storage_location}/ngram_len=${{ngram_len}}/',
                'compressionType'='gzip'
            )
            """
        params['bucket'] = self.__corpora_bucket
        params['path'] = f"{self.__corpus_id}/athena/output/"

        self.athena_sync(client=self.athena_client, params=params)

        params['query'] = "MSCK REPAIR TABLE ngram_freq_map"
        self.athena_sync(client=self.athena_client, params=params)

    def __create_ngram_freq_reduce_table(self):
        s3_storage_location =  f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/freq/reduce' 
        params = dict()
        params['database'] = self.__corpus_id
        params['query'] = f"""
            CREATE EXTERNAL TABLE ngram_freq_reduce ( 
            ngram string,
            ngram_freq int)
            PARTITIONED BY ( ngram_len int)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
            OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION '{s3_storage_location}/' 
            TBLPROPERTIES (
                'storage.location.template'='{s3_storage_location}/ngram_len=${{ngram_len}}/'
            )
            """
        params['bucket'] = self.__corpora_bucket
        params['path'] = f"{self.__corpus_id}/athena/output/"

        self.athena_sync(client=self.athena_client, params=params)

        params['query'] = "MSCK REPAIR TABLE ngram_freq_reduce"
        self.athena_sync(client=self.athena_client, params=params)


    def __unload_ngram_contrib_reduce(self, n=None):
        unload_location =  f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/contrib/reduce/ngram_len={n}/' 
        params = dict()
        params['database'] = self.__corpus_id
        params['bucket'] = self.__corpora_bucket
        params['path'] = f"{self.__corpus_id}/athena/output/"
        params['query'] = f"unload (select ngram, sum(contrib) as ngram_contrib from ngram_contrib_map where ngram_len = {n} group by ngram) TO '{unload_location}' WITH (format = 'PARQUET')"
        
        self.athena_sync(client=self.athena_client, params=params)

    
    def __create_ngram_contrib_map_table(self):
        s3_storage_location =  f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/contrib/map' 
        params = dict()
        params['database'] = self.__corpus_id
        params['query'] = f"""
            CREATE EXTERNAL TABLE ngram_contrib_map ( 
            ngram string,
            contrib float)
            PARTITIONED BY ( ngram_len int)
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            LOCATION '{s3_storage_location}/' 
            TBLPROPERTIES (
                'storage.location.template'='{s3_storage_location}/ngram_len=${{ngram_len}}/',
                'compressionType'='gzip'
            )
            """
        params['bucket'] = self.__corpora_bucket
        params['path'] = f"{self.__corpus_id}/athena/output/"

        self.athena_sync(client=self.athena_client, params=params)

        params['query'] = "MSCK REPAIR TABLE ngram_contrib_map"
        self.athena_sync(client=self.athena_client, params=params)

    def __create_ngram_contrib_reduce_table(self):
        s3_storage_location =  f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/contrib/reduce' 
        params = dict()
        params['database'] = self.__corpus_id
        params['query'] = f"""
            CREATE EXTERNAL TABLE ngram_contrib_reduce ( 
            ngram string,
            ngram_contrib float)
            PARTITIONED BY ( ngram_len int)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
            OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
            LOCATION '{s3_storage_location}/' 
            TBLPROPERTIES (
                'storage.location.template'='{s3_storage_location}/ngram_len=${{ngram_len}}/'
            )
            """
        params['bucket'] = self.__corpora_bucket
        params['path'] = f"{self.__corpus_id}/athena/output/"

        self.athena_sync(client=self.athena_client, params=params)

        params['query'] = "MSCK REPAIR TABLE ngram_contrib_reduce"
        self.athena_sync(client=self.athena_client, params=params)

    def __exit_gracefully(self, signum, frame):
        self.__abort(f"Received {signum} signal")

    def __download_manifest(self):
        self.logger.info(f"downloading manifest file: {self.__manifest}")
        s3_uri_parse = urlparse(self.__manifest)
        manifest_key = s3_uri_parse.path[1:]

        with NamedTemporaryFile(mode='w+b', delete=False) as file_obj:
            self.s3_client.download_fileobj(s3_uri_parse.netloc, manifest_key, file_obj)

        return file_obj.name
    
    def __submit_batch_jobs(self, manifests):

        self.batch_client = boto3.client('batch')
        aws_region = os.environ['AWS_DEFAULT_REGION']
        s3_json_config = os.environ['S3_JSON_CONFIG']
        s3_python_script = os.environ['S3_PYTHON_SCRIPT']
       
        self.__jobs=dict()
        for manifest in manifests:
            self.logger.info(f"Submit batch job to process manaifest: {manifest}")
            job_name = f"cnm-{self.__corpus_id}"
            response = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.__batch_job_queue,
                jobDefinition=self.__cpu_inference_batch_job_definition,
                retryStrategy={'attempts': 1},
                timeout={'attemptDurationSeconds': 86400},
                containerOverrides={
                    'command': ['--manifest', f'{manifest}'],
                    'environment': [
                        {
                            'name': 'S3_PYTHON_SCRIPT',
                            'value': s3_python_script
                        },
                        {
                            'name': 'S3_JSON_CONFIG',
                            'value': s3_json_config
                        },
                        {
                            'name': 'AWS_DEFAULT_REGION',
                            'value': aws_region
                        }
                    ]
                })
            job_id = response["jobId"]
            self.__jobs[job_id] = manifest

        self.__pending=[ job_id for job_id in self.__jobs.keys() ]

        while self.__pending:
            pending_jobs = []
            for i in range(0, len(self.__pending), 100):

                jobs_slice = self.__pending[i:i+100]
                if jobs_slice:
                    response = self.batch_client.describe_jobs(jobs=jobs_slice)
                    
                    for _job in response["jobs"]:
                        job_id = _job['jobId']
                        if _job["status"] == 'FAILED':
                            reason = f'Job failed: {job_id}'
                            self.__abort(reason)
                        elif _job['status'] != 'SUCCEEDED':
                            pending_jobs.append(job_id)
            
            self.__pending = pending_jobs

            time.sleep(60)
    
    def __abort(self, reason):
        for job_id in self.__jobs.keys():
            try:
                self.batch_client.terminate_job(jobId=job_id, reason=reason)
            except Exception as e:
                self.logger.warning(f"ignoring {e}")
        
        raise RuntimeError(reason)


    def __get_manifests(self):
        manifests = []
        prefix = f"{self.__corpus_id}/athena/manifest/"
        for key in s3_bucket_keys(self.s3_client, bucket_name=self.__corpora_bucket, bucket_prefix=prefix):
            manifests.append(f"s3://{self.__corpora_bucket}/{key}")
        
        return manifests
    
    def __process_manifests(self):
        manifests = self.__get_manifests()
        if len(manifests) > 1 and self.__batch_job_queue and self.__cpu_inference_batch_job_definition:
            self.__submit_batch_jobs(manifests)
        else:
            for manifest in manifests:
                self.__manifest = manifest
                self.__process_manifest()
    

    def __get_freq_map_nvalues(self):
        n_values = set()

        bucket_prefix = f"{self.__corpus_id}/ngrams/freq/map/"
        for key in s3_bucket_keys(s3_client=self.s3_client, 
                                  bucket_name=self.__corpora_bucket, 
                                  bucket_prefix=bucket_prefix):
            m=re.match(r".+\/ngram_len=(\w+)\/", key)
            if m:
                n = int(m[1])
                n_values.add(n)

        return n_values
    
    def __call__(self):
       
        if self.__manifest:
            self.__process_manifest()
        else:
            self.logger.info("Processing manifests")
            self.__process_manifests()

            self.__create_ngram_freq_map_table()
            self.__create_ngram_contrib_map_table()
            map_nvalues = self.__get_freq_map_nvalues()
            for n in map_nvalues:
                self.__unload_ngram_freq_reduce(n)
                self.__unload_ngram_contrib_reduce(n)
            self.__create_ngram_freq_reduce_table()
            self.__create_ngram_contrib_reduce_table()
            
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Compute ngram metrics')

    parser.add_argument('--config', type=str,  help='Create corpus config file', required=True)
    parser.add_argument('--manifest', type=str, default="", help='S3 URI for manifest part', required=False)
    args, _ = parser.parse_known_args()
    compute_ngram_metrics = ComputeNgramMetrics(args)
    compute_ngram_metrics()