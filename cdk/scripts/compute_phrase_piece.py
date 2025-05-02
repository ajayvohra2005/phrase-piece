import argparse
import  logging
import json
import time
import boto3
import os
from   urllib.parse import urlparse
import math
import gzip
from tempfile import NamedTemporaryFile
import signal
from collections import OrderedDict
import csv
from uuid import uuid4
import random

MAX_ATTEMPTS = 5

def retry(attempt=None, error=None):
    print(f"{error}; will retry")
    if attempt <= MAX_ATTEMPTS:
        attempt += 1
    else:
        raise error
    
    interval = random.uniform(5**(attempt - 1), 5**attempt)
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

class ComputePhrasePiece:

    NGRAM_CACHE_CAPACITY = 2**24

    def __init__(self, args):
        logging.basicConfig(
            format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("compute_phrase_piece")

        with open(args.config) as json_file:
            config = json.load(json_file)
            self.__corpora_bucket = config['corpora_bucket']
            self.__corpus_id = config['corpus_id']
            self.__batch_job_queue = config['batch_job_queue']
            self.__batch_job_definition = config['batch_job_definition']
            self.__max_length = config["max_length"]

        if args.manifest:
            self.__manifest = args.manifest
            self.__freq_cache = LRUCache(self.NGRAM_CACHE_CAPACITY)
        else:
            self.__manifest = None

        self.s3_client = boto3.client('s3')
        self.athena_client = boto3.client('athena')
        
        signal.signal(signal.SIGINT, self.__exit_gracefully)
        signal.signal(signal.SIGTERM, self.__exit_gracefully)


    def __exit_gracefully(self, signum, frame):
        self.__abort(f"Received {signum} signal")

    def __get_ngram_frequency(self, ngram=None, n=None):

        ngram_escaped = ngram.replace("'", "''")
        ngram_freq = self.__freq_cache.get(key=ngram)

        if ngram_freq is None:
            params = dict()
            params['database'] = self.__corpus_id
            params['bucket'] = self.__corpora_bucket
            params['path'] = f"{self.__corpus_id}/athena/output/"
            params['query'] = f"""
                select ngram, ngram_freq from ngram_freq_reduce 
                where ngram_len = {n}
                and ngram = '{ngram_escaped}'
                """
            
            s3_uri = self.athena_sync(client=self.athena_client, params=params)
            s3_uri_parse = urlparse(s3_uri)
            obj = self.s3_client.get_object(Bucket=s3_uri_parse.netloc, Key=s3_uri_parse.path[1:]) 
            data = obj['Body'].read().decode('utf-8').splitlines() 
            records = csv.reader(data) 
            next(records) 
            record = next(records)
            assert record[0] == ngram
            ngram_freq = int(record[1])
            self.__freq_cache.put(key=ngram, value=ngram_freq)

        return ngram_freq

    
    def __compute_phrase_piece(self, n=None, ngram=None, ngram_contrib=None):
        
        ngram_tokens = ngram.split()
        denom = 0.0

        ngram_frequency = self.__get_ngram_frequency(ngram=ngram, n=n)
        for _1gram in ngram_tokens:
            _1gram_frequency = self.__get_ngram_frequency(ngram=_1gram, n=1)
            
            if _1gram_frequency >= ngram_frequency:
                denom += math.log(_1gram_frequency)
            else:
                raise RuntimeError(f"1gram frequency is less than ngram frequency {ngram}: {ngram_frequency}, {_1gram}: {_1gram_frequency}")

        mean_ngram_contrib = ngram_contrib/ngram_frequency
        phrase_piece = (math.log(n) + 1)*math.log(ngram_frequency) - denom/n + math.log(mean_ngram_contrib)
        return phrase_piece

    def __pump_freq_cache(self, n=None):
        bucket_prefix = f"{self.__corpus_id}/ngrams/cache/ngram_len={n}/"

        start = time.time()
        count = 0
        
        for key in s3_bucket_keys(s3_client=self.s3_client, bucket_name=self.__corpora_bucket, bucket_prefix=bucket_prefix):
        
            with NamedTemporaryFile(mode='w+b', delete=True) as file_obj:
                self.s3_client.download_fileobj(self.__corpora_bucket, key, file_obj)
                file_obj.seek(0)

                with gzip.open(file_obj, mode="r") as gzip_obj:
                    while (json_str := gzip_obj.readline()):
                        json_obj = json.loads(json_str)
                        full = self.__freq_cache.put(json_obj['ngram'], json_obj['ngram_freq'])
                        assert not full
                        count += 1

                    gzip_obj.close()
                file_obj.close()

            throughput = count / ( time.time() - start)
            self.logger.info(f"Pumping freq cache, size: {count}, throughput: {throughput} n-grams/sec ")
    

    def __upload_score_file_obj(self, file_obj=None, n=None):

        attempt = 0
        file_name = str(uuid4())
        key = f"{self.__corpus_id}/ngrams/score/ngram_len={n}/{file_name}.gz"
        
        while True:
            try:
                file_obj.seek(0)
                self.logger.info(f"upload score file: s3://{self.__corpora_bucket}/{key}")
                self.s3_client.put_object(Body=file_obj, Bucket=self.__corpora_bucket, Key=key)
                break
            except Exception as e:
                attempt = retry(attempt=attempt, error=e)

    def __process_manifest(self):    
        self.logger.info(f"processing manifest file: {self.__manifest}")  
        start = time.time()

        self.__pump_freq_cache(n=1)

        s3_uri_parse = urlparse(self.__manifest)
        bucket=s3_uri_parse.netloc
        key=s3_uri_parse.path[1:]
        ngram_len = int(key.rsplit("/", 2)[-2].split('=')[1])
        if ngram_len > 1:
            self.__pump_freq_cache(n=ngram_len)
        
        with NamedTemporaryFile(mode='w+b', delete=True) as score_file_obj:
            with gzip.open(score_file_obj, mode="wb") as score_gzip_obj:
                with NamedTemporaryFile(mode='w+b', delete=True) as manifest_file_obj:
                    self.s3_client.download_fileobj(bucket, key, manifest_file_obj)
                    manifest_file_obj.seek(0)

                    with gzip.open(manifest_file_obj, mode="r") as manifest_gzip_obj:
                        while (json_str := manifest_gzip_obj.readline()):
                            json_obj = json.loads(json_str)
                            ngram = json_obj['ngram']
                            ngram_contrib = json_obj['ngram_contrib']

                            phrase_piece = self.__compute_phrase_piece(n=ngram_len, ngram=ngram, ngram_contrib=ngram_contrib)
                            json_obj = {
                                "ngram": ngram,
                                "phrase_piece": float(phrase_piece)
                            }
                            json_str = json.dumps(json_obj) + "\n"
                            score_gzip_obj.write(json_str.encode("utf-8"))

                        manifest_gzip_obj.close()
                    manifest_file_obj.close()
                score_gzip_obj.close()

            self.__upload_score_file_obj(file_obj=score_file_obj, n=ngram_len)
            score_file_obj.close()
        

        elapsed = time.time() - start
        self.logger.info(f"process_manifest elapsed time: {elapsed}")
        self.logger.info(f"LRU cache 1 hit ratio: {self.__freq_cache.hit_ratio()}")
                        
    def __submit_batch_jobs(self, manifests):
        
        self.batch_client = boto3.client('batch')
        aws_region = os.environ['AWS_DEFAULT_REGION']
        s3_json_config = os.environ['S3_JSON_CONFIG']
        s3_python_script = os.environ['S3_PYTHON_SCRIPT']
        
        self.__jobs=dict()
        for manifest in manifests:
            job_name = f"cns-{self.__corpus_id}"
            response = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.__batch_job_queue,
                jobDefinition=self.__batch_job_definition,
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
        prefix = f"{self.__corpus_id}/ngrams/scored/"
        for key in s3_bucket_keys(self.s3_client, bucket_name=self.__corpora_bucket, bucket_prefix=prefix):
            manifests.append(f"s3://{self.__corpora_bucket}/{key}")
        
        return manifests

    def __process_manifests(self):
        manifests = self.__get_manifests()
        if len(manifests) > 1 and self.__batch_job_queue and self.__batch_job_definition:
            self.__submit_batch_jobs(manifests)
        else:
            for manifest in manifests:
                self.__manifest = manifest
                self.__process_manifest()
    
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

    def __unload_ngrams_freq_cache(self, n=None, capacity=None):
        unload_location = f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/cache/ngram_len={n}/' 
        params = dict()
        params['database'] = self.__corpus_id
        params['bucket'] = self.__corpora_bucket
        params['path'] = f"{self.__corpus_id}/athena/output/"
        params['query'] = f"unload (select ngram, ngram_freq from ngram_freq_reduce where ngram_len = {n} ORDER BY ngram_freq DESC LIMIT {capacity} ) TO '{unload_location}' WITH (format = 'JSON')"
        
        self.athena_sync(client=self.athena_client, params=params)

    def __unload_ngrams_scored(self, n=None):
        unload_location = f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/scored/ngram_len={n}/' 
        params = dict()
        params['database'] = self.__corpus_id
        params['bucket'] = self.__corpora_bucket
        params['path'] = f"{self.__corpus_id}/athena/output/"
        params['query'] = f"unload (select ngram, ngram_contrib from ngram_contrib_reduce where ngram_len = {n} ) TO '{unload_location}' WITH (format = 'JSON')"
        
        self.athena_sync(client=self.athena_client, params=params)

    def __create_phrase_piece_table(self):
        s3_storage_location =  f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/score' 
        params = dict()
        params['database'] = self.__corpus_id
        params['query'] = f"""
            CREATE EXTERNAL TABLE phrase_piece ( 
            ngram string,
            ngram_freq int,
            phrase_piece float )
            PARTITIONED BY ( ngram_len int )
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

        params['query'] = "MSCK REPAIR TABLE phrase_piece"
        self.athena_sync(client=self.athena_client, params=params)

    def __call__(self):
       
        if self.__manifest:
            self.__process_manifest()
        else:
            capacity = int(self.NGRAM_CACHE_CAPACITY/self.__max_length)
            for n in range(1, self.__max_length+1, 1):
                self.__unload_ngrams_freq_cache(n=n, capacity=capacity)
                self.__unload_ngrams_scored(n=n)
            self.__process_manifests()
            self.__create_phrase_piece_table()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Compute ngram score')

    parser.add_argument('--config', type=str,  help='Create corpus config file', required=True)
    parser.add_argument('--manifest', type=str, default="", help='S3 URI for manifest part', required=False)
    args, _ = parser.parse_known_args()
    compute_phrase_piece = ComputePhrasePiece(args)
    compute_phrase_piece()