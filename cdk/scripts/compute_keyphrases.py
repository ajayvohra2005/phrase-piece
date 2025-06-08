import argparse
import logging
import json
import math
import os
from uuid import uuid4
import boto3
import time
import random
import gzip
from tempfile import NamedTemporaryFile
from sentence_transformers import SentenceTransformer
from sklearn.cluster import AgglomerativeClustering
import numpy as np
import networkx as nx

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

def put_object(s3_client=None, bucket=None, key=None, object=None):
    attempt = 0
    while True:
        try:
            s3_client.put_object( Bucket=bucket, Key=key, Body=json.dumps(object))
            break
        except Exception as e:
            attempt = retry(attempt=attempt, error=e)

class ComputeKeyphrases:

    def __init__(self, args):
        logging.basicConfig(
            format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("compute_keyphrases")

        with open(args.config) as json_file:
            config = json.load(json_file)
            self.__corpus_id = config['corpus_id']
            self.__corpora_bucket = config['corpora_bucket']
            self.__metric = config['metric']
            self.__distance_threshold = config['distance_threshold']
            self.__top_k = config['top_k']
            self.__tag = config['tag']

            self.s3_client = boto3.client("s3")
            self.athena_client = boto3.client("athena")


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
    
    def __unload_ngrams_topk(self):
        unload_location = f's3://{self.__corpora_bucket}/{self.__corpus_id}/ngrams/topk/' 
        params = dict()
        params['database'] = self.__corpus_id
        params['bucket'] = self.__corpora_bucket
        params['path'] =  f"{self.__corpus_id}/athena/output/"
        params['query'] = f"unload (select ngram, phrase_piece from phrase_piece ORDER BY phrase_piece DESC LIMIT {self.__top_k}) TO '{unload_location}' WITH (format = 'JSON')"
        self.athena_sync(client=self.athena_client, params=params)

    @staticmethod
    def find_best_sample(samples:list, sample_embeddings:dict, personalization:dict=None):
        G = nx.Graph()
    
        V = [ (i, {"sample": sample }) for i, sample in enumerate(samples) ]
        W = {  i : personalization[sample] for i,sample in enumerate(samples) }

        G.add_nodes_from(V)

        E = []
        num_sample_nodes = len(V)
        for i in range(0, num_sample_nodes):
            for j in range(i+1, num_sample_nodes):
                sample_i = V[i][1]['sample']
                sample_j = V[j][1]['sample']
                edge_weight = np.mean(np.dot(sample_embeddings[sample_i], sample_embeddings[sample_j].T))
                E.append( (i, j, { 'weight': edge_weight}) )
            
        G.add_edges_from(E)

        pr = nx.pagerank(G, weight='weight', personalization=W)
        pr_list = [ (k, v) for k,v in pr.items() ]
        pr_list.sort(key=lambda x: x[1], reverse=True)
        best_node = pr_list[0][0]
        best_sample = V[best_node][1]['sample']
        return best_sample
    
    @classmethod
    def fit_agg_cluster(cls, st_model=None, samples=None, metric="cosine", distance_threshold=0.4):
        embeddings = cls.normalized_embedding(samples, st_model)
        cluster = AgglomerativeClustering(n_clusters=None, compute_full_tree=True,
             metric=metric, linkage='average', distance_threshold=distance_threshold) 
        cluster.fit(embeddings)

        sample_embeddings = { samples[i]: embeddings[i] for i in range(len(samples))}
        return sample_embeddings, cluster
    
    def __get_top_ngrams(self):
        ngrams = dict()

        self.__unload_ngrams_topk()
        
        bucket_prefix = f"{self.__corpus_id}/ngrams/topk/"
        for key in s3_bucket_keys(s3_client=self.s3_client, bucket_name=self.__corpora_bucket, bucket_prefix=bucket_prefix):
        
            with NamedTemporaryFile(mode='w+b', delete=True) as file_obj:
                self.s3_client.download_fileobj(self.__corpora_bucket, key, file_obj)
                file_obj.seek(0)

                with gzip.open(file_obj, mode="r") as gzip_obj:
                    while (json_str := gzip_obj.readline()):
                        json_obj = json.loads(json_str)
                        ngram = json_obj['ngram']
                        phrase_piece = json_obj['phrase_piece']

                        ngrams[ngram] = phrase_piece
                    gzip_obj.close()

                file_obj.close()

        return ngrams

    @classmethod
    def normalized_embedding(cls, sample, st_model):
        sample_embedding = st_model.encode(sample)
        normalized_sample_embedding =  sample_embedding/np.linalg.norm(sample_embedding, axis=-1, keepdims=True)
        return normalized_sample_embedding

    @staticmethod
    def copy_s3_folder_to_local(s3_client, bucket:str, prefix: str, local_dir:str):
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    local_path = os.path.join(local_dir, key[len(prefix)+1:])
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    print(f"Downloading {key} to {local_path}")
                    s3_client.download_file(bucket, key, local_path)

    def cluster_ngrams(self):
        ngrams = self.__get_top_ngrams()
        self.logger.info(f"Top ngrams: {ngrams}")

        ngram_list = list(ngrams.keys())
        st_model = "sentence-transformers/distiluse-base-multilingual-cased-v2"
        self.copy_s3_folder_to_local(
                    s3_client=self.s3_client,
                    bucket=self.__corpora_bucket,
                    prefix=f"hf/{st_model}",
                    local_dir=st_model
                )
        st_model = SentenceTransformer(st_model)
        embeddings, agg_cluster = self.fit_agg_cluster(st_model=st_model, 
                                                       samples=ngram_list, 
                                                       metric=self.__metric, 
                                                       distance_threshold=self.__distance_threshold)

        cluster_assignment = agg_cluster.labels_

        clustered_sentences = {}
        for sentence_id, cluster_id in enumerate(cluster_assignment):
            if cluster_id not in clustered_sentences:
                clustered_sentences[cluster_id] = []

            clustered_sentences[cluster_id].append(ngram_list[sentence_id])

        for cluster_id, cluster in clustered_sentences.items():
            self.logger.info(f"Cluster {cluster_id}: {cluster}")

        keyphrases = list()
        for cluster_id, cluster in clustered_sentences.items(): 
            if len(cluster) > 1:
                personalization = { c: math.exp(ngrams[c])  for c in cluster } 
                cluster_ngram = self.find_best_sample(samples=cluster, 
                                                                sample_embeddings=embeddings, 
                                                                personalization=personalization)
            else:
                cluster_ngram = cluster[0]

            keyphrases.append({ "keyphrase": cluster_ngram, "phrase_piece": ngrams[cluster_ngram]})

        self.logger.info(f"Keyphrases: {keyphrases}")
        self.__save_keyphrases(keyphrases_final=keyphrases)
        
    def __upload_keyphrases(self, file_obj=None):
        attempt = 0
        file_name = str(uuid4())
        key = f"keyphrases/tag={self.__tag}/{file_name}.gz"
        
        while True:
            try:
                file_obj.seek(0)
                self.s3_client.put_object(Body=file_obj, Bucket=self.__corpora_bucket, Key=key)
                break
            except Exception as e:
                attempt = retry(attempt=attempt, error=e)

    @staticmethod
    def dedup_keyphrases(keyphrases):
        keyphrases.sort(key=lambda x: x['phrase_piece'], reverse=True)
        keyphrases_final = []

        for keyphrase in keyphrases:
            included = False
            for keyphrase_final in keyphrases_final:
                if (keyphrase_final['keyphrase'].find(keyphrase['keyphrase']) >= 0):
                    included = True
                    break
            if not included:
                keyphrases_final.append(keyphrase)

        return keyphrases_final

    def __save_keyphrases(self, keyphrases_final):
        timestamp = int(time.time())

        keyphrases_final = self.dedup_keyphrases(keyphrases_final)
        self.logger.info(f"Final keyphrases after de-duplication: {keyphrases_final}")

        with NamedTemporaryFile(mode='w+b', delete=True) as file_obj:
            with gzip.open(file_obj, mode="wb") as gzip_obj:
                for keyphrase_final in keyphrases_final:
                    json_obj = { 
                                "keyphrase": keyphrase_final['keyphrase'], 
                                "phrase_piece": keyphrase_final['phrase_piece'],
                                "timestamp": timestamp
                                }
                    json_str = json.dumps(json_obj) + "\n"
                    gzip_obj.write(json_str.encode('utf-8'))

                gzip_obj.close()
                self.__upload_keyphrases(file_obj)


    def __call__(self):
        self.cluster_ngrams()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Compute Keyphrases')

    parser.add_argument('--config', type=str,  help='Corpus config file', required=True)
    args, _ = parser.parse_known_args()
    compute_key_phrases = ComputeKeyphrases(args)
    compute_key_phrases()