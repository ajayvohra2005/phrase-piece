import argparse
import  logging
import json
import time
import boto3
import gzip
from urllib.parse import urlparse
from multiprocessing import Process
from tempfile import NamedTemporaryFile
import signal
import random
from uuid import uuid4

class AthenaManifestProcess(Process):

    def __init__(self, manifest=None, start=None, end=None, corpora_bucket=None, corpus_id=None,):
        Process.__init__(self)

        logging.basicConfig(
            format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("athena_manifest_process")
        self.__s3_client = boto3.client('s3')
        self.__manifest = manifest
        self.__start = start
        self.__end = end
        self.__corpus_id = corpus_id
        self.__corpora_bucket = corpora_bucket

    @staticmethod
    def get_object(s3_client=None, bucket=None, key=None):
        MAX_ATTEMPTS = 5
        attempt = 0
        while True:
            try:
                s3_obj = s3_client.get_object(Bucket=bucket, Key=key)
                return s3_obj['Body'].read().decode('utf-8')
            except Exception as e:
                print(f"get_object exception: {e}")
                if attempt <= MAX_ATTEMPTS:
                    attempt += 1
                
                interval = random.uniform(5**(attempt - 1), 5**attempt)
                time.sleep(interval)

    def __upload_manifest_file_obj(self, file_obj=None):
        attempt = 0
        file_name = str(uuid4())
        key = f"{self.__corpus_id}/athena/manifest/{file_name}.gz"
        
        while True:
            try:
                file_obj.seek(0)
                self.__s3_client.put_object(Body=file_obj, Bucket=self.__corpora_bucket, Key=key)
                break
            except Exception as e:
                attempt = self.retry(attempt=attempt, error=e)

    def run(self):    
        i = 0
        count = 0
        with open(self.__manifest, "rb") as file_obj:
            file_obj.seek(0)

            manifest_file_obj = NamedTemporaryFile(mode='w+b', delete=True)
            manifest_gzip_obj = gzip.open(manifest_file_obj, mode="wb")
           
            while line := file_obj.readline():
                if i >= self.__start and i < self.__end:
                    s3_link = line.strip().decode('utf-8')
                    s3_uri_parse = urlparse(s3_link)
                    source_bucket = s3_uri_parse.netloc
                    source_key = s3_uri_parse.path[1:]

                    text = self.get_object(s3_client=self.__s3_client, bucket=source_bucket, key=source_key)
                    json_obj = {"text": text}
                    json_str = json.dumps(json_obj)+"\n"
                    manifest_gzip_obj.write(json_str.encode('utf-8'))
                    count += 1
                    
                i += 1
            
            file_obj.close()
            manifest_gzip_obj.close()

            if count > 0:
                self.__upload_manifest_file_obj(file_obj=manifest_file_obj)
            manifest_file_obj.close()

class AthenaManifest:
    P_CONCURRENT = 4

    def __init__(self, args):
        logging.basicConfig(
            format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("athena_manifest")

        with open(args.config) as json_file:
            config = json.load(json_file)
            self.__corpora_bucket = config['corpora_bucket']
            self.__corpus_id = config['corpus_id']
           
        if args.manifest:
            self.__manifest = args.manifest
        else:
            self.__manifest = None
        
        self.s3_client = boto3.client('s3')
        
        signal.signal(signal.SIGINT, self.__exit_gracefully)
        signal.signal(signal.SIGTERM, self.__exit_gracefully)

    def __exit_gracefully(self, signum, frame):
        self.__abort(f"Received {signum} signal")

    def __download_manifest(self):
        s3_uri_parse = urlparse(self.__manifest)
        manifest_key = s3_uri_parse.path[1:]

        with NamedTemporaryFile(mode='w+b', delete=False) as file_obj:
            self.s3_client.download_fileobj(s3_uri_parse.netloc, manifest_key, file_obj)
            file_obj.seek(0)
            
            count = 0
            while _ := file_obj.readline():
                count += 1

            file_name = file_obj.name
        
        return count, file_name
    
    def __process_manifest(self):    
        self.logger.info(f"processing manifest file: {self.__manifest}")  
        start = time.time()

        _p_list = []
        count, manifest = self.__download_manifest()
        part = int(count/self.P_CONCURRENT)
  
        for index in range(1, self.P_CONCURRENT+1, 1):
            start = (index - 1) * part
            end = index*part if index < self.P_CONCURRENT else count
            if end > start:
                p = AthenaManifestProcess( manifest=manifest, 
                                        start=start, end=end,
                                        corpora_bucket=self.__corpora_bucket,
                                        corpus_id=self.__corpus_id)
                _p_list.append(p)
                p.start()

        for _p in _p_list:
            _p.join()
            if _p.exitcode != 0:
                raise RuntimeError(f"AthenaManifestProcess exited with exit code: {_p.exitcode}")

        elapsed = time.time() - start
        self.logger.info(f"Athena process_manifest elapsed time: {elapsed}")
    
    def __abort(self, reason):
        for job_id in self.__jobs.keys():
            try:
                self.batch_client.terminate_job(jobId=job_id, reason=reason)
            except Exception as e:
                self.logger.warning(f"ignoring {e}")
        
        raise RuntimeError(reason)

    def __call__(self):
        self.__process_manifest()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Create Athena Manifest')

    parser.add_argument('--config', type=str,  help='Corpus config file', required=True)
    parser.add_argument('--manifest', type=str, default="", help='S3 URI for manifest part', required=True)
    args, _ = parser.parse_known_args()
    athena_manifest = AthenaManifest(args)
    athena_manifest()