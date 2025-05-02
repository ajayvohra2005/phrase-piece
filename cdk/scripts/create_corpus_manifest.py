import argparse
import os
from tempfile import NamedTemporaryFile
import logging
import json
import time
from urllib.parse import urlparse
import boto3

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

class AthenaManifest:

    UNLOAD_SQL_TEMPLATE = "UNLOAD (select text  FROM {} WHERE timestamp_ms >= {}  AND timestamp_ms < {} AND language_code >= '{}' AND language_code <= '{}' AND country_code >= '{}' AND country_code <= '{}' ) TO '{}' WITH (format = 'JSON') "
    
    def __init__(self, athena_database=None, output_location=None):
        logging.basicConfig( format='%(levelname)s:%(process)d:%(message)s', level=logging.INFO)
        self.logger = logging.getLogger("athena_manifest")
        
        self.athena_client = boto3.client('athena')
        self.athena_database = athena_database
        self.output_location = output_location

    def __athena_query(self, query=None):
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': self.athena_database
            },
            ResultConfiguration={
                'OutputLocation': self.output_location
            }
        )
        return response

    def execute(self, query=None):

        execution = self.__athena_query(query=query)
        execution_id = execution['QueryExecutionId']
        state = 'RUNNING'

        response = None
        while ( state in ['RUNNING', 'QUEUED']):
            response = self.athena_client.get_query_execution(QueryExecutionId = execution_id)
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

        return response

class CreateCorpusManifest:

    def __init__(self, args):
        logging.basicConfig(
            format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("create_corpus_manifest")

        with open(args.config) as json_file:
            config = json.load(json_file)
            self.__corpora_bucket = config['corpora_bucket']

            self.__corpus_id = config['corpus_id']
            self.__language_code = config['language_code']
            self.__country_code = config['country_code']
            if self.__country_code == '':
                self.__country_code = '%'

            self.__category = config['category']
            if self.__category == '':
                self.__category = '%'

            self.__start_epoch = config['start_epoch']
            self.__end_epoch = config['end_epoch']
            self.__s3_uri = config.get("s3_uri", "")

            self.__athena_database = config.get("athena_database")
            self.__athena_table = config.get("athena_table")
           
            assert self.__start_epoch < self.__end_epoch
            assert len(self.__language_code) == 2
            assert self.__country_code == '%' or len(self.__country_code) == 2

            self.__athena_manifest = None
           
            self.__batch_job_queue = config['batch_job_queue']
            self.__batch_job_definition = config['batch_job_definition']
               
    def __create_s3_manifest(self):

        s3_uri_parse = urlparse(self.__s3_uri)
        source_bucket = s3_uri_parse.netloc
        source_key = s3_uri_parse.path[1:]
        
        s3_client = boto3.client("s3")
        with NamedTemporaryFile(mode='w+b', delete=True) as file_obj:
            for key in s3_bucket_keys(s3_client=s3_client, bucket_name=source_bucket, bucket_prefix=source_key):
                file_obj.write(f"s3://{source_bucket}/{key}\n".encode("utf-8"))

            file_obj.seek(0)
            s3_client.put_object(Body=file_obj, Bucket=self.__corpora_bucket, Key=f"{self.__corpus_id}/manifest/part-0000")

    def __create_corpus_manifest(self):
        if self.__s3_uri:
            self.__create_s3_manifest()
            self.__create_athena_manifest()
        else:
            output_location = f"s3://{self.__corpora_bucket}/{self.__corpus_id}/athena/output/"
            self.__s3_uri = f"s3://{self.__corpora_bucket}/{self.__corpus_id}/athena/manifest/"     
            self.__athena_manifest = AthenaManifest(athena_database=self.__athena_database, output_location=output_location)

            country_code = '' if self.__country_code == '%' else self.__country_code 

            start_epoch_ms = int(self.__start_epoch * 1000) if self.__start_epoch < int(10**10) else self.__start_epoch
            end_epoch_ms = int(self.__end_epoch * 1000) if self.__end_epoch < int(10**10) else self.__end_epoch

            query = self.__athena_manifest.UNLOAD_SQL_TEMPLATE.format(
                self.__athena_table,
                start_epoch_ms,  end_epoch_ms, 
                self.__language_code, self.__language_code,
                country_code, country_code, self.__s3_uri)
            self.__athena_manifest.execute(query=query)

        self.__create_manifest_table()

    def __create_manifest_table(self):
        output_location = f"s3://{self.__corpora_bucket}/{self.__corpus_id}/athena/output/"
        athena_manifest = AthenaManifest(athena_database=self.__corpus_id, output_location=output_location)
        s3_storage_location =   f"s3://{self.__corpora_bucket}/{self.__corpus_id}/athena/manifest/" 
        query= f"""
            CREATE EXTERNAL TABLE manifest (text string)
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            LOCATION '{s3_storage_location}/' 
            TBLPROPERTIES ( 'compressionType'='gzip' )
            """
        return athena_manifest.execute(query=query)

    def __create_athena_manifest(self):
        self.__batch_client = boto3.client('batch')
        aws_region = os.environ['AWS_DEFAULT_REGION']
        s3_json_config = os.environ['S3_JSON_CONFIG']
        s3_python_script = os.environ['S3_PYTHON_SCRIPT']

        s3_python_script = s3_python_script.rsplit("/", 1)[0] + "/create_athena_manifest.py"

        prefix = f"{self.__corpus_id}/manifest/"

        self.__jobs=dict()
        s3_client = boto3.client("s3")
        
        for key in s3_bucket_keys(s3_client, bucket_name=self.__corpora_bucket, bucket_prefix=prefix):
            job_name = f"cam-{self.__corpus_id}"
            manifest = f"s3://{self.__corpora_bucket}/{key}"
            response = self.__batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.__batch_job_queue,
                jobDefinition=self.__batch_job_definition,
                retryStrategy={'attempts': 3},
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
            self.__jobs[job_id] = job_id

        self.__pending=[ job_id for job_id in self.__jobs.keys() ]
        while self.__pending:
            pending_jobs = []
            for i in range(0, len(self.__pending), 100):

                jobs_slice = self.__pending[i:i+100]
                if jobs_slice:
                    response = self.__batch_client.describe_jobs(jobs=jobs_slice)
                    
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
                self.__batch_client.terminate_job(jobId=job_id, reason=reason)
            except Exception as e:
                self.logger.warning(f"ignoring {e}")
        
        raise RuntimeError(reason)
    
    def __call__(self):
        start = time.time()
        self.__create_corpus_manifest()
        end = time.time()
        self.logger.info(f"Create corpus manifest completed in {end - start} secs")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Create corpus manifest')

    parser.add_argument('--config', type=str,  help='Create corpus config file', required=True)
    args, _ = parser.parse_known_args()
    create_corpus_manifest = CreateCorpusManifest(args)
    create_corpus_manifest()