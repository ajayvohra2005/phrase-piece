
import gzip
import json
import os
from tempfile import NamedTemporaryFile
import time
from huggingface_hub import snapshot_download
from tempfile import TemporaryDirectory
from pathlib import Path


def get_lamabda_function_arn(lambda_client, 
                             aws_region: str,
                             partial_arn: str):
    next_marker = None
    while True:
        if not next_marker:
            response = lambda_client.list_functions()
        else:
            response = lambda_client.list_functions(
                Marker=next_marker,
            )
        
        functions = response.get('Functions', [])
        for function in functions:
            if function['FunctionArn'].startswith(partial_arn):
                return function['FunctionArn']

        next_marker = response.get('NextMarker', None)
        if not next_marker:
            return None
        
def create_corpus(lambda_client,
                  create_corpus_arn: str,
                  name:str, 
                  s3_uri:SystemError):

  json_data = { 
    "CorpusName": name,
    "S3Uri": s3_uri,
    "SimThreshold": "0.10"
  }
  
  payload = json.dumps({ "body": json.dumps(json_data) })

  response = lambda_client.invoke(
      FunctionName=create_corpus_arn,
      InvocationType='RequestResponse',
      Payload=payload
  )

  print(response)
  
  json_obj = json.loads(response['Payload'].read())
  data = json.loads(json_obj['body'])
  return data

def wait_for_sfn_sm(sfn_client, sm_execution_arn:str):
    status = 'RUNNING'
    while status == 'RUNNING':
        response = sfn_client.describe_execution(executionArn=sm_execution_arn)
        status = response.get('status')
        if status == 'RUNNING':
            time.sleep(15)
        
    return status

def s3_bucket_keys(s3_client, bucket_name:str, bucket_prefix:str):
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

def delete_corpus(lambda_client, delete_corpus_arn:str, corpus_id:str ):
    payload = json.dumps( { "body": "{ \"CorpusId\": \"" + corpus_id + "\" }" } )

    lambda_client.invoke(
        FunctionName=delete_corpus_arn,
        InvocationType='RequestResponse',
        Payload=payload
    )

def get_candidates(s3_client, corpora_bucket:str, bucket_prefix:str):
        
    extracted = []
    try:
        for key in s3_bucket_keys(s3_client=s3_client, bucket_name=corpora_bucket, bucket_prefix=bucket_prefix):
            with NamedTemporaryFile(mode='w+b', delete=True) as file_obj:
                s3_client.download_fileobj(corpora_bucket, key, file_obj)
                file_obj.seek(0)

                with gzip.open(file_obj, mode="rb") as gzip_obj:
                    while (line := gzip_obj.readline()):
                        json_obj=json.loads(line.decode('utf-8'))
                        keyphrase = json_obj['keyphrase']
                        phrase_piece = json_obj['phrase_piece']
                        extracted.append( (keyphrase, phrase_piece) )
    except KeyError as e:
        print(e)

    extracted.sort(key = lambda x: x[1], reverse=True)
    return extracted

def get_references(s3_client, universe_bucket:str, bucket_prefix:str):
    gt = []
    try:
        for key in s3_bucket_keys(s3_client=s3_client, bucket_name=universe_bucket, bucket_prefix=bucket_prefix):
            s3_obj = s3_client.get_object(Bucket=universe_bucket, Key=key)
            json_str = s3_obj['Body'].read().decode('utf-8')
            json_obj = json.loads(json_str)
            gt.extend(json_obj['keyphrases'])
    except KeyError as e:
        print(e)

    return gt


def snapshot_hf_model_to_s3(s3_client, hf_model: str, bucket:str, prefix: str):

    with TemporaryDirectory(suffix="model", prefix="hf", dir=".") as cache_dir:
        ignore_patterns = ["*.h5", "*.onnx_data", "*.onnx", "*.pt", "*.bin", "openvino*"]
        snapshot_download(repo_id=hf_model, cache_dir=cache_dir, ignore_patterns=ignore_patterns)

        local_model_path = Path(cache_dir)
        model_snapshot_path = str(list(local_model_path.glob(f"**/snapshots/*"))[0])
      
        for root, dirs, files in os.walk(model_snapshot_path):
            for file in files:
                full_path = os.path.join(root, file)
                with open(full_path, 'rb') as data:
                    key = f"{prefix}/{full_path[len(model_snapshot_path)+1:]}"
                    print(f"Uploading {key}")
                    s3_client.upload_fileobj(data, bucket, key)

        