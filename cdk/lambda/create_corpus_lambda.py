import json
import time
import boto3
import os
import logging
from uuid import uuid4

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    result = None
    try:
        logger.info("event:"+ str(event))
        logger.info("context:"+ str(context))
        
        body = json.loads(event["body"])

        config = get_config(body)
        config_s3_uri = upload_config(config)
        corpus_id = config['corpus_id']

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['corpus_table'])

        corpus_name = config['corpus_name']
        put_corpus_item(corpus_name=corpus_name, table=table, corpus_id=corpus_id, 
            language_code=config['language_code'], 
            country_code=config['country_code'],
            start_epoch=config['start_epoch'],
            end_epoch=config['end_epoch'],
            max_length=config['max_length'],
            category=config['category'],
            athena_database=config.get('athena_database'),
            athena_table=config.get('athena_table'))

        glue_client = boto3.client("glue")
        glue_client.create_database( DatabaseInput={'Name': corpus_id})

        sm_execution  = start_state_machine(sm_arn=os.environ['create_corpus_sm_arn'],
                corpus_id=corpus_id, config_s3_uri=config_s3_uri)

        update_corpus_sm(table=table, corpus_id=corpus_id, sm_execution=sm_execution )
        corpus_item = get_corpus_item(table=table, corpus_id=corpus_id)

        response={}
        response["CorpusId"]=corpus_id
        response["CorpusName"]=corpus_item['corpus_name']
        response["CorpusState"]=corpus_item['corpus_state']
        response["CorpusStateMachine"]=corpus_item['state_machine_execution_arn']

        body_json=json.dumps(response)

        result = {}
        result["statusCode"]="200"
        headers={}
        headers["Access-Control-Allow-Origin"]="*"
        result["headers"]=headers
        result["body"]=body_json
        result["isBase64Encoded"]=False
    except Exception as e:
        logger.error(f"{e}")
        result = request_error(event)

    return result

def request_error(event):
    result = {}
    result["statusCode"]="400"
    headers={}
    headers["Access-Control-Allow-Origin"]="*"
    result["headers"]=headers
    response={}
    response["Error"] = event
    body_json=json.dumps(response)
    result["body"] = body_json
    result["isBase64Encoded"]=False

    return result

def get_corpus_item(table=None, corpus_id=None):
    response = table.get_item(Key={'corpus_id': corpus_id})
    return response['Item']

def put_corpus_item( corpus_name=None, table=None, corpus_id=None, 
    start_epoch=None, end_epoch=None, category=None,
    language_code=None, 
    country_code=None,  
    max_length=None, 
    athena_database=None,
    athena_table=None,
    s3_uri=None):
    
    if not country_code:
        country_code = 'null'
        
    item = {
            'corpus_id': corpus_id,
            'corpus_name': corpus_name,
            'corpus_state': "CREATING",
            'start_epoch': start_epoch,
            'end_epoch': end_epoch,
            'language_code': language_code,
            'country_code': country_code,
            'max_length': max_length,
            'state_machine_execution_arn': 'null',
            'state_machine_start_date': 'mull'
    }

    if athena_database and athena_table:
        item['athena_database'] = athena_database
        item['athena_table'] = athena_table

    if category:
        item['cateogry'] = category

    if s3_uri:
        item['s3_uri'] = s3_uri

    table.put_item(Item=item)

def start_state_machine(sm_arn=None, corpus_id=None, config_s3_uri=None):
    sfn_client = boto3.client('stepfunctions')
    response = sfn_client.start_execution(
        stateMachineArn=sm_arn,
        name=f"create_corpus_{corpus_id}",
        input="{ \"S3JsonConfig\" : \"" + config_s3_uri + "\" }"
    )

    return response

def update_corpus_sm(table=None, corpus_id=None, sm_execution=None):
    table.update_item(
        Key={'corpus_id': corpus_id},
        UpdateExpression="set state_machine_execution_arn=:a, state_machine_start_date=:d",
        ExpressionAttributeValues={':a': sm_execution['executionArn'], ':d': str(sm_execution['startDate']) }
    )

def upload_config(config):
    corpus_id = config['corpus_id']
    key = f"{corpus_id}/config/create_corpus.json"

    s3 = boto3.resource('s3')
    bucket_name = config['corpora_bucket']
    bucket = s3.Bucket(bucket_name)
    bucket.put_object(Body=json.dumps(config), Key=key)

    return f"s3://{bucket_name}/{key}"

def get_config(body):
    config = dict()

    config['corpus_name']=body['CorpusName']
    config['athena_database'] = body.get('AthenaDatabase', '')
    config['athena_table'] = body.get('AthenaTable', '')
    config['s3_uri'] = body.get('S3Uri', '')
    config['corpus_table'] = os.environ['corpus_table']
    
    config['corpora_bucket'] = os.environ['corpora_bucket']
    config['batch_job_queue'] = os.environ['batch_job_queue']
    config['batch_job_definition'] = os.environ['batch_job_definition']
    config['cpu_inference_batch_job_definition'] = os.environ['cpu_inference_batch_job_definition']
    
    config['corpus_id'] = str(uuid4())
    config['language_code'] = body.get('LanguageCode', 'en')
    config['start_epoch'] = body.get('StartTimestamp', 946684800)
    config['end_epoch'] = body.get('EndTimestamp', int(time.time()) )
    config['use_athena'] = config.get('athena_database')  and config.get('athena_table')
    config['metric'] = body.get("DistanceMetric", "cosine")
    assert config['metric'] in ["cosine" , "euclidean" , "l1" , "l2" , "manhattan" ]

    config['top_k'] = int(body.get("TopK", 100))

    config["distance_threshold"] = float(body.get("DistanceThreshold",  0.30))

    config['max_length'] = int(body.get("MaxLength", 10))
    config['tag'] = body.get("Tag", config['corpus_name'])

    assert config['max_length'] >= 2

    config['pos_pattern'] = body.get("PosPattern", "")
   
    if 'CountryCode' in body:
        config['country_code'] = body['CountryCode']
    else:
        config['country_code'] = ''

    if 'Category' in body:
        config['category'] = body['Category']
    else:
        config['category'] = ''

    return config