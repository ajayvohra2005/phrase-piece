import json
import boto3
import os
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):

    result = None
    try:
        logger.info("event:"+ str(event))
        logger.info("context:"+ str(context))
        
        body = json.loads(event["body"])
        corpus_id = body['CorpusId']
        corpora_bucket = os.environ['corpora_bucket']
        config_s3_uri = f"s3://{corpora_bucket}/{corpus_id}/config/create_corpus.json"
    
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(os.environ['corpus_table'])
        corpus_item = get_corpus_item(table=table, corpus_id=corpus_id)
        
        if corpus_item['corpus_state'] == 'READY' or corpus_item['corpus_state'] == 'FAILED':
            update_corpus_state(table=table, corpus_id=corpus_id, state="DELETED")
            
            sm_execution = start_state_machine(sm_arn=os.environ['delete_corpus_sm_arn'],
                corpus_id=corpus_id, config_s3_uri=config_s3_uri)
            update_corpus_sm(table=table, corpus_id=corpus_id, sm_execution=sm_execution )
            corpus_item = get_corpus_item(table=table, corpus_id=corpus_id)
                
        response={}
        response["CorpusId"]=corpus_id
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
    item = None
    try:
        response = table.get_item(Key={'corpus_id': corpus_id})
        item = response['Item']
    except Exception as e:
        logger.error(f"{e}")

    return item

def update_corpus_state(table=None, corpus_id=None, state=None):
    table.update_item(
        Key={'corpus_id': corpus_id},
        UpdateExpression="set corpus_state=:s",
        ExpressionAttributeValues={':s': state}
    )

def update_corpus_sm(table=None, corpus_id=None, sm_execution=None):
    table.update_item(
        Key={'corpus_id': corpus_id},
        UpdateExpression="set state_machine_execution_arn=:a, state_machine_start_date=:d",
        ExpressionAttributeValues={':a': sm_execution['executionArn'], ':d': str(sm_execution['startDate']) }
    )

def start_state_machine(sm_arn=None, corpus_id=None, config_s3_uri=None):
    sfn_client = boto3.client('stepfunctions')
    ts = int(time.time())
    response = sfn_client.start_execution(
        stateMachineArn=sm_arn,
        name=f"delete_corpus_{corpus_id}_{ts}",
        input="{ \"S3JsonConfig\" : \"" + config_s3_uri + "\" }"
    )

    return response