import argparse
import logging
import json
import boto3


class CreateCorpusSuccess:
    LOG_INTERVAL = 1000000
    
    def __init__(self, args):
        logging.basicConfig(
            format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("create_corpus_success")

        with open(args.config) as json_file:
            config = json.load(json_file)
            self.__corpus_id = config['corpus_id']
            self.__corpus_table = config['corpus_table']
        
        self.dynamodb = boto3.resource('dynamodb')

    def __update_corpus_item(self):
        table = self.dynamodb.Table(self.__corpus_table)
        table.update_item(Key={'corpus_id': self.__corpus_id},
            UpdateExpression="set corpus_state = :s, state_machine_execution_arn=:a, state_machine_start_date=:d",
            ExpressionAttributeValues={':s': "READY", ':a': "none", ':d': "none"})

    def __call__(self):
        self.__update_corpus_item()
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Create corpus success')

    parser.add_argument('--config', type=str,  help='Create corpus config file', required=True)
    args, _ = parser.parse_known_args()
    create_corpus_success = CreateCorpusSuccess(args)
    create_corpus_success()