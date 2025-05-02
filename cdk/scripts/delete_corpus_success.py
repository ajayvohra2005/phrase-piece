import argparse
import logging
import json
import boto3

class DeleteCorpusSuccess:
    LOG_INTERVAL = 1000000

    def __init__(self, args):
        logging.basicConfig(
            format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)
        self.logger = logging.getLogger("delete_corpus_success")

        with open(args.config) as json_file:
            config = json.load(json_file)
            self.__corpus_id = config['corpus_id']
            self.__corpus_table = config['corpus_table']

        self.dynamodb = boto3.resource('dynamodb')
        
    def __delete_corpus_item(self):
        table = self.dynamodb.Table(self.__corpus_table)
        table.delete_item(Key={'corpus_id': self.__corpus_id})

    def __call__(self):
        self.__delete_corpus_item()
        self.__delete_glue_database()
 
    def __delete_glue_database(self):
        try:
            glue_client = boto3.client("glue")
            response = glue_client.delete_database(Name=self.__corpus_id)
            self.logger.info(f"{response}")
        except Exception as e:
            self.logger.warning(f"Ignoring: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Delete corpus success')

    parser.add_argument('--config', type=str,  help='Delete corpus config file', required=True)
    args, _ = parser.parse_known_args()
    delete_corpus_success = DeleteCorpusSuccess(args)
    delete_corpus_success()