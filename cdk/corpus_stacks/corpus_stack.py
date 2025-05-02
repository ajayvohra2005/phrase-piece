import logging
from os import path
import os

from aws_cdk import (
    Stack,
    cloudformation_include,
    aws_ecr_assets
)

from constructs import Construct
from os.path import exists as file_exists
import subprocess

class CorpusStack(Stack):

    CFN_TEMPLATE_FILE = "cfn/corpus.yaml"
    UPLOAD_SCRIPT = "bin/upload-scripts-s3.sh"

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.logger = logging.getLogger("corpus_stack")
        logging.basicConfig(
            format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)

        assert(file_exists(CorpusStack.CFN_TEMPLATE_FILE))
        assert(file_exists(CorpusStack.UPLOAD_SCRIPT))
        
        code_bucket = self.node.try_get_context("code-bucket")
        assert(code_bucket)

        universe_bucket = self.node.try_get_context("universe-bucket")
        assert(universe_bucket)

        batch_image_asset = aws_ecr_assets.DockerImageAsset(self, "BatchImage",directory="batch_image")
        batch_cpu_inference_image_asset = aws_ecr_assets.DockerImageAsset(self, "BatchCpuInferenceImage",directory="batch_cpu_inference_image")

        cfn_parameters = {}

        cfn_parameters["BatchImageEcrUri"] = batch_image_asset.image_uri
        cfn_parameters["BatchCpuInferenceImageEcrUri"] = batch_cpu_inference_image_asset.image_uri
        cfn_parameters['CodeBucket'] = code_bucket
        cfn_parameters['UniverseBucket'] = universe_bucket

        lambda_version = self.node.try_get_context("lambda-function-version")
        if not lambda_version:
            lambda_version = "v1.0.0"

        cfn_parameters['LambdaFunctionsVersion'] = lambda_version

        scripts_version = self.node.try_get_context("scripts-version")
        if not scripts_version:
            scripts_version = "v1.0.0"

        cfn_parameters['ScriptsVersion'] = scripts_version

        args = [ CorpusStack.UPLOAD_SCRIPT ]
        env = { "CODE_BUCKET": code_bucket, "SCRIPTS_VERSION": scripts_version, "LAMBDA_VERSION": lambda_version, "PATH": os.getenv("PATH")}
        subprocess.check_call(args, env=env)

        cloudformation_include.CfnInclude(self, "CorpusTemplate", 
            template_file=CorpusStack.CFN_TEMPLATE_FILE,
            parameters=cfn_parameters)
