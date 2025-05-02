#!/usr/bin/env python3
import os
import logging

import aws_cdk
#from cdk_nag import AwsSolutionsChecks

from corpus_stacks.corpus_stack import CorpusStack

logger = logging.getLogger("mars_app")
logging.basicConfig(format='%(levelname)s:%(process)d:%(message)s',
            level=logging.INFO)

app = aws_cdk.App()
#aws_cdk.Aspects.of(app).add(AwsSolutionsChecks());

corpus_stack_name = app.node.try_get_context("corpus-stack-name")
logger.info(f"Corpus stack name: {corpus_stack_name}")
corpus_stack = CorpusStack(app, "CorpusStack", stack_name = corpus_stack_name,
    env=aws_cdk.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')))


app.synth()
