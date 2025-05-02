#!/bin/bash

[[ -z "${CODE_BUCKET}" ]] && echo "CODE_BUCKET env variable is required" && exit 1
[[ -z "${LAMBDA_VERSION}" ]] && echo "LAMBDA_VERSION env variable is required" && exit 1
[[ -z "${SCRIPTS_VERSION}" ]] && echo "SCRIPTS_VERSION env variable is required" && exit 1

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR=$cur_dir/..

list_scripts=$(aws s3 ls s3://$CODE_BUCKET/scripts/${SCRIPTS_VERSION}/)
[[ -z "${list_scripts}" ]] && aws s3 cp --recursive $DIR/scripts s3://$CODE_BUCKET/scripts/${SCRIPTS_VERSION}/

list_lambda=$(aws s3 ls s3://$CODE_BUCKET/lambda/$LAMBDA_VERSION/)

if [[ -z "${list_lambda}" ]]
then
    cd $DIR/lambda
    zip /tmp/create_corpus_lambda.zip create_corpus_lambda.py
    aws s3 cp /tmp/create_corpus_lambda.zip s3://$CODE_BUCKET/lambda/$LAMBDA_VERSION/create_corpus_lambda.zip
    rm /tmp/create_corpus_lambda.zip

    zip /tmp/delete_corpus_lambda.zip delete_corpus_lambda.py
    aws s3 cp /tmp/delete_corpus_lambda.zip s3://$CODE_BUCKET/lambda/$LAMBDA_VERSION/delete_corpus_lambda.zip
    rm /tmp/delete_corpus_lambda.zip
fi
