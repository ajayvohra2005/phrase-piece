# Phrase Piece

## Setup

Complete the [prerequisites](#prerequisites). 

Run following command on the CDK build machine:

    source setup-venv.sh 

## Login into ECR

To login into ECR:

    aws ecr get-login-password --region us-west-2  | docker login --username AWS --password-stdin 763104351884.dkr.ecr.us-west-2.amazonaws.com

## Configure CDK Context

Configure `code-bucket` and `universe-bucket` (can be the same S3 bucket) in [cdk.context.json](./cdk/cdk.context.json):

    {
        "code-bucket": "",
        "universe-bucket": "",
        "corpus-stack-name": "phrase-piece",
        "lambda-function-version": "phrase_piece_v1.5",
        "scripts-version": "phrase_piece_v1.9"
    }

## Deploy CDK stack

Run following command to deploy cdk stack:

    cdk deploy all

## Destroy CDK stack

To interactively destroy the deployed stack, execute:

    cdk destroy --all

## Prerequisites

Before we can deploy this solution, we need to complete following prerequisites:

1. [AWS account access](#aws-account-access)
2. [CDK Build machine](#cdk-build-machine)


### AWS account access

First, you need an AWS account. If needed, [create an AWS account](https://docs.aws.amazon.com/accounts/latest/reference/manage-acct-creating.html). This solution assumes you have [system administrator job function](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_job-functions.html) access to the AWS Management Console.

### CDK build machine

Next, you need a build machine. This solution uses [AWS CDK](https://aws.amazon.com/cdk/) to build the required stacks. You may use any machine with NodeJS, Python, [Docker](https://www.docker.com/) and [AWS CDK for Typescript](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) installed as your build machine.  If you are new to AWS CDK, we recommend launching a fully-configured build machine in your target AWS region, as described below:

* Select your [AWS Region](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html). The AWS Regions supported by this solution include, us-east-1, us-east-2, us-west-2, eu-west-1, eu-central-1, ap-southeast-1, ap-southeast-2, ap-northeast-1, ap-northeast-2, and ap-south-1. 
* If you do not already have an Amazon EC2 key pair, [create a new Amazon EC2 key pair](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#prepare-key-pair). You need the key pair name to specify the `KeyName` parameter when creating the AWS CloudFormation stack below. 
* Use the [public internet address](http://checkip.amazonaws.com/) of your laptop as the base value for the [CIDR](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html) to specify `SecurityGroupAccessCIDR` parameter in the CloudFormation template used below.  
* Using AWS Management console, create the build machine using [`cfn/ubuntu-developer-machine.yaml`](cfn/ubuntu-developer-machine.yaml) [AWS CloudFormation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html) template. This template creates [AWS Identity and Access Management (IAM)](https://aws.amazon.com/iam/) resources, so when you [create the CloudFormation Stack using the console](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-create-stack.html), in the **Review** step, you must check 
**I acknowledge that AWS CloudFormation might create IAM resources.**  
* Once the stack status in CloudFormation console is `CREATE_COMPLETE`, find the EC2 instance launched in your stack in the Amazon EC2 console, and [connect to the instance using SSH](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) as user `ubuntu`, using your SSH key pair.
* When you connect to the instance using SSH, if you see the message `"Cloud init in progress."`, disconnect and try later after about 10 minutes. If you see the message `AWS developer machine is ready!`, your build machine is ready.
* If you see the message ```AWS developer machine is ready!```, run the command ```sudo passwd ubuntu``` to set a new password for user ```ubuntu```. 
* Download and install the [Amazon DCV client](https://docs.aws.amazon.com/dcv/latest/userguide/client.html) on your laptop.
* Use the Amazon DCV Client to login to the developer machine as user ```ubuntu```
* When you first login to the developer machine using the Amazon DCV client, you will be asked if you would like to upgrade the OS version. **Do not upgrade the OS version** .

#### NOTE
The developer machine uses EC2 [user-data](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/user-data.html) to automatically install the required software in the developer machine instance. The log output of this automatic installation is available in `/var/log/cloud-init-output.log` file. Most *transient* failures in the automatic user-data installation can be fixed by rebooting the instance.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) and [CODE OF CONDUCT](CODE_OF_CONDUCT.md) for more information.

## License

This solution is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.