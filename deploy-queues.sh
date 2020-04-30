#!/bin/bash
aws cloudformation package --template queues.yml --s3-bucket aws-sam-cli-managed-default-samclisourcebucket-16by7zdqsml2x --output-template queues-export.yml
aws cloudformation deploy --template-file queues-export.yml --stack-name TagQueueStack
