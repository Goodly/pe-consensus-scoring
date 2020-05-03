#!/bin/bash
aws cloudformation package --template queues.yml --output-template queues-export.yml --s3-bucket cloudformation-sourcebucket-ztlebfosltkjbq
aws cloudformation deploy --template-file queues-export.yml --stack-name pe-task-queues --parameter-overrides "KMSKey=public-editor-queues-key:1"
