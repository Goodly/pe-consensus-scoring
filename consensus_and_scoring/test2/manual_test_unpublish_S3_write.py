import os
import json
import argparse

from app import send_pipeline_message
from send_to_s3 import handle_unpublish_article

import logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

from app import get_s3_config
S3_VISUALIZATION_OUTPUT = os.getenv('S3_VISUALIZATION_OUTPUT', 's3://dev.publiceditor.io/visualizations')
viz_s3_bucket, viz_s3_prefix = get_s3_config(S3_VISUALIZATION_OUTPUT)

def load_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-q', '--sqs-queue',
        help='SQS Queue URL to send response to.'
    )
    return parser.parse_args()

if __name__ == '__main__':
    args = load_args()
    response_sqs_url = ''
    if args.sqs_queue:
        response_sqs_url = args.sqs_queue

    dest_dirname = "../data"
    if not os.path.exists(dest_dirname):
        os.makedirs(dest_dirname)
    with open("../tests/request_unpublish_p1-a146.json", "r") as f:
        message = json.load(f)
        response = handle_unpublish_article(message, viz_s3_bucket)
        if response:
            sqs_response = send_pipeline_message(response_sqs_url, response)
