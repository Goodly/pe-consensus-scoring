import os
import json
import argparse

from app import handle_publish_article, send_pipeline_message

import logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

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
    with open("../tests/request_publish_p1-a146.json", "r") as f:
        message = json.load(f)
        response = handle_publish_article(message, dest_dirname)
        if response:
            sqs_response = send_pipeline_message(response_sqs_url, response)
