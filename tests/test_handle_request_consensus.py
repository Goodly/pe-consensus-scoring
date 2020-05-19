import os
import json
import argparse

from app import handle_request_consensus, send_pipeline_message

import logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

def test_iaa_only(response_sqs_url = ''):
    test_messages = [
        'request_consensus_Covid_Form1.0.json',
        'request_consensus_Covid_Semantics1.0.json',
#       'request_consensus_Covid_ArgumentRelevancev1.2.json',
#       'request_consensus_Covid_Evidencev1.json',
#       'request_consensus_Covid_Languagev1.1.json',
#       'request_consensus_Covid_Probabilityv1.json',
#       'request_consensus_Covid_Reasoningv1.json',
#       'request_consensus_Covid_SourceRelevancev1.json',
    ]
    for filename in test_messages:
        file_path = os.path.join("../tests", filename)
        dest_dirname = "../data_iaa_"
        dest_dirname += filename[len("request_consensus_Covid_"):-len(".json")]
        if not os.path.exists(dest_dirname):
            os.makedirs(dest_dirname)
        with open(file_path, "r") as f:
            message = json.load(f)
            if message:
                response = handle_request_consensus(message, dest_dirname)
                print(json.dumps(response, indent=2))
                sqs_response = send_pipeline_message(response_sqs_url, response)

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
    test_iaa_only(response_sqs_url)
