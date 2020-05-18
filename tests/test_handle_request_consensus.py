import os
import json
from app import handle_request_consensus

import logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

if __name__ == '__main__':

    test_messages = [
        'request_consensus_Covid_ArgumentRelevancev1.2.json',
        'request_consensus_Covid_Evidencev1.json',
        'request_consensus_Covid_Form1.0.json',
        'request_consensus_Covid_Languagev1.1.json',
        'request_consensus_Covid_Probabilityv1.json',
        'request_consensus_Covid_Reasoningv1.json',
        'request_consensus_Covid_Semantics1.0.json',
        'request_consensus_Covid_SourceRelevancev1.json',
    ]
    for filename in test_messages:
        file_path = os.path.join("../tests", filename)
        dest_dirname = "../test_iaa_"
        dest_dirname += filename[len("request_consensus_Covid_"):-len(".json")]
        if not os.path.exists(dest_dirname):
            os.makedirs(dest_dirname)
        with open(file_path, "r") as f:
            message = json.load(f)
            if message:
                handle_request_consensus(message, dest_dirname)
