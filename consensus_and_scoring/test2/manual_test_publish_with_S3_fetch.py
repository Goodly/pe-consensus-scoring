import os
import json
import fnmatch

import logging
if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr.
    # If a handler is already configured, `.basicConfig` does not execute.
    # Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from app import handle_publish_article


def test_publish_article(fetch_remote_data=True):
    output_path = "../test_output/"
    messages_path = "../test_data_persistent/request_publish/"
    test_messages = os.listdir(messages_path)
    test_messages = sorted(fnmatch.filter(test_messages, '*.json'))
    for filename in test_messages:
        file_path = os.path.join(messages_path, filename)
        parent_dirname = "../tests/output_"
        parent_dirname = os.path.join(output_path, filename.lstrip("request_consensus_").rstrip(".json"))
        if not os.path.exists(parent_dirname):
            os.makedirs(parent_dirname)
        with open(file_path, "r") as f:
            body = json.load(f)
            if body:
                logger.info("---BEGIN request_publish_article {}".format(filename))
                response = handle_publish_article(body, parent_dirname, fetch_remote_data)
