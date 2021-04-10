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

from test2.manual_test_publish_with_S3_fetch import test_publish_article

def test_publish_local_only():
    test_publish_article(fetch_remote_data=False)
