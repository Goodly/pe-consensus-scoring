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

from test2.test_publish_article_locally import run_publish_article

from app import fetch_publish_files

def test_publish_local_only():
    run_publish_article(run_publish_article)
