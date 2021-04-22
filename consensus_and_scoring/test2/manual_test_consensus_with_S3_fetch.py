import os
import json
import fnmatch
import pytest

import logging
if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr.
    # If a handler is already configured, `.basicConfig` does not execute.
    # Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from test2.test_consensus_locally import run_iaa_only

from app import (
    fetch_tags_files,
    fetch_highlighter_files,
    fetch_datahunt_files,
)


def test_iaa_with_S3_fetch():
    run_iaa_only(fetch_tags_files, fetch_highlighter_files, fetch_datahunt_files)
