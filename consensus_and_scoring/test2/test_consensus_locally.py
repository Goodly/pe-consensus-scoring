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

from process_dirs import (
    configure_consensus_directories,
    generate_highlighter_consensus,
    generate_datahunt_consensus,
)

def noop(*args):
    pass

fetch_tags_files = noop
fetch_highlighter_files = noop
fetch_datahunt_files = noop

def test_iaa_without_S3_fetch():
    run_iaa_only(fetch_tags_files, fetch_highlighter_files, fetch_datahunt_files)

def run_iaa_only(fetch_tags_files, fetch_highlighter_files, fetch_datahunt_files):
    output_path = "../test_output/"
    messages_path = "../test_data_persistent/request_consensus/"
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
                logger.info("---BEGIN request_consensus {}".format(filename))
                project_name = body.get('project_name', '')
                project_uuid = body.get('project_uuid', '')
                task_type = body.get('task_type', '')
                dir_dict = configure_consensus_directories(task_type, parent_dirname)
                fetch_tags_files(body, dir_dict)
                if task_type == "HLTR":
                    fetch_highlighter_files(body, dir_dict)
                    generate_highlighter_consensus(dir_dict)
                    consensus_dir = dir_dict['consensus_dir']
                elif task_type == "QUIZ":
                    fetch_datahunt_files(body, dir_dict)
                    generate_datahunt_consensus(dir_dict)
                    consensus_dir = dir_dict['adjud_dir']
                else:
                    raise Exception(u"request_consensus: Project '{}' has unknown task_type '{}'."
                                    .format(project_name, task_type))
                logger.info("---END request_consensus {}".format(filename))
