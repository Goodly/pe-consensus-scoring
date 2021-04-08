import os
import json
import argparse

import logging
if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr.
    # If a handler is already configured, `.basicConfig` does not execute.
    # Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from app import (
    fetch_tags_files,
    fetch_highlighter_files,
    generate_highlighter_consensus,
    fetch_datahunt_files,
    generate_datahunt_consensus,
)

import logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

def test_iaa_only():
    test_messages = [
#        'request_consensus_t2826.json',
         'request_consensus_t2260.json',
         'request_consensus_t2225.json',
         'request_consensus_t2327.json',
         'request_consensus_t2255.json',
         'request_consensus_t2256.json',
         'request_consensus_Covid_Form1.0.json',
         'request_consensus_Covid_Semantics1.0.json',
         'request_consensus_Covid_ArgumentRelevancev1.2.json',
         'request_consensus_Covid_Evidencev1.json',
         'request_consensus_Covid_Languagev1.1.json',
         'request_consensus_Covid_Probabilityv1.json',
         'request_consensus_Covid_Reasoningv1.json',
         'request_consensus_Covid_SourceRelevancev1.json',
    ]
    for filename in test_messages:
        file_path = os.path.join("../tests", filename)
        parent_dirname = "../tests/output_"
        parent_dirname += filename[len("request_consensus_"):-len(".json")]
        if not os.path.exists(parent_dirname):
            os.makedirs(parent_dirname)
        with open(file_path, "r") as f:
            body = json.load(f)
            if body:
                logger.info("---BEGIN request_consensus handler---")
                project_name = body.get('project_name', '')
                project_uuid = body.get('project_uuid', '')
                task_type = body.get('task_type', '')
                if task_type == "HLTR":
                    dir_dict = fetch_highlighter_files(body, parent_dirname)
                    fetch_tags_files(body, parent_dirname, dir_dict)
                    generate_highlighter_consensus(dir_dict)
                    consensus_dir = dir_dict['consensus_dir']
                elif task_type == "QUIZ":
                    dir_dict = fetch_datahunt_files(body, parent_dirname)
                    fetch_tags_files(body, parent_dirname, dir_dict)
                    generate_datahunt_consensus(dir_dict)
                    consensus_dir = dir_dict['adjud_dir']
                else:
                    raise Exception(u"request_consensus: Project '{}' has unknown task_type '{}'."
                                    .format(project_name, task_type))
                logger.info("---END request_consensus handler---")
