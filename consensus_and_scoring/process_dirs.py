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

from iaa_only import iaa_only
from TriagerScoring import importData
from post_adjudicator import post_adjudicator_master

# This code does not invoke any AWS APIs such as S3 or SQS so that it can be
# imported by the test code without requiring AWS credentials.

def configure_consensus_directories(task_type, parent_dirname):
    dir_dict = {}
    dir_dict['tags_dir'] = make_dir(parent_dirname, 'tags')
    dir_dict['negative_tasks_dir'] = make_dir(parent_dirname, 'negative_tasks')
    if task_type == "HLTR":
        dir_dict['highlighters_dir'] = make_dir(parent_dirname, 'highlighters')
        dir_dict['consensus_dir']= make_dir(parent_dirname, "output_HLTR_consensus")
        clean_output_csvs(dir_dict['consensus_dir'])
    elif task_type == "QUIZ":
        dir_dict['config_path'] = './config/'
        dir_dict['texts_dir'] = make_dir(parent_dirname, 'texts')
        dir_dict['schemas_dir'] = make_dir(parent_dirname, 'schemas')
        dir_dict['datahunts_dir'] = make_dir(parent_dirname, 'datahunts')
        dir_dict['consensus_dir'] = make_dir(parent_dirname, "output_datahunt_consensus")
        dir_dict['scoring_dir'] = make_dir(parent_dirname, "output_scoring")
        dir_dict['adjud_dir'] = make_dir(parent_dirname, "output_adjud")
        clean_output_csvs(dir_dict['consensus_dir'])
        clean_output_csvs(dir_dict['scoring_dir'])
        clean_output_csvs(dir_dict['adjud_dir'])
    return dir_dict

def generate_highlighter_consensus(dir_dict):
    highlighters_dir = dir_dict['highlighters_dir']
    consensus_dir = dir_dict['consensus_dir']
    for filename in os.listdir(highlighters_dir):
        if filename.endswith(".csv"):
            input_file = os.path.join(highlighters_dir, filename)
            output_file = os.path.join(consensus_dir, "S_IAA_" + filename)
            importData(input_file, output_file)

def generate_datahunt_consensus(dir_dict):
    result_dir = iaa_only(
        dir_dict['datahunts_dir'],
        dir_dict['texts_dir'],
        dir_dict['config_path'],
        use_rep = False,
        repCSV = None,
        iaa_dir = dir_dict['consensus_dir'],
        schema_dir = dir_dict['schemas_dir'],
        adjud_dir = dir_dict['adjud_dir'],
        threshold_func = 'raw_50'
    )
    assert(result_dir == dir_dict['adjud_dir'])
    return result_dir

def configure_publish_directories(parent_dirname):
    dir_dict = {}
    dir_dict['config_path'] = './config/'
    dir_dict['tags_dir'] = make_dir(parent_dirname, 'tags')
    dir_dict['negative_tasks_dir'] = make_dir(parent_dirname, 'negative_tasks')
    dir_dict['texts_dir'] = make_dir(parent_dirname, 'texts')
    dir_dict['metadata_dir'] = make_dir(parent_dirname, 'metadata')
    dir_dict['schemas_dir'] = make_dir(parent_dirname, 'schemas')
    dir_dict['datahunts_dir'] = make_dir(parent_dirname, 'datahunts')
    dir_dict['focus_tags_dir'] = make_dir(parent_dirname, 'focus_tags')
    dir_dict['adj_tags_dir'] = make_dir(parent_dirname, 'adj_tags')
    dir_dict['adj_negative_tasks_dir'] = make_dir(parent_dirname, 'adj_negative_tasks')
    dir_dict['output_dir'] = make_dir(parent_dirname, "output_publish")
    dir_dict['iaa_temp_dir'] = make_dir(parent_dirname, "output_iaa_temp")
    dir_dict['scoring_dir'] = make_dir(parent_dirname, "output_scoring")
    dir_dict['viz_dir'] = make_dir(parent_dirname, "output_viz")
    clean_output_csvs(dir_dict['output_dir'])
    clean_output_csvs(dir_dict['iaa_temp_dir'])
    clean_output_csvs(dir_dict['scoring_dir'])
    clean_output_csvs(dir_dict['viz_dir'])
    return dir_dict

def generate_article_to_publish(dir_dict):
    threshold_function = 'raw_50'
    post_adjudicator_master(
        dir_dict['adj_tags_dir'],
        dir_dict['schemas_dir'],
        dir_dict['output_dir'],
        dir_dict['iaa_temp_dir'],
        dir_dict['datahunts_dir'],
        dir_dict['scoring_dir'],
        dir_dict['viz_dir'],
        dir_dict['focus_tags_dir'],
        dir_dict['texts_dir'],
        dir_dict['config_path'],
        threshold_function,
    )

def make_dir(parent_dirname, join_path):
    dest_dirname = os.path.join(parent_dirname, join_path)
    if not os.path.exists(dest_dirname):
        os.makedirs(dest_dirname)
    return dest_dirname

# A more conservative routine than shutil.rmtree that can lay waste
# to a nested directory tree. We're just expecting to clean out some
# CSVs in a single directory, so let's limit narrowly to that action.
def clean_output_csvs(dest_dirname):
    files = os.listdir(dest_dirname)
    csvs = fnmatch.filter(files, '*.csv')
    for filename in csvs:
        file_path = os.path.join(dest_dirname, filename)
        logger.info("Cleaning {}".format(file_path))
        os.remove(file_path)
