import os

import logging
if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr.
    # If a handler is already configured, `.basicConfig` does not execute.
    # Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


import json
import tempfile
import gzip
import shutil
import unicodecsv

import boto3
from botocore.exceptions import ClientError

from send_to_s3 import send_s3, get_s3_config, s3_safe_path, send_command
from send_to_s3 import handle_unpublish_article

# Do setup outside of listener
sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')

S3_CONSENSUS_OUTPUT = os.getenv('S3_CONSENSUS_OUTPUT', 's3://dev.publiceditor.io/consensus')
consensus_s3_bucket, consensus_s3_prefix = get_s3_config(S3_CONSENSUS_OUTPUT)
S3_VISUALIZATION_OUTPUT = os.getenv('S3_VISUALIZATION_OUTPUT', 's3://dev.publiceditor.io/visualizations')
viz_s3_bucket, viz_s3_prefix = get_s3_config(S3_VISUALIZATION_OUTPUT)

from process_dirs import (
    configure_directories,
    generate_highlighter_consensus,
    generate_datahunt_consensus,
)

def lambda_handler(event, context):
    """
        event: will have Records with eventSource=="aws:sqs".
        Each sqs message body will have one of several messages requesting an action
        and lists of files to retrieve from S3 for processing.
        context:
        See Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
    """
    if isinstance(event.get('Records'), list):
        for record in event['Records']:
            if record.get('eventSource','') == "aws:sqs":
                body = json.loads(record.get('body','{}'))
                message_action = body.get('Action', '')
                message_version = body.get('Version', '')
                pipeline_name = body.get('pipeline_name', '')
                pipeline_uuid = body.get('pipeline_uuid', '')
                response_sqs_url = body.get('response_sqs_url', '')
                logger.info("Message '{}:{}' from pipeline '{}'"
                            .format(message_action, message_version, pipeline_name))
                with tempfile.TemporaryDirectory() as parent_dirname:
                    response = None
                    if message_action == "request_consensus" and message_version == "1":
                        response = handle_request_consensus(body, parent_dirname)
                    if message_action == "publish_article" and message_version == "1":
                        response = handle_publish_article(body, parent_dirname)
                    if message_action == "unpublish_article" and message_version == "1":
                        response = handle_unpublish_article(body, viz_s3_bucket)
                    if response:
                        sqs_response = send_pipeline_message(response_sqs_url, response)

    return {'done': True}

def handle_request_consensus(body, parent_dirname):
    logger.info("---BEGIN request_consensus handler---")
    project_name = body.get('project_name', '')
    project_uuid = body.get('project_uuid', '')
    task_type = body.get('task_type', '')
    dir_dict = configure_directories(task_type, parent_dirname)
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
    fallback_path = "00000000-0000-0000-0000-000000000000_NoProject"
    project_path = s3_safe_path(project_uuid + "_" + project_name, fallback_path)
    project_s3_prefix = os.path.join(consensus_s3_prefix, project_path)
    s3_locations = send_consensus_files(consensus_dir, consensus_s3_bucket, project_s3_prefix)
    message = build_consensus_message(body, s3_locations)
    return message

def fetch_tags_files(body, dir_dict):
    tags = body.get('Tags', [])
    negative_tasks = body.get('NegativeTasks', [])
    retrieve_file_list(tags, dir_dict['tags_dir'])
    retrieve_file_list(negative_tasks, dir_dict['negative_tasks_dir'])

def fetch_highlighter_files(body, dir_dict):
    highlighters = body.get('Highlighters', [])
    retrieve_file_list(highlighters, dir_dict['highlighters_dir'])
    logger.info("---FILES RETRIEVED SUCCESSFULLY in request_highlighter_consensus handler---")

def fetch_datahunt_files(body, dir_dict):
    texts = body.get('Texts', [])
    texts = use_article_sha256_filenames(texts)
    schemas = body.get('Schemas', [])
    datahunts = body.get('DataHunts', [])
    retrieve_file_list(texts, dir_dict['texts_dir'])
    retrieve_file_list(schemas, dir_dict['schemas_dir'])
    retrieve_file_list(datahunts, dir_dict['datahunts_dir'])
    rename_schema_files(dir_dict['schemas_dir'])
    logger.info("---FILES RETRIEVED SUCCESSFULLY in request_datahunt_consensus handler---")

def send_consensus_files(consensus_dir, consensus_s3_bucket, consensus_s3_prefix):
    s3_locations = []
    for filename in os.listdir(consensus_dir):
        if filename.endswith(".csv") or filename.endswith(".csv.gz"):
            src_path = os.path.join(consensus_dir, filename)
            dest_key = os.path.join(consensus_s3_prefix, filename)
            # Must wait for transfer because the parent dir will be deleted on exit.
            send_command(src_path, consensus_s3_bucket, dest_key, wait=True)
            s3_locations.append({
                "bucket_name": consensus_s3_bucket,
                "key": dest_key,
                "filename": filename,
            })
    return s3_locations

def build_consensus_message(body, s3_locations):
    message = {
        'Action': 'consensus_tags',
        'Version': '1',
        'Tags': s3_locations,
        'user_id': body.get('user_id', 1),
        'project_name': body.get('project_name', ''),
        'project_uuid': body.get('project_uuid', ''),
        'task_type': body.get('task_type', ''),
    }
    message['log_message'] = (u"{} for '{}'"
        .format(message['Action'], message['project_name'])
    )
    return message

def send_pipeline_message(response_sqs_url, message):
    # Expect SQS URL like
    # https://sqs.us-west-2.amazonaws.com/012345678901/pe-task-queues-InputQueue
    response = None
    log_message = message.get('log_message', '')
    if response_sqs_url:
        logger.info("Sending message {} to output queue {}."
                    .format(log_message, response_sqs_url))
        queue = sqs.Queue(response_sqs_url)
        response = queue.send_message(
            MessageBody=json.dumps(message),
        )
    else:
        logger.warn("send_pipeline_message can't send message {} "
                    "because no output queue was specified."
                    .format(log_message))
    return response

def handle_publish_article(body, parent_dirname, fetch_remote_data=True):
    logger.info("------BEGIN publish_article handler-------")
    texts = body.get('Texts', [])
    texts = use_article_sha256_filenames(texts)
    metadata_for_texts = unnest_metadata_key(texts)
    schemas = body.get('Schemas', [])
    datahunts = body.get('DataHunts', [])
    focus_tags = body.get('FocusTags', [])
    #tags = body.get('Tags', [])
    #negative_tasks = body.get('NegativeTasks', [])
    adj_tags = body.get('AdjTagsElseIAATags', [])
    adj_negative_tasks = body.get('AdjNegativeTasksElseIAA', [])
    logger.info("texts count {}".format(len(texts)))
    logger.info("metadata count {}".format(len(metadata_for_texts)))
    logger.info("schemas count {}".format(len(schemas)))
    logger.info("datahunts count {}".format(len(datahunts)))
    logger.info("focus_tags count {}".format(len(focus_tags)))
    #logger.info("tags count {}".format(len(tags)))
    #logger.info("negative_tasks count {}".format(len(negative_tasks)))
    logger.info("adj_tags count {}".format(len(adj_tags)))
    logger.info("adj_negative_tasks count {}".format(len(adj_negative_tasks)))
    texts_dir = make_dir(parent_dirname, 'texts')
    metadata_dir = make_dir(parent_dirname, 'metadata')
    schemas_dir = make_dir(parent_dirname, 'schemas')
    datahunts_dir = make_dir(parent_dirname, 'datahunts')
    focus_tags_dir = make_dir(parent_dirname, 'focus_tags')
    #tags_dir = os.path.join(parent_dirname, 'tags')
    #negative_tasks_dir = os.path.join(parent_dirname, 'negative_tasks')
    adj_tags_dir = make_dir(parent_dirname, 'adj_tags')
    adj_negative_tasks_dir = make_dir(parent_dirname, 'adj_negative_tasks')
    if fetch_remote_data:
        retrieve_file_list(texts, texts_dir)
        retrieve_file_list(metadata_for_texts, metadata_dir)
        retrieve_file_list(schemas, schemas_dir)
        retrieve_file_list(datahunts, datahunts_dir)
        retrieve_file_list(focus_tags, focus_tags_dir)
        #retrieve_file_list(tags, tags_dir)
        #retrieve_file_list(negative_tasks, negative_tasks_dir)
        retrieve_file_list(adj_tags, adj_tags_dir)
        retrieve_file_list(adj_negative_tasks, adj_negative_tasks_dir)
        rename_schema_files(schemas_dir)
        logger.info("------FILES RETRIEVED SUCCESSFULLY in publish_article handler-------")

    # additional input config data
    config_path = './config/'
    rep_file = './UserRepScores.csv'
    threshold_function = 'raw_30'
    # outputs
    output_dir = make_dir(parent_dirname, "output_publish")
    iaa_temp_dir = make_dir(parent_dirname, "output_iaa_temp")
    scoring_dir = make_dir(parent_dirname, "output_scoring")
    viz_dir = make_dir(parent_dirname, "output_viz")
    post_adjudicator_master(
        adj_tags_dir,
        schemas_dir,
        output_dir,
        iaa_temp_dir,
        datahunts_dir,
        scoring_dir,
        viz_dir,
        focus_tags_dir,
        texts_dir,
        config_path,
        threshold_function,
    )
    viz_files_sent = send_s3(viz_dir, texts_dir, metadata_dir, viz_s3_bucket, s3_prefix=viz_s3_prefix)
    message = build_published_message(body, viz_s3_bucket, viz_files_sent)
    logger.info("------END publish_article handler-------")
    return message

def build_published_message(body, viz_s3_bucket, viz_files_sent):
    viz_urls = [
        {
            'html_s3_key': viz['html_s3_key'],
            'data_s3_key': viz['data_s3_key'],
            'article_s3_key': viz['article_s3_key'],
        }
        for viz in viz_files_sent
    ]
    if len(viz_files_sent) == 1:
        viz_group = viz_files_sent[0]
        pipeline_name = body.get('pipeline_name')
        article_number = body.get('article_number')
        html_s3_key = viz_group.get('html_s3_key')
        user_message = (u"Pipeline '{}' published article {} to https://{}/{}"
            .format(
                pipeline_name, article_number, viz_s3_bucket, html_s3_key
            )
        )
    else:
        # Not expecting this to be reached. Articles sent one per message.
        pipeline_name = body.get('pipeline_name')
        user_message = (u"Pipeline '{}' published {} articles."
            .format(pipeline_name, len(viz_files_sent))
        )
    logger.info(user_message)

    message = {
        'Action': 'publish_article_response',
        'Version': '1',
        'user_id': body.get('user_id', 1),
        'pipeline_name': body.get('pipeline_name', 'MissingPipelineName'),
        'pipeline_uuid': body.get('pipeline_uuid'),
        'article_number': body.get('article_number'),
        'article_sha256': body.get('article_sha256'),
        'viz_s3_bucket': viz_s3_bucket,
        'viz_files_sent': viz_urls,
        'user_message': user_message,
    }
    message['log_message'] = (u"{} for '{}'"
        .format(message['Action'], message['pipeline_name'])
    )
    return message

def retrieve_file_list(s3_locations, dest_dirname):
    logger.info("Making dir {}".format(dest_dirname))
    if not os.path.exists(dest_dirname):
        os.makedirs(dest_dirname)
    for s3_location in s3_locations:
        retrieve_s3_file(s3_location, dest_dirname)
    return dest_dirname

def retrieve_s3_file(s3_location, dest_dirname):
    s3_bucket = s3_location['bucket_name']
    s3_key = s3_location['key']
    print("Retrieving s3://{}/{}".format(s3_bucket, s3_key))
    s3_obj = s3.Object(s3_bucket, s3_key)
    filename = os.path.basename(s3_location['filename'])
    dest_path = os.path.join(dest_dirname, filename)
    s3_obj.download_file(dest_path)
    # if gzipped, decompress
    if filename.endswith(".gz"):
        new_path = os.path.join(dest_dirname, filename.rstrip(".gz"))
        with gzip.open(dest_path, 'rb') as f_in, \
            open(new_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(dest_path)

# text file names are all "text.txt.gz" because source uses SHA-256 as parent dir.
def use_article_sha256_filenames(texts):
    for s3_location in texts:
        old_filename = s3_location['filename']
        new_filename = s3_location['article_sha256'] + ".txt"
        if old_filename.endswith(".gz"):
            new_filename += ".gz"
        s3_location['filename'] = new_filename
    return texts

# Each article location has its metadata_location nested. Unnest.
# Also need to change metadata filenames from metadata.json.gz to use SHA-256.
def unnest_metadata_key(text_locations):
    metadata_locations = []
    for article_location in text_locations:
        if article_location.get('metadata_location'):
            metadata = article_location['metadata_location']
            new_filename = article_location['article_sha256'] + ".metadata.json"
            if metadata['filename'].endswith(".gz"):
                new_filename += ".gz"
            metadata['filename'] = new_filename
            metadata_locations.append(metadata)
    return metadata_locations

# Rename schema files from the friendly name to the SHA-256 of the schema source.
def rename_schema_files(schemas_dir):
    for schema in os.listdir(schemas_dir):
        if schema.endswith(".csv"):
            filepath = os.path.join(schemas_dir, schema)
            row = None
            with open(filepath, 'rb') as csv_file:
                reader = unicodecsv.DictReader(csv_file, encoding='utf-8-sig')
                if u'schema_sha256' in reader.fieldnames:
                    row = next(reader)
            if row:
                new_path = os.path.join(schemas_dir, row['schema_sha256'] + ".csv")
                logger.info("Renaming '{}' to '{}'".format(filepath, new_path))
                os.rename(filepath, new_path)
            else:
                logger.warn("Failed to find SHA-256 for '{}'".format(filepath))
