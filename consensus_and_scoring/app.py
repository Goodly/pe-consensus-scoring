import os

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

import json
import tempfile
import gzip
import shutil
import unicodecsv
#import pathlib # Python >= 3.5

import boto3
from botocore.exceptions import ClientError

from iaa_only import iaa_only
from TriagerScoring import importData
from master import calculate_scores_master

# Do setup outside of listener
sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')

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
                logger.info("Message '{}:{}' from pipeline '{}'"
                            .format(message_action, message_version, pipeline_name))
                with tempfile.TemporaryDirectory() as parent_dirname:
                    if message_action == "notify_all" and message_version == "1":
                        handle_notify_all(body, parent_dirname)
                    if message_action == "request_consensus" and message_version == "1":
                        handle_request_consensus(body, parent_dirname)

    return {'done': True}

def handle_request_consensus(body, parent_dirname):
    logger.info("---BEGIN request_consensus handler---")
    project_name = body.get('project_name', '')
    project_uuid = body.get('project_uuid', '')
    task_type = body.get('task_type', '')

    tags = body.get('Tags', [])
    negative_tasks = body.get('NegativeTasks', [])
    tags_dir = os.path.join(parent_dirname, 'tags')
    negative_tasks_dir = os.path.join(parent_dirname, 'negative_tasks')
    retrieve_file_list(tags, tags_dir)
    retrieve_file_list(negative_tasks, negative_tasks_dir)

    if task_type == "HLTR":
        handle_highlighter_consensus(body, parent_dirname)
    elif task_type == "QUIZ":
        handle_datahunt_consensus(body, parent_dirname)
    else:
        raise Exception(u"request_consensus: Project '{}' has unknown task_type '{}'."
                        .format(project_name, task_type))
    logger.info("---END request_consensus handler---")

def handle_highlighter_consensus(body, parent_dirname):
    highlighters = body.get('Highlighters', [])
    highlighters_dir = os.path.join(parent_dirname, 'highlighters')
    retrieve_file_list(highlighters, highlighters_dir)
    logger.info("highlighters count {}".format(len(highlighters)))
    logger.info("---FILES RETRIEVED SUCCESSFULLY in request_highlighter_consensus handler---")
    output_dir = tempfile.mkdtemp(dir=parent_dirname)
    for filename in os.listdir(highlighters_dir):
        if filename.endswith(".csv"):
            input_file = os.path.join(highlighters_dir, filename)
            output_file = os.path.join(output_dir, "S_IAA_" + filename)
            importData(input_file, output_file)

def handle_datahunt_consensus(body, parent_dirname):
    schemas = body.get('Schemas', [])
    datahunts = body.get('DataHunts', [])
    schemas_dir = os.path.join(parent_dirname, 'schemas')
    datahunts_dir = os.path.join(parent_dirname, 'datahunts')
    retrieve_file_list(schemas, schemas_dir)
    retrieve_file_list(datahunts, datahunts_dir)
    rename_schema_files(schemas_dir)
    logger.info("schemas count {}".format(len(schemas)))
    logger.info("datahunts count {}".format(len(datahunts)))
    logger.info("---FILES RETRIEVED SUCCESSFULLY in request_datahunt_consensus handler---")
    config_path = './config/'
    rep_file = './UserRepScores.csv'
    output_dir = tempfile.mkdtemp(dir=parent_dirname)
    scoring_dir = tempfile.mkdtemp(dir=parent_dirname)
    iaa_only(
        datahunts_dir,
        config_path,
        use_rep = False,
        repCSV = None,
        iaa_dir = output_dir,
        schema_dir = schemas_dir,
        scoring_dir = scoring_dir,
        threshold_func = 'raw_30'
    )

def handle_notify_all(body, parent_dirname):
    logger.info("------BEGIN notify_all handler-------")
    texts = body.get('Texts', [])
    texts = use_article_sha256_filenames(texts)
    metadata_for_texts = unnest_metadata_key(texts)
    schemas = body.get('Schemas', [])
    datahunts = body.get('DataHunts', [])
    tags = body.get('Tags', [])
    negative_tasks = body.get('NegativeTasks', [])
    logger.info("texts count {}".format(len(texts)))
    logger.info("metadata count {}".format(len(metadata_for_texts)))
    logger.info("schemas count {}".format(len(schemas)))
    logger.info("datahunts count {}".format(len(datahunts)))
    logger.info("tags count {}".format(len(tags)))
    logger.info("negative_tasks count {}".format(len(negative_tasks)))
    texts_dir = os.path.join(parent_dirname, 'texts')
    metadata_dir = os.path.join(parent_dirname, 'metadata')
    schemas_dir = os.path.join(parent_dirname, 'schemas')
    datahunts_dir = os.path.join(parent_dirname, 'datahunts')
    tags_dir = os.path.join(parent_dirname, 'tags')
    negative_tasks_dir = os.path.join(parent_dirname, 'negative_tasks')
    retrieve_file_list(texts, texts_dir)
    retrieve_file_list(metadata_for_texts, metadata_dir)
    retrieve_file_list(schemas, schemas_dir)
    retrieve_file_list(datahunts, datahunts_dir)
    retrieve_file_list(tags, tags_dir)
    retrieve_file_list(negative_tasks, negative_tasks_dir)
    rename_schema_files(schemas_dir)
    logger.info("------FILES RETRIEVED SUCCESSFULLY in notify_all handler-------")

    # additional input config data
    config_path = './config/'
    rep_file = './UserRepScores.csv'
    threshold_function = 'raw_30'
    # outputs
    s3_bucket = 'articles.publiceditor.io'
    s3_prefix = 'visualizations'
    output_dir = tempfile.mkdtemp(dir=parent_dirname)
    scoring_dir = tempfile.mkdtemp(dir=parent_dirname)
    viz_dir = tempfile.mkdtemp(dir=parent_dirname)
    calculate_scores_master(
        datahunts_dir,
        texts_dir,
        config_path,
        schema_dir = schemas_dir,
        iaa_dir = output_dir,
        scoring_dir = scoring_dir,
        repCSV = rep_file,
        viz_dir = viz_dir,
        s3_bucket = s3_bucket,
        s3_prefix = s3_prefix,
        threshold_func = threshold_function,
        tua_dir = tags_dir,
        metadata_dir = metadata_dir
    )
    logger.info("------END notify_all handler-------")

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

# To use, send a message with TagWorks pipeline.sqs_notify.notify_all
# and then call this to receive. Must use a queue that is NOT attached to
# a lambda that will consume the event first.
# Or to bypass queue testing, save the JSON from
# pipeline.sqs_notify.build_notify_all_message and pass that to handle_notify_all.
def test_receive_notify_all(input_SQS_url):
    # SQS URL like 'https://sqs.us-west-2.amazonaws.com/012345678901/public-editor-covid'
    queue = sqs.Queue(input_SQS_url)
    # Long poll. Recommend queue be configured to longest poll time of 20 seconds.
    messages = queue.receive_messages()
    for message in messages:
        body = json.loads(message.body)
        if body.get('Action', '') == "notify_all" and body.get('Version','') == "1":
            handle_notify_all(body)
        message.delete()


if __name__ == '__main__':
    pass
