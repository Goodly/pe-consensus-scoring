import os

import logging
logger = logging.getLogger(__name__)

import json
import tempfile
import gzip
import shutil
import unicodecsv
#import pathlib # Python >= 3.5

import boto3
from botocore.exceptions import ClientError

from master import calculate_scores_master

# Do setup outside of listener
sqs = boto3.resource('sqs')
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    """
    Parameters
    ----------
    sqs_message: dict, required
        SQS Receive Message

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Sends to output queue.
    Returns None
    ------

    """
    if isinstance(event.get('Records'), list):
        for record in event['Records']:
            if record.get('eventSource','') == "aws:sqs":
                body = json.loads(record.get('body','{}'))
                if body.get('Action', '') == "notify_all" and body.get('Version','') == "1":
                    handle_notify_all(body)

    return {'done': True}

def handle_notify_all(body):
    logger.info("------BEGIN notify_all handler-------")
    pipeline_name = body.get('pipeline_name', '')
    pipeline_uuid = body.get('pipeline_uuid', '')
    message_action = body.get('Action', '')
    message_version = body.get('Version', '')
    logger.info(u"Message '{}:{}' from pipeline '{}'"
                .format(message_action, message_version, pipeline_name))
    texts = body.get('Texts', [])
    texts = use_article_sha256_filenames(texts)
    schemas = body.get('Schemas', [])
    datahunts = body.get('DataHunts', [])
    tags = body.get('Tags', [])
    negative_tasks = body.get('Negative Tasks', [])
    logger.info(u"texts count {}".format(len(texts)))
    logger.info(u"schemas count {}".format(len(schemas)))
    logger.info(u"datahunts count {}".format(len(datahunts)))
    logger.info(u"tags count {}".format(len(tags)))
    logger.info(u"negative_tasks count {}".format(len(negative_tasks)))
    with tempfile.TemporaryDirectory() as parent_dirname:
        texts_dir = os.path.join(parent_dirname, 'texts')
        schemas_dir = os.path.join(parent_dirname, 'schemas')
        datahunts_dir = os.path.join(parent_dirname, 'datahunts')
        tags_dir = os.path.join(parent_dirname, 'tags')
        negative_tasks_dir = os.path.join(parent_dirname, 'negative_tasks')
        retrieve_file_list(texts, texts_dir)
        retrieve_file_list(schemas, schemas_dir)
        retrieve_file_list(datahunts, datahunts_dir)
        retrieve_file_list(tags, tags_dir)
        retrieve_file_list(negative_tasks, negative_tasks_dir)
        rename_schema_files(schemas_dir)

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
            tua_dir = tags_dir
        )
    logger.info("------END notify_all handler-------")

def retrieve_file_list(s3_locations, dest_dirname):
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

# text file names are all "text.txt.gz" because source uses SHA256 as parent dir.
def use_article_sha256_filenames(texts):
    for s3_location in texts:
        old_filename = s3_location['filename']
        new_filename = s3_location['article_sha256'] + ".txt"
        if old_filename.endswith(".gz"):
            new_filename += ".gz"
        s3_location['filename'] = new_filename
    return texts

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
                logger.info(u"Renaming '{}' to '{}'".format(filepath, new_path))
                os.rename(filepath, new_path)
            else:
                logger.warn(u"Failed to find SHA-256 for '{}'".format(filepath))

def test_receive_notify_all(input_SQS_url):
    # SQS URL like 'https://sqs.us-west-2.amazonaws.com/012345678901/public-editor-covid'
    queue = sqs.Queue(input_SQS_url)
    # Long poll. Recommend queue be configured to longest poll time of 20 seconds.
    messages = queue.receive_messages()
    for message in messages:
        lambda_handler(message, {})
        message.delete()

if __name__ == '__main__':
    pass
