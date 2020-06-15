import os
import json
import re
from string import Template
import tempfile
from io import StringIO

import logging
if len(logging.getLogger().handlers) > 0:
    # The Lambda environment pre-configures a handler logging to stderr.
    # If a handler is already configured, `.basicConfig` does not execute.
    # Thus we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import mimetypes
mimetypes.init()
from requests.compat import urlparse
import boto3
from botocore.exceptions import ClientError

s3 = boto3.resource('s3')

def get_s3_config(s3_url):
    s3_parts = urlparse(s3_url)
    if s3_parts.scheme != "s3":
        raise Exception("{} must be an S3 URL with protocol s3://".format(s3_url))
    s3_bucket = s3_parts.netloc
    s3_prefix = s3_parts.path.lstrip('/')
    return s3_bucket, s3_prefix

unsafe_chars = re.compile(r"[^-_.0-9a-zA-Z]")

def s3_safe_path(unsafe_name, fallback):
    safe_name = re.sub(unsafe_chars, '-', unsafe_name)
    # if the name consisted entirely of unsafe characters, fallback to id.
    if safe_name == "":
        safe_name = re.sub(unsafe_chars, '-', fallback)
    return safe_name

def send_s3(viz_dir, text_dir, metadata_dir, s3_bucket, s3_prefix):
    viz_to_send = collect_files_to_send(viz_dir, text_dir)

    print("Sending visualization files to S3.")
    # Retrieve HTML template.
    with open("visualizations_src/Visualization.html", "r") as f:
        html_source = f.read()
        html_template = Template(html_source)

    for viz in viz_to_send:
        # Group visualization files into a directory that is a shortened SHA-256.
        viz_data_filename_s3 = 'viz_data.csv'
        data_s3_key = os.path.join(s3_prefix, viz['article_shorter'], viz_data_filename_s3)
        viz['data_s3_key'] = data_s3_key
        send_with_max_age(viz['data_filepath'], s3_bucket, data_s3_key,
                          ACL='public-read', max_age=10)

        article_filename_s3 = 'article.txt'
        article_s3_key = os.path.join(s3_prefix,  viz['article_shorter'], article_filename_s3)
        viz['article_s3_key'] = article_s3_key
        send_command(viz['article_filepath'], s3_bucket, article_s3_key, ACL='public-read')

        html_s3_key = os.path.join(s3_prefix,  viz['article_shorter'], 'visualization.html')
        viz['html_s3_key'] = html_s3_key
        # Since the data files are in the same directory as the html, the
        # relative url is just the filename.
        context = {
            'DATA_CSV_URL': viz_data_filename_s3,
            'ARTICLE_TEXT_URL': article_filename_s3,
        }
        # Merge template URLs into HTML file
        html_output = html_template.substitute(context)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".html", \
            encoding="utf-8", delete=True) as html_file:
            html_file.write(html_output)
            html_file.seek(0, os.SEEK_SET)
            send_command(html_file.name, s3_bucket, html_s3_key,
                         wait=True, ACL='public-read')

    send_assets("visualizations_src/assets", s3_bucket, "visualizations/assets")

    newsfeed_items = generate_newsfeed_items(viz_to_send, metadata_dir)
    update_newsfeed(newsfeed_items, s3_bucket, "newsfeed/visData.json")
    return viz_to_send

def collect_files_to_send(viz_dir, text_dir):
    # Collect the list of sha256 of by iterating over the VisualizationData_sha256.csv files
    print("Gathering files to send for visualization")
    viz_to_send = []
    for dirpath, dirnames, filenames in os.walk(viz_dir):
        for file in filenames:
            if file.endswith('.csv') and 'VisualizationData_' in file:
                # -4 to strip .csv
                article_sha256 = file.split('Data_', 1)[1][:-4]
                # Output paths will use the first 32 of 64 characters in SHA-256
                article_shorter = article_sha256[:32]
                article_filepath = os.path.join(text_dir, article_sha256 + ".txt")
                if os.path.exists(article_filepath):
                    viz = {
                        'sha_256': article_sha256,
                        'article_shorter': article_shorter,
                        'data_filepath': os.path.join(dirpath, file),
                        'data_filename': file,
                        'article_filepath': article_filepath,
                    }
                    viz_to_send.append(viz)
                else:
                    print(article_filepath, 'not found')
    return viz_to_send

def generate_newsfeed_items(viz_to_send, metadata_dir):
    newsfeed_items = []
    for viz in viz_to_send:
        metadata = {}
        metadata_filepath = os.path.join(metadata_dir, viz['sha_256'] + ".metadata.json")
        if os.path.exists(metadata_filepath):
            print("Merging metadata from {}".format(metadata_filepath))
            with open(metadata_filepath, "r") as f:
                metadata = json.load(f)
                extra = metadata.get('extra', {})
        item = {
            "article_sha256": viz['sha_256'],
            "articleHash": extra.get('articleHash', ''),
            "Title": extra.get('articleTitle', ''),
            "Author": extra.get('author', ''),
            "Date": extra.get('publishedDate', ''),
            "ID": metadata.get('article_number', ''),
            "Article Link": extra.get('url', ''),
            "Visualization Link": os.path.join("/", viz['html_s3_key']),
            "Plain Text": os.path.join("/", viz['article_s3_key']),
            "Highlight Data": os.path.join("/", viz['data_s3_key'])
        }
        newsfeed_items.append(item)
    return newsfeed_items

def update_newsfeed(newsfeed_items, s3_bucket, newsfeed_s3_key):
    # Read the current newsfeed json, add new article(s), write out.
    # Obvi, Newsfeed updates should be using a database like DynamoDB.
    newsfeed_json = load_existing_newsfeed(s3_bucket, newsfeed_s3_key)
    newsfeed_json = merge_newsfeed_articles(newsfeed_json, newsfeed_items)
    send_newsfeed(newsfeed_json, s3_bucket, newsfeed_s3_key)
    send_assets("newsfeed/assets", s3_bucket, "newsfeed/assets")
    send_command("newsfeed/newsfeed.html", s3_bucket, "newsfeed/newsfeed.html",
                 wait=True, ACL='public-read')

def send_newsfeed(newsfeed_json, s3_bucket, newsfeed_s3_key):
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", \
        encoding="utf-8", delete=True) as newsfeed_file:
        json.dump(newsfeed_json, newsfeed_file)
        newsfeed_file.seek(0, os.SEEK_SET)
        send_with_max_age(newsfeed_file.name, s3_bucket, newsfeed_s3_key,
                          wait=True, ACL='public-read', max_age=10)

def load_existing_newsfeed(s3_bucket, newsfeed_s3_key):
    newsfeed_json = []
    try:
        with tempfile.NamedTemporaryFile(mode="wb+", suffix=".json", \
            delete=True) as newsfeed_file:
                s3_obj = s3.Object(s3_bucket, newsfeed_s3_key)
                s3_obj.download_fileobj(newsfeed_file)
                newsfeed_file.seek(0, os.SEEK_SET)
                newsfeed_json = json.load(newsfeed_file)
    except ClientError as e:
        logger.info(u"While retrieving s3://{}/{}, received error {}."
                    .format(s3_bucket, newsfeed_s3_key, str(e)))
    return newsfeed_json

def merge_newsfeed_articles(newsfeed_json, newsfeed_items):
    current_items = make_article_lookup(newsfeed_json)
    update_items = make_article_lookup(newsfeed_items)
    current_items.update(update_items)
    return list(current_items.values())

def remove_newsfeed_article(article_sha256, s3_bucket, newsfeed_s3_key):
    newsfeed_json = load_existing_newsfeed(s3_bucket, newsfeed_s3_key)
    current_items = make_article_lookup(newsfeed_json)
    if article_sha256 in current_items:
        del current_items[article_sha256]
    newsfeed_json = list(current_items.values())
    send_newsfeed(newsfeed_json, s3_bucket, newsfeed_s3_key)

def make_article_lookup(newsfeed_json):
    lookup_article = {}
    for article_info in newsfeed_json:
        item_key = article_info.get('article_sha256')
        if item_key:
            lookup_article[item_key] = article_info
    return lookup_article

def send_assets(asset_dir, s3_bucket, s3_prefix):
    for dirpath, dirnames, filenames in os.walk(asset_dir):
        for asset_name in filenames:
            source_path = os.path.join(dirpath, asset_name)
            # Preserve directory nesting below asset_dir
            common_path = os.path.commonpath([asset_dir, dirpath])
            subpath = dirpath[len(common_path):].lstrip('/')
            asset_s3_key = os.path.join(s3_prefix, subpath, asset_name)
            send_command(source_path, s3_bucket, asset_s3_key,
                         ACL='public-read')

def send_with_max_age(source_filename, s3_bucket, s3_key,
                      wait=False, ACL='private', max_age=24*60*60):
    cache_control = 'max-age={:d}'.format(max_age)
    send_command(source_filename, s3_bucket, s3_key,
                 wait=wait, ACL=ACL, CacheControl=cache_control)

def send_command(source_filename, s3_bucket, s3_key,
                 wait=False, ACL='private', **kwargs):
    print("Pushing s3://{}/{}".format(s3_bucket, s3_key))
    s3_obj = s3.Object(s3_bucket, s3_key)
    (content_type, encoding_type) = mimetypes.guess_type(source_filename, strict=False)
    with open(source_filename, 'rb') as file_obj:
        s3_obj.put(
            Body=file_obj,
            ContentType=content_type,
            ACL=ACL,
            **kwargs
        )
        if wait:
            s3_obj.wait_until_exists()

def handle_unpublish_article(body, s3_bucket):
    article_sha256 = body.get('article_sha256')
    newsfeed_s3_key = "newsfeed/visData.json"
    if article_sha256:
        remove_newsfeed_article(article_sha256, s3_bucket, newsfeed_s3_key)
        article_number = body.get('article_number')
        user_message = "Removed article number {} from newsfeed.".format(article_number)
    message = {
        'Action': 'publish_article_response',
        'Version': '1',
        'user_id': body.get('user_id', 1),
        'pipeline_name': body.get('pipeline_name', 'MissingPipelineName'),
        'pipeline_uuid': body.get('pipeline_uuid'),
        'article_number': body.get('article_number'),
        'article_sha256': body.get('article_sha256'),
        'viz_s3_bucket': s3_bucket,
        'newsfeed_s3_key': newsfeed_s3_key,
        'user_message': user_message,
    }
    message['log_message'] = (u"{} for '{}'"
        .format(message['Action'], message['pipeline_name'])
    )
    return message
