import os
from string import Template
import tempfile
import mimetypes
mimetypes.init()
import boto3

s3 = boto3.resource('s3')

def send_s3(viz_dir, text_dir, s3_bucket, s3_prefix):
    print("Pushing to s3")
    # Collect the list of sha256 of by iterating over the VisualizationData_sha256.csv files
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


    # Retrieve HTML template.
    with open("visualizations/Visualization.html", "r") as f:
        html_source = f.read()
        html_template = Template(html_source)

    for viz in viz_to_send:
        # Group visualization files into a directory that is a shortened SHA-256.
        viz_data_filename_s3 = 'viz_data.csv'
        data_s3_key = os.path.join(s3_prefix, viz['article_shorter'], viz_data_filename_s3)
        send_command(viz['data_filepath'], s3_bucket, data_s3_key)

        article_filename_s3 = 'article.txt'
        article_s3_key = os.path.join(s3_prefix,  viz['article_shorter'], article_filename_s3)
        send_command(viz['article_filepath'], s3_bucket, article_s3_key)

        html_s3_key = os.path.join(s3_prefix,  viz['article_shorter'], 'visualization.html')
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
            send_command(html_file.name, s3_bucket, html_s3_key, wait=True)

    send_assets("visualizations/assets", s3_bucket, "visualizations/assets")

def send_assets(asset_dir, s3_bucket, s3_prefix):
    for dirpath, dirnames, filenames in os.walk(asset_dir):
        for asset_name in filenames:
            source_path = os.path.join(dirpath, asset_name)
            # Preserve directory nesting below asset_dir
            common_path = os.path.commonpath([asset_dir, dirpath])
            subpath = dirpath[len(common_path):].lstrip('/')
            asset_s3_key = os.path.join(s3_prefix, subpath, asset_name)
            send_command(source_path, s3_bucket, asset_s3_key)

def send_command(source_filename, s3_bucket, s3_key, wait=False):
    print("Pushing s3://{}/{}".format(s3_bucket, s3_key))
    s3_obj = s3.Object(s3_bucket, s3_key)
    (content_type, encoding_type) = mimetypes.guess_type(source_filename, strict=False)
    with open(source_filename, 'rb') as file_obj:
        s3_obj.put(
            Body=file_obj,
            ContentType=content_type,
            ACL='public-read',
        )
        if wait:
            s3_obj.wait_until_exists()
