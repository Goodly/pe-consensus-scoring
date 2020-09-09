import os
import uuid
import hashlib
import random
import json
import pandas as pd

def get_config():
    with open('test_config.json') as json_file:
        data = json.load(json_file)
    return data
config = get_config()

def make_test_directory(config, directory):
    directory = os.path.join(config["test_dir"], directory)
    if directory[-1] != '/':
        directory = directory +'/'
    try:
        os.mkdir(directory)
    except FileExistsError:
        pass
    return directory

def make_uuid():
    return str(uuid.uuid4())

def make_sha256(input):
    if input == None:
        input = str(random.getrandbits(256))
    return hashlib.sha256(input.encode('utf-8')).hexdigest()

def make_number(max = 10000):
    return random.randrange(max)

def count_matching_rows(df, params):
    for col in params.keys():
        if len(df) < 1:
            return 0
        val = params[col]
        if type(val) != list and type(val) != tuple:
            val = [val]
        df = df.loc[df[col].isin(val)]
    return len(df)

#identical functionin conftest.py, called config but this isn't a ficture

def sha256_from_namespace(namespace):
    dict = make_schema_namespace_sha256_map()
    print(dict)
    return dict[namespace]

schema_sha_256_map = None
def make_schema_namespace_sha256_map():
    global schema_sha_256_map
    if schema_sha_256_map:
        return schema_sha_256_map
    data_path = config['data_dir']
    #todo get this wokring with os.path.join
    schema_dir = data_path+ '/schemas'
    out = {}
    for root, dir, files in os.walk(schema_dir):
        for file in files:
            df = pd.read_csv(os.path.join(schema_dir, file),encoding='utf-8')
            sha_256 = df['schema_sha256'].iloc[0]
            namespace = df['namespace'].iloc[0]
            out[namespace] = sha_256
    schema_sha_256_map = out
    return out

def get_schema_data(schema_sha256, question, answer):
    data_path = config['data_dir']
    schema_dir = data_path+ '/schemas'
    schema_file_path = os.path.join(schema_dir, schema_sha256+'.csv')
    schema_df = pd.read_csv(schema_file_path, encoding='utf-8')
    label = 'T1.Q'+str(question)+'.A'+str(answer)
    schema_row = schema_df[schema_df['answer_label'] == label]
    if len(schema_row)!=1:
        label = 'T1.Q' + str(question)
        schema_row = schema_df[schema_df['question_label'] == label]
        if len(schema_row) != 1:
            return 'cantfind', 'cantfind', 'cantfind'
        return 'cantfind', 'cantfind', schema_row['question_text'].iloc[0]
    return schema_row['answer_uuid'].iloc[0], schema_row['answer_content'].iloc[0], schema_row['question_text'].iloc[0]

