import os
import uuid
import hashlib
import random
import json
import string
import pandas as pd

#functions to help setup testing 
def get_config():
    with open('../test/test_config.json') as json_file:
        data = json.load(json_file)
    return data

def make_overall_test_dir(config):
    directory = config['test_dir']
    try:
        os.mkdir(directory)
    except FileExistsError:
        pass
    return directory
#*****************************
#Useful global vars
config = get_config()
texts_dir = config['test_dir']+config['texts_dir']
make_overall_test_dir(config)

#------------
#Functions to make test-writing easier
def make_test_directory(config, directory):
    directory = config["test_dir"]+directory
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
    test_schema_dir = config['persistent_test_dir']+'/schemas'
    for root, dir, files in os.walk(test_schema_dir):
        for file in files:
            df = pd.read_csv(os.path.join(test_schema_dir, file),encoding='utf-8')
            sha_256 = df['schema_sha256'].iloc[0]
            namespace = df['namespace'].iloc[0]
            out[namespace] = sha_256
    schema_sha_256_map = out
    return out

def get_schema_df(schema_sha256):
    data_path = config['data_dir']

    try:
        schema_dir = data_path + '/schemas'
        schema_file_path = os.path.join(schema_dir, schema_sha256 + '.csv')
        schema_df = pd.read_csv(schema_file_path, encoding='utf-8')
    except FileNotFoundError:
        schema_dir = config['persistent_test_dir']+'/schemas'
        schema_file_path = os.path.join(schema_dir, schema_sha256 + '.csv')
        schema_df = pd.read_csv(schema_file_path, encoding='utf-8')
    return schema_df
def get_schema_data(schema_sha256, question, answer):

    schema_df = get_schema_df(schema_sha256)
    label = 'T1.Q'+str(question)+'.A'+str(answer)
    schema_row = schema_df[schema_df['answer_label'] == label]
    if len(schema_row)!=1:
        label = 'T1.Q' + str(question)
        schema_row = schema_df[schema_df['question_label'] == label]
        if len(schema_row) != 1:
            return 'cantfind', 'cantfind', 'cantfind'
        return 'cantfind', 'cantfind', schema_row['question_text'].iloc[0]
    return schema_row['answer_uuid'].iloc[0], schema_row['answer_content'].iloc[0], schema_row['question_text'].iloc[0]

def schema_has_hl(schema_sha256, answer_label):
    schema_df = get_schema_df(schema_sha256)
    schema_row = schema_df[schema_df['answer_label'] == answer_label]
    if len(schema_row) != 0:
        if schema_row['highlight'].iloc[0] == 0:
            return False
    return True

def get_schema_col_val(schema_sha256, column):
    '''
    returns the first value in a column from the schema file
    '''
    schema_df = get_schema_df(schema_sha256)
    return schema_df[column].iloc[0]
    label = 'T1.Q'+str(question)+'.A'+str(answer)
    schema_row = schema_df[schema_df['answer_label'] == label]
    if len(schema_row)!=1:
        label = 'T1.Q' + str(question)
        schema_row = schema_df[schema_df['question_label'] == label]
        if len(schema_row) != 1:
            return 'cantfind', 'cantfind', 'cantfind'
        return 'cantfind', 'cantfind', schema_row['question_text'].iloc[0]
    return schema_row['answer_uuid'].iloc[0], schema_row['answer_content'].iloc[0], schema_row['question_text'].iloc[0]

def make_text_data(sha_256, length=3000, text=None):
    '''
    creates a text file of random characters of the given length in the texts folder of the test_data directory
    '''

    out_dir = make_test_directory(config, config['texts_dir'])
    out_path = out_dir+sha_256+'.txt'
    if text==None:
        text =''
        for i in range(length):
            if i % 50 == 49:
                text+= '\n'
            else:
                text += random.choice(string.ascii_letters)
    out_file = open(out_path, 'w', encoding='utf-8')
    out_file.write(text)
    out_file.close()
    return out_path

def open_text_file(article_sha, start, end):
    with open(os.path.join(texts_dir, article_sha + ".txt"), 'r', encoding='utf-8') as file:
        sourceText = file.read()
    return sourceText[start:end]

def make_highlight_indices(start, end):
    return json.dumps(list(range(start,end)))
if __name__ == '__main__':
    print(get_schema_data('a6eca3a8c143a31e7b37337f744912e6d33c5b2f49cd29570daee3cfdb9a017c', '1',1))