import os
import pandas as pd
import json
from dataV3 import getAnsNumberFromLabel
from dataV3 import  getQuestionNumberFromLabel
#workflow

#import all s_iaa, concat into a single df
#import all tags, concat into a single df
#Do the operations to get the rest of the things and add that to the df
#Split the df into component parts before dependency.py
ADJUDICATED_AGREEMENT_SCORE = .8
def import_tags(tags_dir, schema_dir, output_dir):
    '''
    old_s_iaa_dir is directory to the output of iaa before it got sent to the adjudicator
    tags_dir is directory to adjudicator output
    schema_dir is directory to where the schemas are held, should be same location as when IAA was run
    output_dir is where the updated S_IAA files will be sent to

    Requires every iaa file have namespaces that are unique to each schema
    if a question didn't pass IAA, it won't have its fields corrected
    output file naming convention is 'S_IAA'+schema_namespace
    '''
    tag_files = []
    for root, dir, files in os.walk(tags_dir):
        for file in files:
            if file.endswith('.csv') and 'adj' in file.lower():
                if 'DataHunt' in file:
                    tag_files.append(tags_dir+'/'+file)


    schema_files = []
    for root, dir, files in os.walk(schema_dir):
        for file in files:
            if file.endswith('.csv'):
                schema_files.append(schema_dir + '/' + file)
    temp_dfs = []
    for i in range(len(tag_files)):
        temp_dfs.append(pd.read_csv(tag_files[i]))

    tags = pd.concat(temp_dfs)
    temp_dfs = []
    for i in range(len(schema_files)):
        temp_dfs.append(pd.read_csv(schema_files[i]))
    schema = pd.concat(temp_dfs)
    #namespace_to_schema = make_namespace_to_schema_dict(tags, iaa, schema_dir)
    tags['question_Number'] = 'ERICYOUMISSEDASPOT'
    tags['agreed_Answer'] = 'ERICYOUMISSEDASPOT'
    tags['namespace'] = 'ERICYOUMISSEDASPOT'
    tags['schema_sha256'] = 'ERICYOUMISSEDASPOT'
    tags['tua_uuid'] = 'ERICYOUMISSEDASPOT'
    tags['agreement_score'] = 'ERICYOUMISSEDASPOT'
    tags['highlighted_indices'] = 'L'
    tags['alpha_unitizing_score'] = 'N/A'

    for i in range(len(tags.index)):
        a_uid = tags['answer_uuid'].iloc[i]
        if a_uid == 0 or a_uid == '0' or a_uid == 'XXX':
            answer_number = 'L'
            question_number = 'L'
            namespace = 'L'
            schema_sha = 'L'
            tua_id = 'L'
            agreement_score = 0
        else:
            schem_row = schema[schema['answer_uuid'] == a_uid]
            if len(schem_row.index) == 0:
                raise Exception("no schema has answer_uuid matching the tag")
            a_label = schem_row['answer_label'].iloc[0]
            answer_number = getAnsNumberFromLabel(a_label)
            question_number = getQuestionNumberFromLabel(a_label)
            namespace = schem_row['namespace'].iloc[0]
            schema_sha = schem_row['schema_sha256'].iloc[0]
            task_id = tags['source_task_uuid'].iloc[i]
            extra = json.loads(tags['extra'].iloc[i])
            tua_id = extra['tua_uuid']
            agreement_score = extra['agreement_score']
            start = tags['start_pos'].iloc[i]
            end = tags['end_pos'].iloc[i]
            highlight_indices = list(range(start,end+1))
        tags.iloc[i, tags.columns.get_loc('question_Number')] = question_number
        tags.iloc[i, tags.columns.get_loc('agreed_Answer')] = answer_number
        tags.iloc[i, tags.columns.get_loc('namespace')] = namespace
        tags.iloc[i, tags.columns.get_loc('schema_sha256')] = schema_sha
        tags.iloc[i, tags.columns.get_loc('tua_uuid')] = tua_id
        tags.iloc[i, tags.columns.get_loc('agreement_score')] = agreement_score
        tags.iloc[i, tags.columns.get_loc('highlighted_indices')] = json.dumps(highlight_indices)
    for n in tags['namespace'].unique():
        out = tags[tags['namespace'] == n]
        out_path = output_dir+"S_IAA"+n+".csv"
        print('OUTPUTTING', out_path)
        out.to_csv(out_path)


    return None

def make_namespace_to_schema_dict(tags, iaa, schema_dir):
    names = tags['namespace'].unique()
    print('namespaces', names)
    dict = {}
    for n in names:
        iaa_match = iaa[iaa['namespace'] == n]
        schema_uuid = iaa_match['schema_sha256'].iloc[0]
        schema_df = pd.read_csv(schema_dir+schema_uuid+".csv")
        dict[n] = schema_df
    return dict
if __name__ == '__main__':
    old_s_iaa_dir = '../data/out_iaa/'
    tags_dir = '../data/output_tags/'
    schema_dir = '../data/schemas/'
    output_dir = '../data/adjudicated_iaa/'
    import_tags(old_s_iaa_dir, tags_dir, schema_dir, output_dir)
