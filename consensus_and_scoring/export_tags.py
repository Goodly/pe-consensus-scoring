import os
import pandas as pd
import csv
import json
from math import floor
from dataV3 import get_indices_hard
from dataV3 import  get_path
from Separator import  indicesToStartEnd

def export_datahunt_tags(path, outdir = None):
    if outdir ==None:
        outdir = path
    dep_iaa = []
    for dirpath, dirnames, files in os.walk(path):
        for file in files:
            print("IAA OUTPUT",file)
            if file.endswith('.csv') and 'iaa' in file.lower() and 'adj' not in file.lower():
                file_path = os.path.join(path, file)
                dep_iaa.append(file_path)
    for f in dep_iaa:
        formatFile(f, outdir)
    return

def formatFile(filePath, outdir):
    print("FORMATTING<", filePath)
    dep_iaa_dtypes = {
        'case_number': 'Int64',
        'start_pos': 'Int64',
        'end_pos': 'Int64',
    }
    df = pd.read_csv(filePath, dtype=dep_iaa_dtypes, encoding='utf-8')
    out = pd.DataFrame()
    for i in range(len(df)):
        row = df.iloc[i]
        indices = row['highlighted_indices']
        indices = get_indices_hard(indices)
        starts, ends, c = indicesToStartEnd(indices)
        texts = row['target_text']
        #print("txts:", texts)
        answerid = row['answer_uuid']

        # print(c)
        if starts[0] < 0:
            starts[0] = 0
            ends[0] = 0
            texts = ['']

        #print(texts)
        elif pd.isna(texts):
            #print("text not found")
            texts = 'target_textshould be empty here//break//'+50*'k//break//'
        if type(texts) == str:
            texts = texts.split('//break//')
        for j in range(len(starts)):
            newrow = row
            newrow['case_number'] = 0 #no case numbers in datahunts
            # For Data Hunts, topic_name should be a friendly name,
            # but unique to the answer in the schema.
            # Usually, T1.Q3.A4 for topic 1, question 3, answer 4.
            # For now, just use first 8 chars of answer uuid.
            en = ends[j]
            if type(en) != int:
                print("When exporting tags, end_pos value {} should be an int, but is a {}"
                      .format(en, type(en))
                )
            #print(row['answer_uuid'])
            try:
                newrow['topic_name'] = row['answer_uuid'][:8]
            except IndexError:
                newrow['topic_name'] = row['answer_uuid']
            newrow['highlight_count'] = len(starts)
            newrow['start_pos'] = starts[j]
            newrow['end_pos'] = en
            newrow['target_text'] = texts[j].encode('unicode-escape').decode('utf-8')
            extra = {
                'agreement_score': row['agreement_score'],
                'tua_uuid': row['tua_uuid']
            }
            newrow['extra'] = json.dumps(extra)
            out = out.append(newrow)
    out['end_pos2'] = out['end_pos'].apply(floor)
    out.astype({'end_pos2':'Int64'})
    #out['end_pos'] = out['end_pos'].apply(int)
    path, name = get_path(filePath)
    dest_path = os.path.join(outdir, "Adj_" + name)
    print("outputting TagWorks tag format to ", dest_path)
    #out.to_csv(dest_path, encoding='utf-8')
    #take it out of pandas so we can make it not be a float.
    for_csv = [['article_filename', 'article_sha256', 'source_task_uuid',
                'namespace', 'topic_name', 'case_number', 'start_pos', 'end_pos',
                'target_text', 'answer_uuid', 'answer_text', 'extra']]
    for i in range(len(out['namespace'])):
        article_filename = out['article_filename'].iloc[i]
        article_sha256 = out['article_sha256'].iloc[i]
        source_task_uuid = out['source_task_uuid'].iloc[i]
        namespace = out['namespace'].iloc[i]
        case_number = 0
        start_pos = int(out['start_pos'].iloc[i])
        end_pos = int(out['end_pos'].iloc[i])
        target_text = out['target_text'].iloc[i]
        answer_uuid = out['answer_uuid'].iloc[i]
        topic_name = out['topic_name'].iloc[i]
        answer_text = out['answer_text'].iloc[i]
        extra = out['extra'].iloc[i]
        for_csv.append([article_filename, article_sha256, source_task_uuid,
                        namespace, topic_name, case_number, start_pos, end_pos,
                        target_text, answer_uuid, answer_text, extra])
    with open(dest_path, 'w', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(for_csv)
    return

if __name__ == '__main__':
    output_tags_dir = '../data/output_tags/'
    if not os.path.exists(output_tags_dir):
        os.makedirs(output_tags_dir)
    export_datahunt_tags('../data/out_iaa/', output_tags_dir)
