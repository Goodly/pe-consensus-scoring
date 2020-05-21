import os
import pandas as pd
import csv
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
            if file.endswith('.csv') and 'Dep' in file and 'Adj' not in file:
                if 'Dep_S_IAA' in file:
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
            texts = 'target_textshould be empty here'+6*'k//break//'
        if type(texts) == str:
            texts = texts.split('//break//')
        print(starts, ends, texts)
        for j in range(len(starts)):
            newrow = row
            newrow['case_number'] = 0 #no case numbers in datahunts
            # For Data Hunts, topic_name should be a friendly name,
            # but unique to the answer in the schema.
            # Usually, T1.Q3.A4 for topic 1, question 3, answer 4.
            # For now, just use first 8 chars of answer uuid.
            en = ends[j]
            if type(en)!= int:
                print(en, type(en))
            newrow['topic_name'] = row['answer_uuid'][:8]
            newrow['highlight_count'] = len(starts)
            newrow['start_pos'] = starts[j]
            newrow['end_pos'] = en
            newrow['target_text'] = texts[j]
            out = out.append(newrow)
    out['end_pos2'] = out['end_pos'].apply(floor)
    out.astype({'end_pos2':'Int64'})
    #out['end_pos'] = out['end_pos'].apply(int)
    path, name = get_path(filePath)
    dest_path = os.path.join(outdir, "Adj_" + name)
    print("outputting TagWorks tag format to ", dest_path)
    #out['end_pos'] = out['end_pos'].fillna(0).astype('Int64')
    #out.to_csv(dest_path, encoding='utf-8')
    #take it out of pandas so we can make it not be a float.
    for_csv = [['article_filename', 'article_sha256', 'source_task_uuid', 'namespace', 'case_number',
               'start_pos', 'end_pos', 'target_text', 'answer_uuid', 'answer_text' ]]
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
        answer_text = out['answer_text'].iloc[i]
        for_csv.append([article_filename, article_sha256, source_task_uuid, namespace, case_number, start_pos, end_pos,
                       target_text, answer_uuid, answer_text])
    with open(dest_path, 'w', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(for_csv)
    return

if __name__ == '__main__':
    output_tags_dir = '../data/output_tags/'
    if not os.path.exists(output_tags_dir):
        os.makedirs(output_tags_dir)
    export_datahunt_tags('../data/scoring/', output_tags_dir)
