import os
import pandas as pd
from dataV3 import get_indices_hard
from dataV3 import  get_path
from Separator import  indicesToStartEnd
def exportDataHuntTags(path, outdir = None):
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
    df = pd.read_csv(filePath, encoding = 'utf-8')
    out = pd.DataFrame()
    for i in range(len(df)):
        row = df.iloc[i]
        indices = row['highlighted_indices']
        indices = get_indices_hard(indices)
        starts, ends, c = indicesToStartEnd(indices)
        texts = row['target_text']
        answerid = row['answer_uuid']

        # print(c)
        if starts[0] < 0:
            starts[0] = 0
            ends[0] = 0
            texts = ''
        #print(texts)
        if pd.isna(texts):
            print("text not found")
            texts = 'scdscs//sxasx//ds/csd//ds/cs/cds//cdscs//cdscsd//cdscsd//cdscsd//cdscsa//sdcadscsa//dsfads//adsfa/ds//afdsfas////////afdsfsa//dfasd//'
        texts = texts.split('//')
        #print(starts, ends, texts)
        for j in range(len(starts)):
            newrow = row
            newrow['case_number'] = 0 #no case numbers in datahunts
            newrow['highlight_count'] = len(starts)
            newrow['start_pos'] = starts[j]
            newrow['end_pos'] = ends[j]
            newrow['target_text'] = texts[j]
            out = out.append(newrow)
    path, name = get_path(filePath)
    print("outputting formatted stuff to", outdir+"Adj_"+name)
    out.to_csv(outdir+"Adj_"+name, encoding='utf-8')
    return

if __name__ == '__main__':
    exportDataHuntTags('../data/out_scoring_evi/')

