import os
import pandas as pd
from dataV3 import make_directory

def merge(tua_input_dir, output_csv):
    '''merges all the tuas int he input csv to 1 csv, which gets output to output_csv'''

    tuaFiles = []
    for root, dir, files in os.walk(tua_input_dir):
        for file in files:
            tuaFiles.append(pd.read_csv((tua_input_dir+'/'+file), encoding='utf-8'))
    # we can throw this error because there should always be focus tags if there's specialist tasks
    if len(tuaFiles) == 0:
            raise Exception("No focus tags found {}".format(tua_input_dir))
    merged = pd.concat(tuaFiles)
    make_directory(output_csv)
    merged.to_csv(output_csv+'/triager_data.csv', encoding='utf-8')
    print("Merged focus tag csvs")

if __name__ == '__main__':
    merge("../data/focus_tags/", "../data/output_concat_tags")