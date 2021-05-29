import os
import pandas as pd
from dataV3 import make_directory

def merge(tua_input_dir, output_csv):
    '''merges all the tuas int he input csv to 1 csv, which gets output to output_csv'''

    tuaFiles = []
    for root, dir, files in os.walk(tua_input_dir):
        for file in files:
            tuaFiles.append(pd.read_csv((tua_input_dir+'/'+file), encoding='utf-8'))
    # It is possible theoretically to have an article with no tags.
    # In that case, the preferred solution would be to output just the csv header.
    # But for now throw an error since it is very unlikely an article with zero tags
    # would be sent through the publish pipeline.
    if len(tuaFiles) == 0:
            raise Exception("No tags found {}".format(tua_input_dir))
    merged = pd.concat(tuaFiles)
    make_directory(output_csv)
    merged.to_csv(output_csv+'/triager_data.csv', encoding='utf-8')
    print("Merged tag csvs")

if __name__ == '__main__':
    merge("../test_output/publish_p4-a530712123/focus_tags", "../test_output/publish_p4-a530712123/output_concat_tags")

