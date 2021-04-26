# # Semantics Aggregation Notebook
# This notebook will search through relevant tagworks output files, specifically form files, and aggregate them based on article. In essence, this notebook maps all tagworks form files for a specific article to a single csv that contains the following columns:
#
# * article id
# * article sha256
# * Credibility Indicator Category
# * Start
# * End
# * Case Number
# * Indices of Label in Article
#


import json
import csv
import os
import glob
import pandas as pd
import numpy as np


"""
Read the Form file from the inputted path. By default, expects form file to be in
working directory.
"""
def read_csv_from_path(path=''):

    path += 'Covid_Form1.0.adjudicated-2020-10-04T2314-Tags.csv'
    return pd.read_csv(path)



"""
Take article_number, start_pos, end_pos, and add Indices of Label in Article. Rename them if necesssary.
Assumption: raw_data contains columns named article_number, start_pos, end_pos
By default, we place the created csv in the parent directory, where Visualization.html lives.
"""
def simple_data_from_raw_data(article_sha256, target_dir='../', path=''):

    raw_data = read_csv_from_path(path)
    file_name = target_dir+"article_elements.csv"
    simple_data = raw_data[['article_number', 'article_sha256', 'topic_name', 'start_pos', 'end_pos', 'case_number']]
    simple_data = simple_data.rename(columns = {'article_number': 'Article ID', 'article_sha256': 'Article sha256', 'topic_name': 'Credibility Indicator Category', 'start_pos': 'Start', 'end_pos': 'End', 'case_number': 'Case Number'})


    if simple_data[simple_data['Article sha256'] == article_sha256].empty:
        new_df = pd.DataFrame([["no_article", 0, 0, 0, 0, 0, 0]], columns=['Credibility Indicator Category', 'Question Number', 'Answer Number','Point Recommendation', 'Credibility Indicator Name', 'Start', 'End'])
        print("This article_256 is not in the csv")
        new_df.to_csv(file_name)
        return new_df

    else:
        sub_simple_data = simple_data[simple_data['Article sha256'] == article_sha256].copy()
        sub_simple_data['Indices of Label in Article'] = sub_simple_data.apply(lambda x: list(range(x['Start'], x['End'] + 1)), axis=1)
        sub_simple_data.to_csv(file_name)
        return sub_simple_data


#data_from_path = read_csv_from_path('Covid_Form1.0.adjudicated-2020-10-04T2314-Tags.csv')
#simple_data = simple_data_from_raw_data(data_from_path, 100059)



# def test_suite():
#     data_from_path = read_csv_from_path('Covid_Form1.0.adjudicated-2020-10-04T2314-Tags.csv')
#     simple_data = simple_data_from_raw_data(data_from_path, )
#     assert correct columns, non zero values, each row has correct data typekitId
#
#     data_from_path = read_csv_from_path('Covid_Form1.0.adjudicated-2020-10-04T2314-Tags.csv')
#     simple_data = simple_data_from_raw_data(data_from_path, 1290387139874)
#
#     assert 1 row
