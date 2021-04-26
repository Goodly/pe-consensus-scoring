#!/usr/bin/env python
# coding: utf-8

# # Datahunt Aggregation Notebook
# The purpose of this notebook is to collect rows associated with datahunt files associated with a specific article.
# It reads in datahunt csvs and outputs a single csv corresponding to a single article with the following columns:/
# * Credibility Indicator Category
# * Question Number
# * Answer Number
# * Point Recommendation
# * Credibility Indicator Name
# * Start
# * End
#
# Currently, this datahunt files only contain useful information about article 100059

# In[1]:


import json
import csv
import os
import glob
import pandas as pd
import numpy as np
import re
from collections import Counter


# Read in all the datahunt files from the datahunt folder.


"""
Combines all datahunt files in the /datahunt/ folder into one larger dataframe.
@param: path=''; path to datahunt folder if it exists somewhere besides your working directory
"""
def file_concat(path = ''):

	path +=  'datahunts'
	all_files = glob.glob(path + "/*.csv")

	li = []

	for filename in all_files:
		df = pd.read_csv(filename, index_col=None, header=0)
		li.append(df)

	raw_data = pd.concat(li, axis=0, ignore_index=True)


	if not (raw_data[(raw_data["start_pos"] == -1) & (raw_data["end_pos"] == -1)].head()).empty:
		print('Warning: there are rows in this file that have invalid start and end indices. This means their data may correspond to invalid sections of the article.')
	return raw_data

# ## Branch 1

# We'll pick up the:
#
# * Contributor ID column
# * article's sha256
# * article number
# * question text
# * answer text
# * start of highlight in the article
# * end of highlight in the article

# Selects rows with the inputted user_id.
def select_user_id(df, user_id):
    return df[df["contributor_uuid"] == user_id]

# Selects rows with the inputted article_id.
def select_article_id(df, article_id):
    return df[df["article_number"] == article_id]

# Takes out invalid start_pos and end_pos indices from the dataframe.
def select_valid_indices(df):
    return df[(df["start_pos"] != 0) | (df["start_pos"] != -1) | (df["end_pos"] != 0) | (df["start_pos"] != -1)]

# Converts dataframe to CSV.
def convert_to_csv(df, category, arg):
    if category == "user_id":
        df = select_user_id(df, arg)
        name = str(df["contributor_uuid"][0])
    elif category == "article_id":
        df = select_article_id(df, arg)
        name = str(df["article_number"][0])
    elif category == "valid_indices":
        df = select_valid_indices(df)
        name = "valid"
    else:
        raise ValueError("Invalid category type")

    # "[articleid]_[userid]_user_contributions.csv"
    df.to_csv("aggregate_datahunts/" + name + "_user_contributions.csv")


# Converts dataframe to CSV, putting article_number and contributor_uuid in the name.
def convert_to_csv_user_article(df, user_id, article_id):
    df = select_user_id(df, user_id)
    df = select_article_id(df, article_id)
    df = select_valid_indices(df)
    name = str(df["article_number"][0]) + "_" + str(df["contributor_uuid"][0])
    df.to_csv("aggregate_datahunts/" + name + "_user_contributions.csv")

# ## Branch 2
# ### Points based on Topic Name, Question Number, Answer Number

def create_weight_key(path = ''):
	path += 'weight_key.csv'
	weight_key = pd.read_csv(path)
	return weight_key

"""
create_eta_datahunt will create Explore The Article datahunt csvs containing the
predicted individual contribution for each question asked by Tagworks.
    @param article_sha256r: the article to create the eta_datahunt file for
    @param contributor_id: the contributor requesting the data
	@param : path to the datahunt and weight_key files.
			 Expect datahunt files to be in the folder /datahunt/, weight_key
			 to be in the working directory.
    @return: None. Writes a dataframe of the proper format to be fed into Visualization.html. Contains the predicted point values and labels for the individual contributions to Tagworks. This csv file is in eta_datahunts.

"""
def create_eta_datahunt(article_sha256, path = '', target_dir='../'):
	weight_key = create_weight_key(path)
	raw_data = file_concat(path)
	raw_data = raw_data.loc[raw_data["schema_sha256"] == article_sha256]
	if raw_data.empty:
		new_df = pd.DataFrame([["no_article", 0, 0, 0, 0, 0, 0]], columns=['Credibility Indicator Category', 'Question Number', 'Answer Number','Point Recommendation', 'Credibility Indicator Name', 'Start', 'End'])
	else:
		#raw_data = raw_data.loc[raw_data["contributor_uuid"] == contributor_id]
		if raw_data.empty:
			new_df = pd.DataFrame([['no_user', 0, 0, 0, 0, 0, 0]], columns=['Credibility Indicator Category', 'Question Number', 'Answer Number','Point Recommendation', 'Credibility Indicator Name', 'Start', 'End'])
		else:
			new_df = pd.DataFrame()
			for row_num in raw_data.index:
				new_row = new_from_row(raw_data, row_num, weight_key)
				if (new_row.empty):
					continue
				else:
					new_df = new_df.append(new_row)
			new_df.to_csv(path + target_dir + "user_contributions.csv")
	return new_df

"""

Extracts question and answer number of a topic.
    @param df: the raw_data dataframe
    @param row_number: the row of df that we are trying to extract the question and answer from
    @return: the row from the df with the question and answer values appended to the row.


"""

def get_TopicQA(df, row_number):
    select_df = df[['topic_name', 'question_label', 'answer_label', 'start_pos', 'end_pos']].copy()
    select_df['topic_name'] = select_df['topic_name'].apply(lambda x: x.split(' ')[0])
    select_df['question_label'] = select_df['question_label'].apply(lambda x: re.findall('Q\d+', x)[0])
    select_df['answer_label'] = select_df['answer_label'].apply(lambda x: re.findall('A\d+', x)[0])

    return select_df[select_df.index == row_number]


# need to get points and label from weight key csv
# want point value (col F), topic (category), subset (col G - label)
#  - extract this from weight_key csv (do string parsing - slice out the “Specialist” part)
# into new data frame —> csv — different csv name from previous ones


"""
Gets points and label from csv.
    @param df: the raw_data dataframe
    @param row_number: the row of df that we are trying to extract the question and answer from
    @return: the row from the df with the points, labels, start and end indices appended to the row.

"""

def new_from_row(df, row_number, weight_key):

    TQA_row = get_TopicQA(df, row_number)
    TQA_question = int(TQA_row[TQA_row.index == row_number]['question_label'][row_number][1:])
    TQA_answer = int(TQA_row[TQA_row.index == row_number]['answer_label'][row_number][1:])
    TQA_schema = TQA_row[TQA_row.index == row_number]['topic_name'][row_number]
    new_df = weight_key[(weight_key['Question_Number'] == float(TQA_question)) & (weight_key['Answer_Number'] == float(TQA_answer)) & (weight_key['Schema'] == TQA_schema)]
    if new_df.empty:
        print("There is no algorithm output for question ", TQA_question,
              ",answer ", TQA_answer, ", and category ", TQA_schema)
        return new_df
    else:
        new_df = new_df.drop(columns=['Question_Number_V2', 'Question_Type', 'Key Question', 'answer_uuid'])
        new_df = new_df.rename(columns={'Question_Number': 'Question Number', 'Answer_Number':'Answer Number', 'Point_Recommendation': 'Point Recommendation', "Schema": "Credibility Indicator Category", "Label": "Credibility Indicator Name"})
        new_df['Start'] = np.array(TQA_row['start_pos'])
        new_df['End'] = np.array(TQA_row['end_pos'])
        return new_df


#output = create_eta_datahunt(raw_data, weight_key, first256, first)
