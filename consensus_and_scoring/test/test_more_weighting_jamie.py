import pandas as pd
import os
import random

import test_utils
from filegen_utils import *
from Weighting import *

#Pick 3 random rows from weight_key, make an IAA to get agreement_adjusted_points to equal the point recommendation for those rows
#Tests this for 4 different schema types: language, reasoning, evidence, and probability

def test_random_language(config):
    #Import the csv containing the weights for each question
    weight_df = pd.read_csv('../config/weight_key.csv')
    weight_df = weight_df[weight_df['Schema'] == 'Language']

    #Set up paths for test data to be stored at
    out_path = test_utils.make_test_directory(config, 'language_random_test')
    weight_out_folder = test_utils.make_test_directory(config, 'language_random_test_weighting')

    #Create IAA file with 3 random rows with agreement scores of 1
    iaa = dep_iaa(out_folder=out_path, source_task_id='3random')
    sample_df = weight_df.sample(3)
    for index, row in sample_df.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        iaa.add_row({"namespace":"Covid_Languagev1.1", "agreed_Answer": answer_num, "question_Number": question_num, "agreement_score": 1})

    #Export the data as a dataframe and check if all 3 rows have the correct adjusted weight
    fin_path = iaa.export()
    weighting_out = launch_Weighting(out_path, weight_out_folder)
    assert weighting_out.shape[0] == 3
    for index, row in weighting_out.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        adjusted_points = row['agreement_adjusted_points']
        correct_weight = weight_df[(weight_df['Question_Number'] == question_num) & (weight_df['Answer_Number'] == answer_num)]['Point_Recommendation'].iloc[0]
        assert adjusted_points == correct_weight, "Q" + str(question_num) + "A" + str(answer_num) + " points: " + str(adjusted_points) + ", weight_df: " + str(correct_weight)
        print("language random row",index+1,"checks out")

def test_random_reasoning(config):
    weight_df = pd.read_csv('../config/weight_key.csv')
    weight_df = weight_df[weight_df['Schema'] == 'Reasoning']
    out_path = test_utils.make_test_directory(config, 'reasoning_random_test')
    weight_out_folder = test_utils.make_test_directory(config, 'reasoning_random_test_weighting')
    iaa = dep_iaa(out_folder=out_path, source_task_id='3random')
    sample_df = weight_df.sample(3)
    for index, row in sample_df.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        iaa.add_row({"namespace":"Covid_Reasoning", "agreed_Answer": answer_num, "question_Number": question_num, "agreement_score": 1})
    fin_path = iaa.export()
    weighting_out = launch_Weighting(out_path, weight_out_folder)
    assert weighting_out.shape[0] == 3
    for index, row in weighting_out.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        adjusted_points = row['agreement_adjusted_points']
        correct_weight = weight_df[(weight_df['Question_Number'] == question_num) & (weight_df['Answer_Number'] == answer_num)]['Point_Recommendation'].iloc[0]
        assert adjusted_points == correct_weight, "Q" + str(question_num) + "A" + str(answer_num) + " points: " + str(adjusted_points) + ", weight_df: " + str(correct_weight)
        print("reasoning random row",index+1,"checks out")

def test_random_evidence(config):
    weight_df = pd.read_csv('../config/weight_key.csv')
    weight_df = weight_df[weight_df['Schema'] == 'Evidence']
    out_path = test_utils.make_test_directory(config, 'evidence_random_test')
    weight_out_folder = test_utils.make_test_directory(config, 'evidence_random_test_weighting')
    iaa = dep_iaa(out_folder=out_path, source_task_id='3random')
    sample_df = weight_df.sample(3)
    for index, row in sample_df.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        iaa.add_row({"namespace":"Covid_Evidence2020_03_21", "agreed_Answer": answer_num, "question_Number": question_num, "agreement_score": 1})
    fin_path = iaa.export()
    weighting_out = launch_Weighting(out_path, weight_out_folder)
    assert weighting_out.shape[0] == 3
    for index, row in weighting_out.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        adjusted_points = row['agreement_adjusted_points']
        correct_weight = weight_df[(weight_df['Question_Number'] == question_num) & (weight_df['Answer_Number'] == answer_num)]['Point_Recommendation'].iloc[0]
        assert adjusted_points == correct_weight, "Q" + str(question_num) + "A" + str(answer_num) + " points: " + str(adjusted_points) + ", weight_df: " + str(correct_weight)
        print("evidence random row",index+1,"checks out")

def test_random_probability(config):
    weight_df = pd.read_csv('../config/weight_key.csv')
    weight_df = weight_df[weight_df['Schema'] == 'Probability']
    out_path = test_utils.make_test_directory(config, 'probability_random_test')
    weight_out_folder = test_utils.make_test_directory(config, 'probability_random_test_weighting')
    iaa = dep_iaa(out_folder=out_path, source_task_id='3random')
    sample_df = weight_df.sample(3)
    for index, row in sample_df.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        iaa.add_row({"namespace":"Covid_Probability", "agreed_Answer": answer_num, "question_Number": question_num, "agreement_score": 1})
    fin_path = iaa.export()
    weighting_out = launch_Weighting(out_path, weight_out_folder)
    assert weighting_out.shape[0] == 3
    for index, row in weighting_out.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        adjusted_points = row['agreement_adjusted_points']
        correct_weight = weight_df[(weight_df['Question_Number'] == question_num) & (weight_df['Answer_Number'] == answer_num)]['Point_Recommendation'].iloc[0]
        assert adjusted_points == correct_weight, "Q" + str(question_num) + "A" + str(answer_num) + " points: " + str(adjusted_points) + ", weight_df: " + str(correct_weight)
        print("probability random row",index+1,"checks out")
