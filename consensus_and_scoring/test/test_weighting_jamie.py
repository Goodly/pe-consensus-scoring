import pandas as pd
import os
import random

import test_utils
from filegen_utils import *
from Weighting import *

#Verify that when IAA score isn't 1, the agreement adjusted_points = point recommendation * agreement score
#Tests this for 4 different schema types: language, reasoning, evidence, and probability
#The first test (language) has comments for a detailed explanation of the test

def test_language_weighting(config):
    #Import the csv containing the weights for each question
    weight_df = pd.read_csv('../config/weight_key.csv')

    #Set up paths for test data to be stored at
    out_path = test_utils.make_test_directory(config, 'language_calculation_test')
    weight_out_folder = test_utils.make_test_directory(config, 'language_calculation_test_weighting')
    iaa = dep_iaa(out_folder=out_path, source_task_id='languageweights')

    #Generate an IAA with random agreement scores for each question and answer in the schema
    weight_df = weight_df[weight_df['Schema'] == 'Language']
    for index, row in weight_df.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        agree_score = random.random()
        iaa.add_row({"namespace":"Covid_Languagev1.1", "agreed_Answer": answer_num, "question_Number": question_num, "agreement_score": agree_score})

    #Export the data as a dataframe
    fin_path = iaa.export()
    weighting_out = launch_Weighting(out_path, weight_out_folder)

    #Check that weights (point_recs) * agreement_scores = adjusted_scores
    assert weight_df.shape[0] == weighting_out.shape[0]
    point_recs = weight_df['Point_Recommendation'].to_numpy()
    agreement_scores = weighting_out['agreement_score'].to_numpy()
    adjusted_points = weighting_out['agreement_adjusted_points'].to_numpy()
    print("scores:", agreement_scores[:5])
    print("weights:", point_recs[:5])
    print("adjusted:", adjusted_points[:5])
    assert np.array_equal(point_recs * agreement_scores, adjusted_points)
    print("No differences found in language weighting.")
    print()

def test_reasoning_weighting(config):
    weight_df = pd.read_csv('../config/weight_key.csv')

    out_path = test_utils.make_test_directory(config, 'reasoning_calculation_test')
    weight_out_folder = test_utils.make_test_directory(config, 'reasoning_calculation_test_weighting')
    iaa = dep_iaa(out_folder=out_path, source_task_id='reasoningweights')

    weight_df = weight_df[weight_df['Schema'] == 'Reasoning']
    for index, row in weight_df.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        agree_score = random.random()
        iaa.add_row({"namespace":"Covid_Reasoning", "agreed_Answer": answer_num, "question_Number": question_num, "agreement_score": agree_score})

    fin_path = iaa.export()
    weighting_out = launch_Weighting(out_path, weight_out_folder)

    assert weight_df.shape[0] == weighting_out.shape[0]
    point_recs = weight_df['Point_Recommendation'].to_numpy()
    agreement_scores = weighting_out['agreement_score'].to_numpy()
    adjusted_points = weighting_out['agreement_adjusted_points'].to_numpy()
    print("scores:", agreement_scores[:5])
    print("weights:", point_recs[:5])
    print("adjusted:", adjusted_points[:5])
    assert np.array_equal(point_recs * agreement_scores, adjusted_points)
    print("No differences found in reasoning weighting.")
    print()

def test_evidence_weighting(config):
    weight_df = pd.read_csv('../config/weight_key.csv')

    out_path = test_utils.make_test_directory(config, 'evidence_calculation_test')
    weight_out_folder = test_utils.make_test_directory(config, 'evidence_calculation_test_weighting')
    iaa = dep_iaa(out_folder=out_path, source_task_id='evidenceweights')

    weight_df = weight_df[weight_df['Schema'] == 'Evidence']
    for index, row in weight_df.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        agree_score = random.random()
        iaa.add_row({"namespace":"Covid_Evidence2020_03_21", "agreed_Answer": answer_num, "question_Number": question_num, "agreement_score": agree_score})

    fin_path = iaa.export()
    weighting_out = launch_Weighting(out_path, weight_out_folder)

    assert weight_df.shape[0] == weighting_out.shape[0]
    point_recs = weight_df['Point_Recommendation'].to_numpy()
    agreement_scores = weighting_out['agreement_score'].to_numpy()
    adjusted_points = weighting_out['agreement_adjusted_points'].to_numpy()
    print("scores:", agreement_scores[:5])
    print("weights:", point_recs[:5])
    print("adjusted:", adjusted_points[:5])
    assert np.array_equal(point_recs * agreement_scores, adjusted_points)
    print("No differences found in evidence weighting.")
    print()

def test_probability_weighting(config):
    weight_df = pd.read_csv('../config/weight_key.csv')

    out_path = test_utils.make_test_directory(config, 'probability_calculation_test')
    weight_out_folder = test_utils.make_test_directory(config, 'probability_calculation_test_weighting')
    iaa = dep_iaa(out_folder=out_path, source_task_id='probabilityweights')

    weight_df = weight_df[weight_df['Schema'] == 'Probability']
    for index, row in weight_df.iterrows():
        question_num = row['Question_Number']
        answer_num = row['Answer_Number']
        agree_score = random.random()
        iaa.add_row({"namespace":"Covid_Probability", "agreed_Answer": answer_num, "question_Number": question_num, "agreement_score": agree_score})

    fin_path = iaa.export()
    weighting_out = launch_Weighting(out_path, weight_out_folder)

    assert weight_df.shape[0] == weighting_out.shape[0]
    point_recs = weight_df['Point_Recommendation'].to_numpy()
    agreement_scores = weighting_out['agreement_score'].to_numpy()
    adjusted_points = weighting_out['agreement_adjusted_points'].to_numpy()
    print("scores:", agreement_scores[:5])
    print("weights:", point_recs[:5])
    print("adjusted:", adjusted_points[:5])
    assert np.array_equal(point_recs * agreement_scores, adjusted_points)
    print("No differences found in probability weighting.")
    print()
