import pandas as pd
import os
import random

import test_utils
from filegen_utils import *
from Weighting import *


def test_weighting_sample(config):
    out_path = test_utils.make_test_directory(config, 'weighting_iaa_sample_test')
    weight_out_folder = test_utils.make_test_directory(config, 'out_weighting_iaa_sample_test')

    iaa = dep_iaa(out_folder=out_path, source_task_id='weightsampletests')
    #-.5 points--from the weight key in config folder and the agreement_score
    iaa.add_row({"namespace":"Covid_Evidence2020_03_21", "agreed_Answer": 2, "question_Number": 4, "agreement_score":1})
    #-2 points from ./config/weight_key and agreement score
    iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "agreed_Answer": 1, "question_Number": 8, "agreement_score": .5})
    # +1.5 points from ./config/weight_key and agreement score
    iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "agreed_Answer": 1, "question_Number": 12, "agreement_score": .75})
    fin_path = iaa.export()
    #weighting will output the actual pandas dataframe instead of the directory
    #if you look into the Weighting.py file, you can see the paths to
    weighting_out =  launch_Weighting(out_path, weight_out_folder)
    points = weighting_out['agreement_adjusted_points']
    tot = points.sum()
    print(weighting_out)
    assert tot == -1
    assert len(weighting_out.index) == 3

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
