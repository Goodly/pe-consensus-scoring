import sys
import os
import pandas as pd

import test_utils
from filegen_utils import *
from pointAssignment import *
import conftest

#With a weight highlight that is completely inside of an argument TUA, make sure the points scale as expected
#To test argument TUAs, change topic_name to 'argument' and namespace to an Argument schema
def test_point_assignment_argument(config):
    tua_path = test_utils.make_test_directory(config, 'pa_arg_tua')
    scoring_path = test_utils.make_test_directory(config, 'pa_arg_dep')

    weight = weighted(out_folder=scoring_path, article_num = '520', source_task_id='arg_scaling')
    for i in range(1, 7):
        hl = ((i-1)*10, i*10-1) #creates highlights (0, 9) for 1, (10, 19) for 2, etc.
        weight.add_row({'schema':'Probability', 'namespace':'Covid_Probability', 'Answer_Number':1,'agreement_adjusted_points':5, "Question_Number":i, 'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(hl[0],hl[1])})
    weight_df = weight.df

    #Note that each TUA should have its own ID and unique set of highlights (TUA highlights should not overlap!)
    new_tua = tua(out_folder=tua_path, article_num = '520', source_task_id = 'tua_task_id')
    for i in range(1, 7):
        hl = ((i-1)*10, i*10-1)
        new_tua.add_row({'topic_name': 'argument', 'start_pos': hl[0], 'end_pos': hl[1], 'tua_uuid': str(i)})

    arg_dep = dep_iaa(out_folder=scoring_path, source_task_id ='doesnt matter', article_num= '520')
    for i in range(1, 7):
        arg_dep.add_row({"namespace":"Covid2_ArgumentRelevance_2020_09_20", "agreed_Answer": i, "question_Number": 1, "agreement_score":1, "tua_uuid": str(i)})
    new_tua.export()
    arg_dep.export()

    tuas, weights, tua_raw = pointSort(scoring_path, input_dir=None, weights=weight_df,
                                       scale_guide_dir = config['IAA_config_dir']+'/point_assignment_scaling_guide.csv',
                                       tua_dir=tua_path, reporting=True)
    print("WEIGHTS:",weights)
    assert len(weights) == 6
    #Ensure all point assignments are accurate with point_assignment_scaling_guide.csv
    assert weights['points'].iloc[0] == 5 * 3
    assert weights['points'].iloc[1] == 5 * 2
    assert weights['points'].iloc[2] == 5 * 1
    assert weights['points'].iloc[3] == 5 * 0.7
    assert weights['points'].iloc[4] == 5 * 0.5
    assert weights['points'].iloc[5] == 5 * 1

#With a weight highlight that is completely inside of a source TUA, make sure the points scale as expected
#To test source TUAs, change topic_name to 'source' and namespace to a Source schema
def test_point_assignment_source(config):
    tua_path = test_utils.make_test_directory(config, 'pa_source_tua')
    scoring_path = test_utils.make_test_directory(config, 'pa_source_dep')

    weight = weighted(out_folder=scoring_path, article_num = '520', source_task_id='source_scaling')
    for i in range(1, 8):
        hl = ((i-1)*10, i*10-1) #creates highlights (0, 9) for 1, (10, 19) for 2, etc.
        weight.add_row({'schema':'Probability', 'namespace':'Covid_Probability', 'Answer_Number':1,'agreement_adjusted_points':5, "Question_Number":i, 'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(hl[0],hl[1])})
    weight_df = weight.df

    #Note that each TUA should have its own ID and unique set of highlights (TUA highlights should not overlap!)
    new_tua = tua(out_folder=tua_path, article_num = '520', source_task_id = 'tua_task_id')
    for i in range(1, 8):
        hl = ((i-1)*10, i*10-1)
        new_tua.add_row({'topic_name': 'source', 'start_pos': hl[0], 'end_pos': hl[1], 'tua_uuid': str(i)})

    arg_dep = dep_iaa(out_folder=scoring_path, source_task_id ='doesnt matter', article_num= '520')
    for i in range(1, 8):
        arg_dep.add_row({"namespace":"Covid_Sources_2002_03_20v2.1", "agreed_Answer": i, "question_Number": 8, "agreement_score":1, "tua_uuid": str(i)})
    new_tua.export()
    arg_dep.export()

    tuas, weights, tua_raw = pointSort(scoring_path, input_dir=None, weights=weight_df,
                                       scale_guide_dir = config['IAA_config_dir']+'/point_assignment_scaling_guide.csv',
                                       tua_dir=tua_path, reporting=True)
    print("WEIGHTS:",weights)
    assert len(weights) == 7
    #Ensure all point assignments are accurate with point_assignment_scaling_guide.csv
    assert weights['points'].iloc[0] == 5 * 2
    assert weights['points'].iloc[1] == 5 * 1.5
    assert weights['points'].iloc[2] == 5 * 1
    assert weights['points'].iloc[3] == 5 * 0.5
    assert weights['points'].iloc[4] == 5 * 0
    assert weights['points'].iloc[5] == 5 * -0.5
    assert weights['points'].iloc[6] == 5 * 0
