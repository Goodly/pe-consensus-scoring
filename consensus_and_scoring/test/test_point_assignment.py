import sys
import os
import pandas as pd

import test_utils
from filegen_utils import *
from pointAssignment import *
import conftest


def test_point_assignment_3x(config):
    tua_path = test_utils.make_test_directory(config, 'pa_tua_input')
    scoring_path = test_utils.make_test_directory(config, 'pa_dep_input')

    weight = weighted(out_folder=scoring_path, article_num = '520', source_task_id='practice_makes+[perfect')
    weight.add_row({'schema':'Probability',
    'namespace':'Covid_Probability', 'Answer_Number':3,'agreement_adjusted_points':5, "Question_Number":5, 'agreement_score': 1,
                    'highlighted_indices': test_utils.make_highlight_indices(10,30)})
    weight_df = weight.df
    print(weight_df)
    new_tua = tua(out_folder=tua_path, article_num = '520', source_task_id = 'tua_task_id')
    new_tua.add_row({'topic_name': 'argument', 'start_pos': 10, 'end_pos':30, 'tua_uuid': 'onlyone'})

    arg_dep = dep_iaa(out_folder=scoring_path, source_task_id ='doesnt matter', article_num= '520')
    #-.5 points--from the weight key in config folder and the agreement_score
    arg_dep.add_row({"namespace":"Covid2_ArgumentRelevance_2020_09_20", "agreed_Answer": 1, "question_Number": 1,
                     "agreement_score":1, "tua_uuid": 'onlyone'})
    print(weight_df)
    new_tua.export()
    arg_dep.export()
    print(scoring_path, None, '\n',weight_df.columns,'\n', tua_path, True)

    tuas, weights, tua_raw = pointSort(scoring_path, input_dir=None, weights=weight_df,
                                       scale_guide_dir = config['IAA_config_dir']+'/point_assignment_scaling_guide.csv',
                                       tua_dir=tua_path, reporting=True)

    assert len(weights) == 1
    assert weights['points'].iloc[0] == 15


"""
When a weight highlight is ~70% overlapping an source TUA make sure points scale as expected
"""

def test_point_assignment_source_seventy_percent_overlap(config):

    tua_path = test_utils.make_test_directory(config, 'pa_source_wh_tua')
    scoring_path = test_utils.make_test_directory(config, 'pa_source_wh_dep')

    weight = weighted(out_folder=scoring_path, article_num='520', source_task_id='source_scaling')
    for i in range(1, 8):
        hl = ((i - 1) * 10, i * 10 - 1)  # (0, 9), (10, 19), ...
        weight.add_row({'schema': 'Probability', 'namespace': 'Covid_Probability', 'Answer_Number': 1,
                        'agreement_adjusted_points': 9, "Question_Number": i, 'agreement_score': 1,
                        'highlighted_indices': test_utils.make_highlight_indices(hl[0], hl[1])})
    weight_df = weight.df

    new_tua = tua(out_folder=tua_path, article_num='520', source_task_id='tua_task_id')
    for i in range(1, 8):
        hl = ((i - 1) * 10 + 3, i * 10)  # (3, 10), (13, 20), which will only overlap between 3-9 over 10 numbers
        new_tua.add_row({'topic_name': 'source', 'start_pos': hl[0], 'end_pos': hl[1], 'tua_uuid': str(i)})

    arg_dep = dep_iaa(out_folder=scoring_path, source_task_id='doesnt matter', article_num='520')
    for i in range(1, 8):
        arg_dep.add_row({"namespace": "Covid_Sources_2002_03_20v2.1", "agreed_Answer": i, "question_Number": 8,
                         "agreement_score": 1, "tua_uuid": str(i)})
    new_tua.export()
    arg_dep.export()

    tuas, weights, tua_raw = pointSort(scoring_path, input_dir=None, weights=weight_df,
                                       scale_guide_dir=config['IAA_config_dir'] + '/point_assignment_scaling_guide.csv',
                                       tua_dir=tua_path, reporting=True)

    assert len(weights) == 7
    assert weights['points'].iloc[0] == 9 * 2
    assert weights['points'].iloc[1] == 9 * 1.5
    assert weights['points'].iloc[2] == 9 * 1
    assert weights['points'].iloc[3] == 9 * 0.5
    assert weights['points'].iloc[4] == 9 * 0
    assert weights['points'].iloc[5] == 9 * -0.5
    assert weights['points'].iloc[6] == 9 * 0




"""
When a weight highlight is ~90% overlapping an argument TUA make sure points scale as expected
"""

def test_point_assignment_argument(config):
    tua_path = test_utils.make_test_directory(config, 'pa_arg_tua')
    scoring_path = test_utils.make_test_directory(config, 'pa_arg_dep')

    weight = weighted(out_folder=scoring_path, article_num = '520', source_task_id='arg_scaling')
    for i in range(1, 7):
        hl = ((i-1)*20, i*20-1) #creates highlights (0, 19) for 1, (20, 39) for 2, etc.
        weight.add_row({'schema':'Probability', 'namespace':'Covid_Probability', 'Answer_Number':1,'agreement_adjusted_points':5, "Question_Number":i, 'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(hl[0],hl[1])})
    weight_df = weight.df

    #Note that each TUA should have its own ID and unique set of highlights (TUA highlights should not overlap!)
    new_tua = tua(out_folder=tua_path, article_num = '520', source_task_id = 'tua_task_id')
    for i in range(1, 7):
        hl = ((i-1)*20+1, i*20-2) # highlights (1, 18) (21, 38)...
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


"""
When a weight highlight is ~50% overlapping an source TUA make sure points don't scale 
"""

def test_point_assignment_source_50_percent_overlap(config):

    tua_path = test_utils.make_test_directory(config, 'pa_source_wh_tua1')
    scoring_path = test_utils.make_test_directory(config, 'pa_source_wh_dep1')

    weight = weighted(out_folder=scoring_path, article_num='520', source_task_id='source_scaling')
    for i in range(1, 8):
        hl = ((i - 1) * 10, i * 10 - 1)  # (0, 9), (10, 19), ...
        weight.add_row({'schema': 'Probability', 'namespace': 'Covid_Probability', 'Answer_Number': 1,
                        'agreement_adjusted_points': 9, "Question_Number": i, 'agreement_score': 1,
                        'highlighted_indices': test_utils.make_highlight_indices(hl[0], hl[1])})
    weight_df = weight.df

    new_tua = tua(out_folder=tua_path, article_num='520', source_task_id='tua_task_id')
    for i in range(1, 8):
        hl = ((i - 1) * 10 + 6, i * 10)  # (6, 10), (16, 20), which will only overlap between 6-10 over 10 numbers
        new_tua.add_row({'topic_name': 'source', 'start_pos': hl[0], 'end_pos': hl[1], 'tua_uuid': str(i)})

    arg_dep = dep_iaa(out_folder=scoring_path, source_task_id='doesnt matter', article_num='520')
    for i in range(1, 8):
        arg_dep.add_row({"namespace": "Covid_Sources_2002_03_20v2.1", "agreed_Answer": i, "question_Number": 8,
                         "agreement_score": 1, "tua_uuid": str(i)})
    new_tua.export()
    arg_dep.export()

    tuas, weights, tua_raw = pointSort(scoring_path, input_dir=None, weights=weight_df,
                                       scale_guide_dir=config['IAA_config_dir'] + '/point_assignment_scaling_guide.csv',
                                       tua_dir=tua_path, reporting=True)

    assert len(weights) == 7
    assert weights['points'].iloc[0] == 9
    assert weights['points'].iloc[1] == 9
    assert weights['points'].iloc[2] == 9
    assert weights['points'].iloc[3] == 9
    assert weights['points'].iloc[4] == 9




"""
When a weight highlight is ~20% overlapping an argument TUA make sure points don't scale 
"""

def test_point_assignment_source_20_percent_overlap(config):

    tua_path = test_utils.make_test_directory(config, 'pa_source_wh_tua2')
    scoring_path = test_utils.make_test_directory(config, 'pa_source_wh_dep2')

    weight = weighted(out_folder=scoring_path, article_num='520', source_task_id='source_scaling')
    for i in range(1, 8):
        hl = ((i - 1) * 10, i * 10 - 1)  # (0, 9), (10, 19), ...
        weight.add_row({'schema': 'Probability', 'namespace': 'Covid_Probability', 'Answer_Number': 1,
                        'agreement_adjusted_points': 9, "Question_Number": i, 'agreement_score': 1,
                        'highlighted_indices': test_utils.make_highlight_indices(hl[0], hl[1])})
    weight_df = weight.df

    new_tua = tua(out_folder=tua_path, article_num='520', source_task_id='tua_task_id')
    for i in range(1, 8):
        hl = ((i - 1) * 10 + 9, i * 10)  # (9, 10), (19, 20), which will only overlap between 9-10 over 10 numbers
        new_tua.add_row({'topic_name': 'source', 'start_pos': hl[0], 'end_pos': hl[1], 'tua_uuid': str(i)})

    arg_dep = dep_iaa(out_folder=scoring_path, source_task_id='doesnt matter', article_num='520')
    for i in range(1, 8):
        arg_dep.add_row({"namespace": "Covid_Sources_2002_03_20v2.1", "agreed_Answer": i, "question_Number": 8,
                         "agreement_score": 1, "tua_uuid": str(i)})
    new_tua.export()
    arg_dep.export()

    tuas, weights, tua_raw = pointSort(scoring_path, input_dir=None, weights=weight_df,
                                       scale_guide_dir=config['IAA_config_dir'] + '/point_assignment_scaling_guide.csv',
                                       tua_dir=tua_path, reporting=True)

    assert len(weights) == 7
    assert weights['points'].iloc[0] == 9
    assert weights['points'].iloc[1] == 9