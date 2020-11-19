import sys
import os
import pandas as pd

import test_utils
from filegen_utils import *
from pointAssignment import *
import conftest


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