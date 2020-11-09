import sys
import os
import pandas as pd

import test_utils
from filegen_utils import *
from pointAssignment import *
import conftest


"""
When the source TUA is entirely inside of the weight highlight make sure points scale as expected
"""
def test_point_assignment_source_in_weight_highlight(config):
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
        hl = ((i - 1) * 10 + 1, i * 10 - 2)  # (1, 8), (11, 18), ...
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
When multiple conditions are met for point scaling (i.e. both a source and an argument), make sure only
the one with the lowest priority is applied
"""
def test_point_assignment_multiple_lowest_priority_applied(config):
    tua_path = test_utils.make_test_directory(config, 'pa_mlpa_tua')
    scoring_path = test_utils.make_test_directory(config, 'pa_mlpa_dep')

    weight = weighted(out_folder=scoring_path, article_num='520', source_task_id='source_scaling')
    for i in range(1, 7):
        hl = ((i - 1) * 10, i * 10 - 1)  # (0, 9), (10, 19), ...
        weight.add_row({'schema': 'Probability', 'namespace': 'Covid_Probability', 'Answer_Number': 1,
                        'agreement_adjusted_points': 9, "Question_Number": i, 'agreement_score': 1,
                        'highlighted_indices': test_utils.make_highlight_indices(hl[0], hl[1])})
    weight_df = weight.df

    new_tua = tua(out_folder=tua_path, article_num='520', source_task_id='tua_task_id')
    # Adding both Argument and Source TUAs
    for i in range(1, 7):
        hl = ((i - 1) * 10, i * 10 - 1)  # (0, 9), (10, 19), ...
        new_tua.add_row({'topic_name': 'source', 'start_pos': hl[0], 'end_pos': hl[1], 'tua_uuid': str(i * 10)})
        new_tua.add_row({'topic_name': 'argument', 'start_pos': hl[0], 'end_pos': hl[1], 'tua_uuid': str(i + 1)})

    arg_dep = dep_iaa(out_folder=scoring_path, source_task_id='doesnt matter', article_num='520')
    for i in range(1, 7):
        arg_dep.add_row({"namespace": "Covid_Sources_2002_03_20v2.1", "agreed_Answer": i, "question_Number": 8,
                         "agreement_score": 1, "tua_uuid": str(i * 10)})
        arg_dep.add_row({"namespace": "Covid2_ArgumentRelevance_2020_09_20", "agreed_Answer": i, "question_Number": 1,
                         "agreement_score": 1, "tua_uuid": str(i + 1)})
    new_tua.export()
    arg_dep.export()

    tuas, weights, tua_raw = pointSort(scoring_path, input_dir=None, weights=weight_df,
                                       scale_guide_dir=config['IAA_config_dir'] + '/point_assignment_scaling_guide.csv',
                                       tua_dir=tua_path, reporting=True)

    assert len(weights) == 6
    assert weights['points'].iloc[0] == 9 * 2
    assert weights['points'].iloc[1] == 9 * 1.5
    assert weights['points'].iloc[2] == 9 * 1
    assert weights['points'].iloc[3] == 9 * 0.5
    assert weights['points'].iloc[4] == 9 * 0
    assert weights['points'].iloc[5] == 9 * -0.5

