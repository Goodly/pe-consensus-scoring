import sys
import os
import pandas as pd

import test_utils
from filegen_utils import *
from pointAssignment import *
import conftest


# def test_point_assignment_3x(config):
#     tua_path = test_utils.make_test_directory(config, 'pa_tua_input')
#     scoring_path = test_utils.make_test_directory(config, 'pa_dep_input')

#     weight = weighted(out_folder=scoring_path, article_num = '520', source_task_id='practice_makes+[perfect')
#     weight.add_row({'schema':'Probability',
#     'namespace':'Covid_Probability', 'Answer_Number':3,'agreement_adjusted_points':5, "Question_Number":5, 'agreement_score': 1,
#                     'highlighted_indices': test_utils.make_highlight_indices(10,30)})
#     weight_df = weight.df
#     print(weight_df)
#     new_tua = tua(out_folder=tua_path, article_num = '520', source_task_id = 'tua_task_id')
#     new_tua.add_row({'topic_name': 'argument', 'start_pos': 10, 'end_pos':30, 'tua_uuid': 'onlyone'})

#     arg_dep = dep_iaa(out_folder=scoring_path, source_task_id ='doesnt matter', article_num= '520')
#     #-.5 points--from the weight key in config folder and the agreement_score
#     arg_dep.add_row({"namespace":"Covid2_ArgumentRelevance_2020_09_20", "agreed_Answer": 1, "question_Number": 1,
#                      "agreement_score":1, "tua_uuid": 'onlyone'})
#     print(weight_df)
#     new_tua.export()
#     arg_dep.export()
#     print(scoring_path, None, '\n',weight_df.columns,'\n', tua_path, True)

#     tuas, weights, tua_raw = pointSort(scoring_path, input_dir=None, weights=weight_df,
#                                        scale_guide_dir = config['IAA_config_dir']+'/point_assignment_scaling_guide.csv',
#                                        tua_dir=tua_path, reporting=True)

#     assert len(weights) == 1
#     assert weights['points'].iloc[0] == 15

