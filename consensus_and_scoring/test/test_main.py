import os
import pandas as pd
import numpy as np

import test_utils
from filegen_utils import *
from master import *

all_schemas = []
schema_path = "./../../data/schemas/"
for root, dir, files in os.walk(schema_path):
    for file in files:
        df = pd.read_csv(schema_path+file, encoding = 'utf-8')
        all_schemas.append(df)
all_schemas = pd.concat(all_schemas)

def make_dh(dh, namespace, start = 0, end = 0, start_shift = lambda x:x, end_shift = None,
              answer = lambda x:x, tua = None):
    if end_shift == None:
        end_shift = start_shift
    schema = all_schemas[all_schemas['namespace'] == namespace]
    for q_lab in np.unique(schema['question_label']):
        for j in range(1,6):
            q_schema = schema[schema['question_label'] == q_lab]
            max_ans = q_schema['answer_count'].iloc[0]
            ans = int(answer(j)%max_ans)
            has_hl = q_schema['highlight'].iloc[0]
            if has_hl == 0:
                my_start = 0
                my_end = 0
            else:
                my_start = start
                my_end = end
            if tua == None:
                dh.add_row({'answer_label': q_lab+'.A' + str(ans), 'namespace': namespace,
                            'contributor_uuid': 'person' + str(j),
                            'start_pos': my_start, 'end_pos': my_end})
            else:
                dh.add_row({'answer_label': q_lab + '.A' + str(ans), 'namespace': namespace,
                            'contributor_uuid': 'person' + str(j),
                            'start_pos': my_start, 'end_pos': my_end,
                            'tua_uuid': tua})
            if has_hl:
                start = start_shift(start)
                end = end_shift(end)
                if start>=dh.article_text_length or end >= dh.article_text_length:
                    start = 0
                    end = 5
def test_master(config):
    dh_path = test_utils.make_test_directory(config, 'mn_dh_')
    iaa_path = test_utils.make_test_directory(config, 'out_mn_iaa')
    scoring_path = test_utils.make_test_directory(config, 'out_mn_scoring')
    tua_path = test_utils.make_test_directory(config, 'mn_tua_')
    viz_path = test_utils.make_test_directory(config, 'out_mn_viz')
    dh = datahunt(out_folder=dh_path, source_task_id='dh1', article_num = '520', article_text_length = 2900)
    for i in range(9):
        for j in range(i + 1):
            dh.add_row({'answer_label': 'T1.Q2.A' + str(j), 'namespace': 'Covid2_Reasoning_2020_09_20',
                        'contributor_uuid': 'Daniel' + str(i)})
    dh = datahunt(out_folder=dh_path, source_task_id='dh2', article_num = '520', article_text_length = 2900)
    make_dh(dh, 'Covid2_Probability2020_09_20', 20,50, start_shift=lambda x:x+5, end_shift=lambda x: x*2)
    dh = datahunt(out_folder=dh_path, source_task_id='dh3', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Probability2020_09_20', 100, 150, start_shift=lambda x: x - 5, end_shift=lambda x: x + 5,
            answer= lambda x: 2)
    dh.export()
    dh = datahunt(out_folder=dh_path, source_task_id='dh4', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Probability2020_09_20', 100, 150, start_shift=lambda x: x +5, end_shift=lambda x: x + 5,
            answer= lambda x: (x+8)/5)
    dh.export()
    dh = datahunt(out_folder=dh_path, source_task_id='dh5', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Holistic_2020_09_20', 0, 0, start_shift=lambda x: x, end_shift=lambda x: x,
            answer= lambda x: 1)
    dh.export()
    dh = datahunt(out_folder=dh_path, source_task_id='dh6', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Evidence2020_09_20', 100, 150, start_shift=lambda x: x +5, end_shift=lambda x: x + 5,
            answer= lambda x: (x+6)/4)
    dh.export()
    dh = datahunt(out_folder=dh_path, source_task_id='dh7', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Sources_2002_09_20', 100, 150, start_shift=lambda x: x +5, end_shift=lambda x: x + 5,
            answer= lambda x: 6, tua = 'qs1')
    dh.export()
    dh = datahunt(out_folder=dh_path, source_task_id='dh8', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Sources_2002_09_20', 500, 760, start_shift=lambda x: x +5, end_shift=lambda x: x + 5,
            answer= lambda x: 6, tua = 'qs2')
    dh.export()
    dh = datahunt(out_folder=dh_path, source_task_id='dh9', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Sources_2002_09_20', 100, 150, start_shift=lambda x: x +5, end_shift=lambda x: x + 5,
            answer= lambda x: 6, tua = 'qs3')
    dh.export()
    dh = datahunt(out_folder=dh_path, source_task_id='dh10', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Sources_2002_09_20', 500, 760, start_shift=lambda x: x +5, end_shift=lambda x: x + 5,
            answer= lambda x: 6, tua = 'qs4')
    dh.export()
    dh = datahunt(out_folder=dh_path, source_task_id='dh11', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Sources_2002_09_20', 100, 150, start_shift=lambda x: x +5, end_shift=lambda x: x + 5,
            answer= lambda x: 6, tua = 'qs5')
    dh.export()
    dh = datahunt(out_folder=dh_path, source_task_id='dh12', article_num='520', article_text_length=2900)
    make_dh(dh, 'Covid2_Sources_2002_09_20', 500, 760, start_shift=lambda x: x +5, end_shift=lambda x: x + 5,
            answer= lambda x: 6, tua = 'qs6')
    dh.export()

    new_tua = tua(out_folder=tua_path, article_num='520', source_task_id='tua_task_id')
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 10, 'end_pos': 30, 'tua_uuid': 'a1'})
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 40, 'end_pos': 80, 'tua_uuid': 'a2'})
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 40, 'end_pos': 80, 'tua_uuid': 'a3'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 20, 'end_pos': 80, 'tua_uuid': 'qs1'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 40, 'end_pos': 60, 'tua_uuid': 'qs2'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 20, 'end_pos': 80, 'tua_uuid': 'qs3'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 40, 'end_pos': 60, 'tua_uuid': 'qs4'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 20, 'end_pos': 80, 'tua_uuid': 'qs5'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 40, 'end_pos': 60, 'tua_uuid': 'qs6'})
    new_tua.export()
    calculate_scores_master(dh_path, config['test_dir']+config['texts_dir'], config['IAA_config_dir'], config['schema_dir'], iaa_path,
                            scoring_path, push_aws = False, tua_dir = tua_path, viz_dir = viz_path, reporting = True)