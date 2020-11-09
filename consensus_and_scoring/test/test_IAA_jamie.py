import pandas as pd
import sys
import os

import test_utils
from filegen_utils import *
from IAA import *

sys.path.append('../../')

#1-N users highlight the same thing, see if agreement score changes
def test_user_highlighting_consensus(config, tmpdir):
    test_path = test_utils.make_test_directory(config, 'test_highlight_consensus')
    for i in range(1, 10):
        #source_task_id generated by smashing keyboard
        dh = datahunt(out_folder=test_path, source_task_id = 'jamietest' + str(i))
        for j in range(i):
            dh.add_row({'answer_label': 'T1.Q2.A2', 'namespace': 'Covid_Evidence2020_03_21', 'contributor_uuid':'human' + str(j),  'start_pos':0, 'end_pos':1})
        for j in range(i, 10):
            dh.add_row({'answer_label': 'T1.Q2.A2', 'namespace': 'Covid_Evidence2020_03_21', 'contributor_uuid':'human' + str(j),  'start_pos':2*j, 'end_pos':(2*j)+1})
        fin_path = dh.export()
        data_path = config['data_dir']
        schema_path = data_path+'/schemas'

    iaa_out = calc_agreement_directory(test_path, schema_path, config['IAA_config_dir'], test_utils.texts_dir, outDirectory = tmpdir)
    i = 1
    scores = []
    for root, dir, files in os.walk(iaa_out):
        for file in files:
            out_df  = pd.read_csv(os.path.join(iaa_out, file), encoding='utf-8')
            print(out_df['highlighted_indices'].tolist())
            #Currently, agreement score does not change based on highlight indices.
            agree_score = out_df['agreement_score'].tolist()[1]
            assert agree_score == 1
            print("When", i, "users have same highlights, agreement score is:", agree_score)
            i += 1

#N users on schema v1 and N users on schema v2--ensure output rows identical
def test_diff_schemas(config, tmpdir):
    test_path = test_utils.make_test_directory(config, 'test_diff_schemas')
    out_path = test_utils.make_test_directory(config, 'out_test_diff_schemas')
    #Covid_Evidence2020_03_21_copy is a copy with Q13 set to Ordinal, which should be detected as a new schema
    for x in [('jamietest_old', 'Covid_Evidence2020_03_21'), ('jamietest_new', 'Covid_Evidence2020_03_21_copy')]:
        dh = datahunt(out_folder=test_path, source_task_id = x[0])
        dh.add_row({'answer_label': 'T1.Q1.A1', 'namespace': x[1], 'contributor_uuid':'A'})
        dh.add_row({'answer_label': 'T1.Q1.A3', 'namespace': x[1], 'contributor_uuid':'B'})
        dh.add_row({'answer_label': 'T1.Q3.A1', 'namespace': x[1], 'contributor_uuid':'C'})
        dh.add_row({'answer_label': 'T1.Q14.A1', 'namespace': x[1], 'contributor_uuid':'D'})
        dh.add_row({'answer_label': 'T1.Q14.A10', 'namespace': x[1], 'contributor_uuid':'E'})
        dh.add_row({'answer_label': 'T1.Q14.A10', 'namespace': x[1], 'contributor_uuid':'F'})
        fin_path = dh.export()
        data_path = config['data_dir']
        schema_path = data_path+'/schemas'

    iaa_out = calc_agreement_directory(test_path, schema_path, config['IAA_config_dir'], test_utils.texts_dir, outDirectory = out_path)
    for root, dir, files in os.walk(iaa_out):
        out_df_old  = pd.read_csv(os.path.join(iaa_out, files[0]), encoding='utf-8')
        out_df_new  = pd.read_csv(os.path.join(iaa_out, files[1]), encoding='utf-8')
    out_df_new = out_df_new.drop(['schema_sha256', 'namespace'], axis=1)
    out_df_old = out_df_old.drop(['schema_sha256', 'namespace'], axis=1)

    assert out_df_old.equals(out_df_new)
