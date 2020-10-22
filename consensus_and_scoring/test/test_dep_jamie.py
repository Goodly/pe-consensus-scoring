import sys
import os
import pandas as pd

import test_utils
from filegen_utils import *
from Dependency import *
import conftest

#a question that passes IAA and has no parents should appear in out_path
def test_no_parent_pass(config):
    iaa_files_path = test_utils.make_test_directory(config, 'dep_orphan_pass')
    out_path = test_utils.make_test_directory(config, 'dep_orphan_pass_out')
    #The questions with no parents in the Evidence schema are 1, 9, 12, 13, and 14
    numAnswers = {1:3, 9:3, 12:4, 13:10, 14:10}
    for i in [1, 9, 12, 13, 14]:
        iaa = IAA_task(out_folder=iaa_files_path, source_task_id='batman' + str(i))
        for j in range(1, numAnswers[i]+1):
            iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "question_Number": i, "agreed_Answer": j})
        #Question 3 has Question 2 as a parent, so it should never appear in any dependencies
        iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "question_Number": 3, "agreed_Answer": 1})
        fin_path = iaa.export()
    data_path = config['data_dir']
    schema_path = data_path + '/schemas'
    dh_path = None
    eval_dependency(dh_path, iaa_files_path, schema_path, out_path)
    for root, dir, files in os.walk(out_path):
        for file in files:
            out_df  = pd.read_csv(os.path.join(out_path, file), encoding='utf-8')
            q_num = out_df['question_Number'].iloc[0]
            #Check that the length of the dependency is the same as the number of answers for the orphan question (3 is excluded)
            print(q_num, len(out_df))
            assert len(out_df) == numAnswers[q_num]

#tests all question that pass IAA and have no parents combined appearing in out_path
def test_all_no_parent_pass(config):
    iaa_files_path = test_utils.make_test_directory(config, 'dep_all_orphans_pass')
    out_path = test_utils.make_test_directory(config, 'dep_all_orphans_pass_out')
    #The questions with no parents in the Evidence schema are 1, 9, 12, 13, and 14
    numAnswers = {1:3, 9:3, 12:4, 13:10, 14:10}
    iaa = IAA_task(out_folder=iaa_files_path, source_task_id='batman')
    for i in [1, 9, 12, 13, 14]:
        for j in range(1, numAnswers[i]+1):
            iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "question_Number": i, "agreed_Answer": j})
        #Question 3 has Question 2 as a parent, so it should never appear in any dependencies
    iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "question_Number": 3, "agreed_Answer": 1})
    fin_path = iaa.export()
    data_path = config['data_dir']
    schema_path = data_path + '/schemas'
    dh_path = None

    eval_dependency(dh_path, iaa_files_path, schema_path, out_path)
    for root, dir, files in os.walk(out_path):
        for file in files:
            out_df  = pd.read_csv(os.path.join(out_path, file), encoding='utf-8')
            assert len(out_df) == 30

#if a child question passes IAA and its parent fails, neither should appear in dep_iaa output file
def test_bad_parent(config):
    iaa_files_path = test_utils.make_test_directory(config, 'dep_bad_dad')
    out_path = test_utils.make_test_directory(config, 'dep_bad_dad_out')

    parents = {1:[2], 2:[3,4,5,7,8], 5:[6], 9:[10,11]}
    childNumAnswers = {2:9, 3:1, 4:6, 5:5, 6:3, 7:1, 8:5, 10:5, 11:5}
    for parent in parents:
        iaa = IAA_task(out_folder=iaa_files_path, source_task_id='gru' + str(parent))
        iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "question_Number": parent, "agreed_Answer": 'Fail'})
        for child in parents[parent]:
            for j in range(1, childNumAnswers[child]+1):
                iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "question_Number": child, "agreed_Answer": j})
        fin_path = iaa.export()
    data_path = config['data_dir']
    schema_path = data_path + '/schemas'
    dh_path = None

    eval_dependency(dh_path, iaa_files_path, schema_path, out_path)
    for root, dir, files in os.walk(out_path):
        for file in files:
            out_df  = pd.read_csv(os.path.join(out_path, file), encoding='utf-8')
            assert len(out_df) == 0, "failing file is " + str(file)
