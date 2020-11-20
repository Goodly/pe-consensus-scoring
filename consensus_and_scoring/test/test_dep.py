import sys
import os
import pandas as pd

import test_utils
from filegen_utils import *
from Dependency import *
import conftest

def test_dep_sample(config):
    iaa_files_path = test_utils.make_test_directory(config, 'dep_sample')
    out_path = test_utils.make_test_directory(config, 'out_dep_sample')
    # source_task_id generated by smashing keyboard
    iaa = IAA_task(out_folder=iaa_files_path, source_task_id='kjncsa87nxao21899102j1j2')
    iaa.add_row({"agreed_Answer": 1, "question_Number": 1, "namespace": 'Covid_Probability',
                 'highlighted_indices': test_utils.make_highlight_indices(10,30)})
    iaa.add_row({"agreed_Answer": 3, "question_Number": 2, "namespace": 'Covid_Probability'})
    fin_path = iaa.export()
    data_path = config['data_dir']
    schema_path = data_path + '/schemas'
    dh_path = None #doesn't get used by dependency but is still an argument

    eval_dependency(dh_path, iaa_files_path, schema_path, out_path)

    for root, dir, files in os.walk(out_path):
        for file in files:
            #should be only 1 file for this case, so just run it on the only one
            # if there's more than 1 then you can get fancy
            out_df  = pd.read_csv(os.path.join(out_path, file), encoding='utf-8')

    #9 answer choices to a checklist question
    assert len(out_df) == 2
    q_three = out_df[out_df['question_Number']==2]
    hl = q_three['highlighted_indices'].iloc[0]
    assert len(hl) >18
    assert '10' in hl
    assert '29' in hl

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

    parents = {1:[2], 2:[3,4,5], 5:[6], 9:[10,11]}
    childNumAnswers = {2:9, 3:1, 4:6, 5:5, 6:3, 7:1, 8:5, 10:5, 11:5}
    for parent in parents:
        iaa = IAA_task(out_folder=iaa_files_path, source_task_id='gru' + str(parent))
        iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "question_Number": parent, "agreed_Answer": 'U'})
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


def test_dep_parent(config):
    #Test if parent has highlight, children don't, dep_iaa should have parent's highlight
    iaa_files_path=test_utils.make_test_directory(config, 'dep_parent')
    outpath=test_utils.make_test_directory(config, 'out_dep_parent')
    # source_task_id generated by smashing keyboard
    all_schema=[ [{"agreed_Answer": 1, "question_Number": 1, "namespace": 'Covid_Probability','highlighted_indices': test_utils.make_highlight_indices(10,30)},{"agreed_Answer": 3, "question_Number": 2, "namespace": 'Covid_Probability'},2],
                 [{"agreed_Answer": 2, "question_Number": 1, "namespace": 'Covid_Languagev1.1','highlighted_indices': test_utils.make_highlight_indices(10,30)},{"agreed_Answer": 3, "question_Number": 3, "namespace": 'Covid_Languagev1.1'},3],
                 [{"agreed_Answer": 4, "question_Number": 15, "namespace": 'Covid_Holisticv1.2','highlighted_indices': test_utils.make_highlight_indices(10, 30)},{"agreed_Answer": 1, "question_Number": 16, "namespace": 'Covid_Holisticv1.2'},16],
                 [{"agreed_Answer": 1, "question_Number": 1, "namespace": 'Covid_Evidence2020_03_21',
                   'highlighted_indices': test_utils.make_highlight_indices(10, 30)},
                  {"agreed_Answer": 1, "question_Number": 2, "namespace": 'Covid_Evidence2020_03_21',
                   'highlighted_indices': test_utils.make_highlight_indices(10, 30)},
                  {"agreed_Answer": 3, "question_Number": 4, "namespace": 'Covid_Evidence2020_03_21'}, 4],
                 [{"agreed_Answer": 1, "question_Number": 1, "namespace": 'Covid_Reasoning', 'highlighted_indices': test_utils.make_highlight_indices(80, 120)},{"agreed_Answer": 1, "question_Number": 2, "namespace": 'Covid_Reasoning','highlighted_indices': test_utils.make_highlight_indices(10, 30)},{"agreed_Answer": 1,
                                                                                      "question_Number": 7,
                                                                                      "namespace": 'Covid_Reasoning'}, 7]
                 ]

    #The test fails if I add these two to the all_Schema list, which I don't understand why since I have been following the same logic
    #[{"agreed_Answer": 1, "question_Number": 2, "namespace": 'Covid_Evidence2020_03_21','highlighted_indices': test_utils.make_highlight_indices(10, 30)},{"agreed_Answer": 3, "question_Number": 4, "namespace": 'Covid_Evidence2020_03_21'}, 4],
    #[{"agreed_Answer": 1, "question_Number": 2, "namespace": 'Covid_Reasoning','highlighted_indices': test_utils.make_highlight_indices(10, 30)}，{"agreed_Answer": 1, "question_Number": 7, "namespace": 'Covid_Reasoning'}, 7]
    for i in all_schema:
        print(i)
        iaa = IAA_task(out_folder=iaa_files_path, source_task_id="auhfdaiughfs")
        for row in i:
            if isinstance(row, dict):
                iaa.add_row(row)
        fin_path = iaa.export()
        data_path = config['data_dir']
        schema_path = data_path + '/schemas'
        dh_path = None #doesn't get used by dependency but is still an argument
        eval_dependency(dh_path, iaa_files_path, schema_path, outpath)
        for root, dir, files in os.walk(outpath):
            for file in files:
                #should be only 1 file for this case, so just run it on the only one
                # if there's more than 1 then you can get fancy
                out_df  = pd.read_csv(os.path.join(outpath, file), encoding='utf-8')
        #9 answer choices to a checklist question
        #This basically works for my first test, child should have parent's highlights if itself doesn't have any but its parent does, thx eric~.
                #assert len(out_df) == 2
                q_three = out_df[out_df['question_Number']==i[-1]]
                hl = q_three['highlighted_indices'].iloc[0]
                assert len(hl) >18
                assert '10' in hl
                assert '29' in hl


def test_dep_parent1(config):
    #Test if parent doesn't have highlight, child does, child still have its highlight
    iaa_files_path=test_utils.make_test_directory(config, 'dep_parent1')
    outpath=test_utils.make_test_directory(config, 'out_dep_parent1')
    # source_task_id generated by smashing keyboard
    all_schema=[ [{"agreed_Answer": 2, "question_Number": 4, "namespace": 'Covid_Sources_2002_03_20v2.1','highlighted_indices': test_utils.make_highlight_indices(10,30)},{"agreed_Answer": 1, "question_Number": 3, "namespace": 'Covid_Sources_2002_03_20v2.1'},4],
                 [{"agreed_Answer": 1, "question_Number": 2, "namespace": 'Covid_Reasoning','highlighted_indices': test_utils.make_highlight_indices(10,30)},{"agreed_Answer": 1, "question_Number": 1, "namespace": 'Covid_Reasoning'},2]
                 ]

    for i in all_schema:
        print(i)
        iaa = IAA_task(out_folder=iaa_files_path, source_task_id="auhfdaiughfs")
        iaa.add_row(i[1])
        iaa.add_row(i[0])
        fin_path = iaa.export()
        data_path = config['data_dir']
        schema_path = data_path + '/schemas'
        dh_path = None #doesn't get used by dependency but is still an argument
        eval_dependency(dh_path, iaa_files_path, schema_path, outpath)
        for root, dir, files in os.walk(outpath):
            for file in files:
                #should be only 1 file for this case, so just run it on the only one
                # if there's more than 1 then you can get fancy
                out_df  = pd.read_csv(os.path.join(outpath, file), encoding='utf-8')
        #9 answer choices to a checklist question
        #This basically works for my first test, child should have parent's highlights if itself doesn't have any but its parent does, thx eric~.
        assert len(out_df) == 2
        q_three = out_df[out_df['question_Number']==i[2]]
        hl = q_three['highlighted_indices'].iloc[0]
        assert len(hl) >18
        assert '10' in hl
        assert '29' in hl