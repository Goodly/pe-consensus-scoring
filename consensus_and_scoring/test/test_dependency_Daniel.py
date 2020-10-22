import pandas as pd
import sys
import os
import pandas as pd

import test_utils
from filegen_utils import *
from IAA import *
from Dependency import *
from Weighting import *


sys.path.append('../../')

def test_dep_iaa_failures(config, tmpdir):
    out_path = test_utils.make_test_directory(config, 'testing_dep_iaa')
    weight_out_folder = test_utils.make_test_directory(config, 'out_testing_dep_iaa')
    iaa = IAA_task(out_folder=out_path, source_task_id='weightsampletests')
    #-.5 points--from the weight key in config folder and the agreement_score
    iaa.add_row({"namespace":"Covid_Evidence2020_03_21", "agreed_Answer": 1, "agreement_score":'L', "question_Number": 4})
    #-2 points from ./config/weight_key and agreement score
    iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "agreed_Answer": 2,  "agreement_score":'U',"question_Number": 5})
    iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "agreed_Answer": 0,  "agreement_score":'M',"question_Number": 6})

    # +1.5 points from ./config/weight_key and agreement score
    # iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "agreed_Answer": 'L', "question_Number": 12})
    fin_path = iaa.export()

    data_path = config['data_dir']
    schema_path = data_path + '/schemas'
    iaa_path = config['test_dir'] +'testing_dep_iaa'

    eval_dependency(out_path, iaa_path, schema_path, config['test_dir'])
    dep_path = config['test_dir'] + 'Dep_testing_dep_iaa/' +'iaa_weightsampletests.csv'
    dep = pd.read_csv(dep_path, encoding='utf-8')
    print(dep)
    # if it fails the iaa tests due to the agreement_score, the shouldn't appear in the dep_iaa file
    assert len(dep['prereq_passed']) == 0

def test_dep_iaa_failures(config, tmpdir):
    out_path = test_utils.make_test_directory(config, 'testing_dep_iaa')
    weight_out_folder = test_utils.make_test_directory(config, 'out_testing_dep_iaa')
    iaa = IAA_task(out_folder=out_path, source_task_id='weightsampletests2')

    #-2 points from ./config/weight_key and agreement score
    iaa.add_row({"namespace":"Covid_Evidence2020_03_21", "agreed_Answer": 'L', "coding_perc_agreement":0.2, "question_Number": 9, "num_users":5})
    iaa.add_row({"namespace":"Covid_Evidence2020_03_21", "agreed_Answer": 'U', "question_Number": 9, "num_users":5})
    iaa.add_row({"namespace":"Covid_Evidence2020_03_21", "agreed_Answer": 2, "agreement_score": 'M', "question_Number": 9, "num_users":5})
    iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "agreed_Answer": 2,  "agreement_score": 0.9,"question_Number": 10, "num_users":5})


    # +1.5 points from ./config/weight_key and agreement score
    # iaa.add_row({"namespace": "Covid_Evidence2020_03_21", "agreed_Answer": 'L', "question_Number": 12})
    fin_path = iaa.export()

    data_path = config['data_dir']
    schema_path = data_path + '/schemas'
    iaa_path = config['test_dir'] +'testing_dep_iaa'

    eval_dependency(out_path, iaa_path, schema_path, config['test_dir'])
    dep_path = config['test_dir'] + 'Dep_testing_dep_iaa/' +'iaa_weightsampletests2.csv'
    dep = pd.read_csv(dep_path, encoding='utf-8')
    print(dep)
    # if it fails the iaa tests due to the agreement_score, the shouldn't appear in the dep_iaa file
    assert len(dep['prereq_passed']) == 0