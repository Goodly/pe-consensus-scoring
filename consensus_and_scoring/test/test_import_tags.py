
import pandas as pd

import test_utils
from filegen_utils import *
from import_tags import *


def test_demo(make_task_demo):
    print(make_task_demo)
    df = pd.read_csv(make_task_demo)
    assert len(df.index) == 1
    #uncomment this to fail!
    #assert len(df.index) == 2

#todo Come up with other testing util funcs for comparing inputs/outputs
def test_iaa_evi_3q(config):
    out_path = test_utils.make_test_directory(config, 'iaa_evi_q5')
    #source_task_id generated by smashing keyboard
    iaa = IAA_task(out_folder=out_path, source_task_id = 'kjncsa87nxao21899102j1j2')
    iaa.add_row({"agreed_Answer": 800, "question_Number": 8})
    iaa.add_row({"agreed_Answer": 800, "question_Number": 5})
    iaa.add_row({"agreed_Answer": 800, "question_Number": 5})
    fin_path = iaa.export()
    read_iaa = pd.read_csv(fin_path, encoding='utf-8')
    assert len(read_iaa) == 3
    count = test_utils.count_matching_rows(read_iaa, {'agreed_Answer':800,
                                                      'question_Number': 8})
    assert count == 1
    count = test_utils.count_matching_rows(read_iaa, {'agreed_Answer': 800})
    assert count == 3

def test_import_tags_adj_1_iaa_1_disagree(config, tmpdir):
    iaa_path = test_utils.make_test_directory(config, 'imptags_iaa_1_iaa_1_adj')
    adj_path = test_utils.make_test_directory(config, 'imptags_adj_1_iaa_1_adj')
    schema_path = config['data_dir'] + '/schemas'
    schema_namespace = 'Covid_Evidence2020_03_21'
    # source_task_id generated by smashing keyboard
    task_id = 'nc87wehcolfg6caanc9w'
    iaa = IAA_task(out_folder=iaa_path, source_task_id= task_id)
    iaa.add_row({"question_Number": 1, "agreed_Answer": 3, 'namespace': schema_namespace})
    iaa.export()
    adj = adjudicator(out_folder = adj_path, source_task_id = task_id)
    adj.add_row({'topic_name':'01.02.02', 'namespace': schema_namespace})
    adj.export()
    i_tags = import_tags(iaa_path, adj_path, schema_path, tmpdir)
    print('temp dir is:', tmpdir, i_tags)
    #i_tags is path to the single file:
    #this is an example of how you would walk through an entire directory
    # for root, dir, files in os.walk(i_tags):
    #     print("files found")
    #     for file in files:
    #         print(file)
    #         #should be only 1 file
    #         i_df  = pd.read_csv(os.path.join(i_tags, file), encoding='utf-8')
    i_df = pd.read_csv(i_tags, encoding='utf-8')
    assert len(i_df) == 1
    assert test_utils.count_matching_rows(i_df, {'agreed_Answer': 2, 'question_Number': 2}) == 1
    assert test_utils.count_matching_rows(i_df, {'agreed_Answer': 1, 'question_Number': 3}) == 0


