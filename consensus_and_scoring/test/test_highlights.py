from filegen_utils import IAA_task
import pytest
import json
import os
import test_utils
from filegen_utils import *
from IAA import *


def test_highlights(config, tmpdir):
    test_path = test_utils.make_test_directory(config, 'test_iaa_highlights')

    # source_task_id generated by smashing keyboard
    dh = datahunt(out_folder=test_path, source_task_id='highlights')

    for i in range(10):
        dh.add_row({'answer_label': 'T1.Q2.A' + str((i % 3) + 1), 'namespace': 'Covid_Evidence2020_03_21',
                    'contributor_uuid': 'Daniel' + str(i), 'start_pos': 0, 'end_pos': 20})
    fin_path = dh.export()

    data_path = config['data_dir']
    schema_path = data_path + '/schemas'

    # out_path = test_utils.make_test_directory(config, 'out_iaa_hl_everythingpass')
    iaa_out = calc_agreement_directory(test_path, schema_path, config['IAA_config_dir'], test_utils.texts_dir,
                                       outDirectory=tmpdir)
    print(iaa_out)
    for root, dir, files in os.walk(iaa_out):
        for file in files:
            # should be only 1 file for this case, so just run it on the only one
            # if there's more than 1 then you can get fancy
            out_df = pd.read_csv(os.path.join(iaa_out, file), encoding='utf-8')
            temp = out_df["highlighted_indices"]

            for i in range(8):
                counter = 1
                listt = list(map(str, temp[i][1:len(temp[i]) - 1].split(',')))
                for j in range(len(listt)):
                    listt[j] = listt[j].strip()
                if listt != ['']:
                    for num in listt:
                        assert int(num) == counter
                        counter += 1

            print("++++++++++++++")
            print(out_df['coding_perc_agreement'].tolist())

# def test_iaa_hl(config):
#      dh_files_path = test_utils.make_test_directory(config, 'iaa_hl_everythingpass')
#      out_path = test_utils.make_test_directory(config, 'iaa_hl_everythingpass')
#      dh = datahunt(out_folder=dh_files_path, source_task_id='highlight_all_pass')
#      for i in range(5):
#         dh.add_row({'answer_label': 'T1.Q2.A1', 'namespace': 'Covid_Evidence2020_03_21',
#                      'contributor_uuid': 'Daniel' + str(i),'start_pos':5, 'end_pos':20})
#         dh.add_row({'answer_label': 'T1.Q2.A3', 'namespace': 'Covid_Evidence2020_03_21',
#                      'contributor_uuid': 'Daniel' + str(i),'start_pos':5, 'end_pos':24})
#         dh.add_row({'answer_label': 'T1.Q2.A5', 'namespace': 'Covid_Evidence2020_03_21',
#                      'contributor_uuid': 'Daniel' + str(i),'start_pos':5, 'end_pos':26})
#      fin_path = dh.export()
#
#      data_path = config['data_dir']
#      schema_path = data_path + '/schemas'
#
#      iaa_out = calc_agreement_directory(dh_files_path, schema_path, config['IAA_config_dir'], test_utils.texts_dir,
#                                         output_directory=out_path)
#      for root, dir, files in os.walk(iaa_out):
#          for file in files:
#              # should be only 1 file for this case, so just run it on the only one
#              # if there's more than 1 then you can get fancy
#              out_df = pd.read_csv(os.path.join(iaa_out, file), encoding='utf-8')
#      our_q = out_df[out_df['question_Number'] == 2]
#      our_a = out_df[out_df['question_Number'] == 2]
#
#      q2_hl = our_q['highlighted_indices'].iloc[1]
#      assert all([str(i) in q2_hl for i in range(70, 101)])
