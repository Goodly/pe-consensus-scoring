import sys
import os
import pandas as pd

import test_utils
from filegen_utils import *
from Dependency import *
import conftest

from consensus_and_scoring.test.filegen_utils import IAA_task
from consensus_and_scoring.Dependency import eval_dependency

"""
When a child question has a highlight that passes IAA and it's parent normally would have a highlight,
but the highlight failed IAA even though the coding passed IAA, the child question should still have its highlight.
"""
def test_iaa_child_retains_highlight_if_parent_fails(config):
    iaa_files_path = test_utils.make_test_directory(config, 'child_retains_highlight_if_parent_fails')
    out_path = test_utils.make_test_directory(config, 'out_child_retains_highlight_if_parent_fails')
    iaa = IAA_task(out_folder=iaa_files_path, source_task_id='70f61bac-eb02-4b98-a2b7-293b7cc05404')
    # Parent question
    iaa.add_row({'agreed_Answer': 1,
                 'question_Number': 1,
                 'namespace': 'Covid_Probability',
                 'question_label': 'T1.Q1',
                 'answer_next_questions': 'T1.Q2',
                 'coding_perc_agreement': 1,
                 'highlighted_indices: []})
    # Child question
    iaa.add_row({'agreed_Answer': 3,
                 'question_Number': 2,
                 'namespace': 'Covid_Probability',
                 'question_label': 'T1.Q2',
                 'highlighted_indices': test_utils.make_highlight_indices(70, 100)})
    fin_path = iaa.export()
    data_path = config['data_dir']
    schema_path = data_path + '/schemas'
    dh_path = None

    eval_dependency(dh_path, iaa_files_path, schema_path, out_path)

    for _, _, files in os.walk(out_path):
        for file in files:
            out_df = pd.read_csv(os.path.join(out_path, file), encoding='utf-8')

    child_q = out_df[out_df['question_Number'] == 2]
    child_q_hl = child_q['highlighted_indices'].iloc[0]

    assert len(out_df) == 2
    assert all([str(i) in child_q_hl for i in range(70, 101)])


"""
When a child question has a highlight that passes IAA and it's parent also has a highlight that passed IAA,
the dep_iaa should have both of the highlights combined.  Make sure this works when the child/parent highlights
are fully disjoint and when there's some overlap.
Developed Higher-Order Function to abstract away the "absorb highlights" test from
disjoint/overlapping highlighted indices
"""
def hof_test_iaa_questions_absorb_highlights(config, parent_hl_start, parent_hl_end, child_hl_start, child_hl_end):
    iaa_files_path = test_utils.make_test_directory(config, 'iaa_questions_absorb_highlights')
    out_path = test_utils.make_test_directory(config, 'out_iaa_questions_absorb_highlights')
    iaa = IAA_task(out_folder=iaa_files_path, source_task_id='7b4af00f-3045-4264-933b-08f6598f0920')
    # Parent question
    iaa.add_row({'agreed_Answer': 1,
                 'question_Number': 1,
                 'namespace': 'Covid_Probability',
                 'question_label': 'T1.Q1',
                 'answer_next_questions': 'T1.Q2',
                 'highlighted_indices': test_utils.make_highlight_indices(parent_hl_start, parent_hl_end)})
    # Child question
    iaa.add_row({'agreed_Answer': 3,
                 'question_Number': 2,
                 'namespace': 'Covid_Probability',
                 'question_label': 'T1.Q2',
                 'highlighted_indices': test_utils.make_highlight_indices(child_hl_start, child_hl_end)})
    fin_path = iaa.export()
    data_path = config['data_dir']
    schema_path = data_path + '/schemas'
    dh_path = None

    eval_dependency(dh_path, iaa_files_path, schema_path, out_path)

    for _, _, files in os.walk(out_path):
        for file in files:
            out_df = pd.read_csv(os.path.join(out_path, file), encoding='utf-8')

    parent_q = out_df[out_df['question_Number'] == 1]
    parent_q_hl = parent_q['highlighted_indices'].iloc[0]

    child_q = out_df[out_df['question_Number'] == 2]
    child_q_hl = child_q['highlighted_indices'].iloc[0]

    assert len(out_df) == 2
    assert all([str(i) in parent_q_hl for i in range(parent_hl_start, parent_hl_end)])
    assert all([str(i) in child_q_hl for i in range(parent_hl_start, parent_hl_end)])
    assert all([str(i) in parent_q_hl for i in range(child_hl_start, child_hl_end)])
    assert all([str(i) in child_q_hl for i in range(child_hl_start, child_hl_end)])


def test_iaa_questions_absorb_highlights_disjoint(config):
    hof_test_iaa_questions_absorb_highlights(config, 10, 30, 70, 100)


def test_iaa_questions_absorb_highlights_overlap(config):
    hof_test_iaa_questions_absorb_highlights(config, 10, 30, 20, 50)