import test_utils
from filegen_utils import *
from holistic_eval import *



def test_he_low_info_true_low_counts(config):
    tua_path = test_utils.make_test_directory(config, 'he_tua_input_low_info_true_low_counts')
    scoring_path = test_utils.make_test_directory(config, 'he_scoring_input_low_info_true_low_counts')
    #out_path = test_utils.make_test_directory(config, 'out_he_low_info_true_low_counts')

    pa = point_assignment(out_folder=scoring_path, article_num = '520', source_task_id='practice_makes+[perfect')
    pa.add_row({'namespace':'Covid2_Reasoning_2020_09_20', 'Answer_Number':3,'points':5, "Question_Number":5,
                              'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(10,30)})

    new_tua = tua(out_folder=tua_path, article_num = '520', source_task_id = 'tua_task_id')
    new_tua.add_row({'topic_name': 'argument', 'start_pos': 10, 'end_pos':30, 'tua_uuid': 'test1'})

    hol_dep = dep_iaa(out_folder=scoring_path, source_task_id ='doesnt matter', article_num= '520')
    #scientific discovery
    hol_dep.add_row({"namespace":"Covid2_Holistic_2020_09_20", "agreed_Answer": 5, "question_Number": 1,
                     "agreement_score":1, "tua_uuid": 'test1'})
    hol_dep.export()
    points = eval_triage_scoring(new_tua.df, pa.df, scoring_path)
    #points.to_csv(out_path+'/AssessedPoints.csv', encoding = 'utf-8')
    assert len(points) == 2
    assert points['points'].sum() == 3

def test_he_low_info_true_many_assert(config):
    tua_path = test_utils.make_test_directory(config, 'test_he_low_info_true_many_assert')
    scoring_path = test_utils.make_test_directory(config, 'test_he_low_info_true_many_assert')
    out_path = test_utils.make_test_directory(config, 'out_he_low_info_true_many_assert')

    pa = point_assignment(out_folder=scoring_path, article_num = '520', source_task_id='practice_makes+[perfect')
    pa.add_row({'namespace':'Covid2_Reasoning_2020_09_20', 'Answer_Number':3,'points':5, "Question_Number":5,
                              'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(10,30)})

    new_tua = tua(out_folder=tua_path, article_num = '520', source_task_id = 'tua_task_id')
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 10, 'end_pos':30, 'tua_uuid': 'test1'})
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 40, 'end_pos': 80, 'tua_uuid': 'test3'})
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 40, 'end_pos': 80, 'tua_uuid': 'test5'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 20, 'end_pos': 80, 'tua_uuid': 'test8'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 40, 'end_pos': 60, 'tua_uuid': 'test7'})

    hol_dep = dep_iaa(out_folder=scoring_path, source_task_id ='doesnt matter', article_num= '520')
    #scientific discovery
    hol_dep.add_row({"namespace":"Covid2_Holistic_2020_09_20", "agreed_Answer": 5, "question_Number": 1,
                     "agreement_score":1, "tua_uuid": 'test1'})
    hol_dep.export()
    points = eval_triage_scoring(new_tua.df, pa.df, scoring_path)
    points.to_csv(out_path+'/AssessedPoints.csv', encoding = 'utf-8')
    assert len(points) == 4
    assert points['points'].sum() == -1

def test_he_low_info_true_many_assert_news(config):
    tua_path = test_utils.make_test_directory(config, 'test_he_low_info_true_many_assert_news')
    scoring_path = test_utils.make_test_directory(config, 'test_he_low_info_true_many_assert_news')
    out_path = test_utils.make_test_directory(config, 'out_he_low_info_true_many_assert_news')

    pa = point_assignment(out_folder=scoring_path, article_num = '520', source_task_id='practice_makes+[perfect')
    pa.add_row({'namespace':'Covid2_Reasoning_2020_09_20', 'Answer_Number':3,'points':5, "Question_Number":5,
                              'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(10,30)})

    new_tua = tua(out_folder=tua_path, article_num = '520', source_task_id = 'tua_task_id')
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 10, 'end_pos':30, 'tua_uuid': 'test1'})
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 40, 'end_pos': 80, 'tua_uuid': 'test3'})
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 40, 'end_pos': 80, 'tua_uuid': 'test5'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 20, 'end_pos': 80, 'tua_uuid': 'test8'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 40, 'end_pos': 60, 'tua_uuid': 'test7'})

    hol_dep = dep_iaa(out_folder=scoring_path, source_task_id ='doesnt matter', article_num= '520')
    #scientific discovery
    hol_dep.add_row({"namespace":"Covid2_Holistic_2020_09_20", "agreed_Answer": 1, "question_Number": 1,
                     "agreement_score":1, "tua_uuid": 'test1'})
    hol_dep.export()
    points = eval_triage_scoring(new_tua.df, pa.df, scoring_path)
    points.to_csv(out_path+'/AssessedPoints.csv', encoding = 'utf-8')
    assert len(points) == 4
    assert points['points'].sum() == -4

def test_he_low_info_false_many_assert(config):
    tua_path = test_utils.make_test_directory(config, 'test_he_low_info_false_many_assert')
    scoring_path = test_utils.make_test_directory(config, 'test_he_low_info_false_many_assert')
    out_path = test_utils.make_test_directory(config, 'out_he_low_info_false_many_assert')

    pa = point_assignment(out_folder=scoring_path, article_num = '520', source_task_id='practice_makes+[perfect')
    pa.add_row({'namespace':'Covid2_Reasoning_2020_09_20', 'Answer_Number':3,'points':5, "Question_Number":5,
                              'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(10,30)})

    new_tua = tua(out_folder=tua_path, article_num = '520', source_task_id = 'tua_task_id')
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 10, 'end_pos':30, 'tua_uuid': 'test1'})
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 40, 'end_pos': 80, 'tua_uuid': 'test3'})
    new_tua.add_row({'topic_name': 'Assertions', 'start_pos': 40, 'end_pos': 80, 'tua_uuid': 'test5'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 20, 'end_pos': 80, 'tua_uuid': 'test8'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 40, 'end_pos': 60, 'tua_uuid': 'test7'})
    new_tua.add_row({'topic_name': 'Evidence', 'start_pos': 20, 'end_pos': 80, 'tua_uuid': 'test9'})
    new_tua.add_row({'topic_name': 'Evidence', 'start_pos': 40, 'end_pos': 60, 'tua_uuid': 'test10'})
    new_tua.add_row({'topic_name': 'Evidence', 'start_pos': 20, 'end_pos': 80, 'tua_uuid': 'test11'})
    new_tua.add_row({'topic_name': 'Evidence', 'start_pos': 40, 'end_pos': 60, 'tua_uuid': 'test12'})

    hol_dep = dep_iaa(out_folder=scoring_path, source_task_id ='doesnt matter', article_num= '520')
    #scientific discovery
    hol_dep.add_row({"namespace":"Covid2_Holistic_2020_09_20", "agreed_Answer": 5, "question_Number": 1,
                     "agreement_score":1, "tua_uuid": 'test1'})
    hol_dep.export()
    points = eval_triage_scoring(new_tua.df, pa.df, scoring_path)
    points.to_csv(out_path+'/AssessedPoints.csv', encoding = 'utf-8')
    assert len(points) == 1
    assert points['points'].sum() == 5

def test_he_vague_sources_true(config):
    tua_path = test_utils.make_test_directory(config, 'he_tua_vague_sources_true')
    scoring_path = test_utils.make_test_directory(config, 'he_scoring_vague_sources_true')
    out_path = test_utils.make_test_directory(config, 'out_he_vague_sources_true')
    #2800 is considered standard article; threhold for scoring is 4 vague sources per 2800 characters
    pa = point_assignment(out_folder=scoring_path, article_num='520', source_task_id='practice_makes+[perfect',
                          article_text_length = 2700)
    pa.add_row({'namespace': 'Covid2_Reasoning_2020_09_20', 'Answer_Number': 3, 'points': 0, "Question_Number": 5,
                'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(10, 30)})
    pa.export()
    src_dep = dep_iaa(out_folder=scoring_path, source_task_id='qs1', article_num='520',
                      article_text_length = 2700)
    # scientific discovery
    src_dep.add_row({'namespace': 'Covid2_Sources_2002_09_20', 'agreed_Answer': 5,  "question_Number": 2,
                'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(10, 30),
                     'tua_uuid': 'tua3'})
    src_dep.export()
    src_dep = dep_iaa(out_folder=scoring_path, source_task_id='qs2', article_num='520',
                      article_text_length = 2700)
    src_dep.add_row({'namespace': 'Covid2_Sources_2002_09_20', 'agreed_Answer': 6, "question_Number": 2,
                'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(15, 38),
                     'tua_uuid': 'tua3'})
    src_dep.add_row({'namespace': 'Covid2_Sources_2002_09_20', 'agreed_Answer': 8, "question_Number": 5,
                'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(7, 27),
                     'tua_uuid': 'tua3'})
    src_dep.export()
    src_dep = dep_iaa(out_folder=scoring_path, source_task_id='qs3', article_num='520',
                      article_text_length = 2700)
    src_dep.add_row({'namespace': 'Covid2_Sources_2002_09_20', 'agreed_Answer': 7,  "question_Number": 5,
                     'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(15, 38),
                     'tua_uuid': 'tua3'})
    src_dep.export()

    new_tua = tua(out_folder=tua_path, article_num='520', source_task_id='tua_task_id',
                  article_text_length = 2700)
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 10, 'end_pos': 30, 'tua_uuid': 'tua1'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 50, 'end_pos': 120, 'tua_uuid': 'tua2'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 900, 'end_pos': 1020, 'tua_uuid': 'tua3'})
    new_tua.export()

    points = eval_triage_scoring(new_tua.df, pa.df, scoring_path)
    points.to_csv(out_path+'/AssessedPoints.csv', encoding = 'utf-8')

    assert points['points'].sum() == -14
    assert len(points) == 4
    hl = points[points['points']== -2]['highlighted_indices'].iloc[0]
    assert all([str(i) in hl for i in range(900, 1020)])

def test_he_vague_sources_false(config):
    tua_path = test_utils.make_test_directory(config, 'he_tua_vague_sources_false')
    scoring_path = test_utils.make_test_directory(config, 'he_scoring_vague_sources_false')
    out_path = test_utils.make_test_directory(config, 'out_he_vague_sources_false')
    #2800 is considered standard article; threhold for scoring is 4 vague sources per 2800 characters
    pa = point_assignment(out_folder=scoring_path, article_num='520', source_task_id='practice_makes+[perfect',
                          article_text_length = 2900)
    pa.add_row({'namespace': 'Covid2_Reasoning_2020_09_20', 'Answer_Number': 3, 'points': 0, "Question_Number": 5,
                'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(10, 30)})
    pa.export()
    src_dep = dep_iaa(out_folder=scoring_path, source_task_id='qs1', article_num='520',
                      article_text_length = 2900)
    # scientific discovery
    src_dep.add_row({'namespace': 'Covid2_Sources_2002_09_20', 'agreed_Answer': 5,  "question_Number": 2,
                'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(10, 30),
                     'tua_uuid': 'tua3'})
    src_dep.export()
    src_dep = dep_iaa(out_folder=scoring_path, source_task_id='qs2', article_num='520',
                      article_text_length = 2900)
    src_dep.add_row({'namespace': 'Covid2_Sources_2002_09_20', 'agreed_Answer': 6, "question_Number": 2,
                'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(15, 38),
                     'tua_uuid': 'tua3'})
    src_dep.add_row({'namespace': 'Covid2_Sources_2002_09_20', 'agreed_Answer': 8, "question_Number": 5,
                'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(7, 27),
                     'tua_uuid': 'tua3'})
    src_dep.export()
    src_dep = dep_iaa(out_folder=scoring_path, source_task_id='qs3', article_num='520',
                      article_text_length = 2900)
    src_dep.add_row({'namespace': 'Covid2_Sources_2002_09_20', 'agreed_Answer': 7,  "question_Number": 5,
                     'agreement_score': 1, 'highlighted_indices': test_utils.make_highlight_indices(15, 38),
                     'tua_uuid': 'tua3'})
    src_dep.export()

    new_tua = tua(out_folder=tua_path, article_num='520', source_task_id='tua_task_id',
                  article_text_length = 2900)
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 10, 'end_pos': 30, 'tua_uuid': 'tua1'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 50, 'end_pos': 120, 'tua_uuid': 'tua2'})
    new_tua.add_row({'topic_name': 'Quoted Sources', 'start_pos': 900, 'end_pos': 1020, 'tua_uuid': 'tua3'})
    new_tua.export()

    points = eval_triage_scoring(new_tua.df, pa.df, scoring_path)
    points.to_csv(out_path+'/AssessedPoints.csv', encoding = 'utf-8')

    assert points['points'].sum() == -4
    assert len(points) == 3
    hl = points[points['points']== -2]['highlighted_indices'].iloc[0]
    assert all([str(i) in hl for i in range(900, 1020)])

