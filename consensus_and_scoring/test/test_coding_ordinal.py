import pandas as pd

import test_utils

from IAA import *


def test_coding_ordinal_evidence():
    questions=["T1.Q8","T1.Q10","T1.Q11","T1.Q12","T1.Q13","T1.Q14"]
    qn=[8,10,11,12,13,14]
    answers=["T1.Q8.A3","T1.Q10.A3","T1.Q11.A3","T1.Q12.A3","T1.Q13.A5","T1.Q14.A7"]
    up_answers=["T1.Q8.A2","T1.Q10.A2","T1.Q11.A2","T1.Q12.A2","T1.Q13.A4","T1.Q14.A6"]
    down_answers=["T1.Q8.A1","T1.Q10.A1","T1.Q11.A1","T1.Q12.A1","T1.Q13.A3","T1.Q14.A5"]
    for i in range(len(answers)):
        file=pd.read_csv("../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence/"+"Covid_Evidencev1-Task-2224-DataHunt.csv")
        file=file[file["question_label"]==questions[i]]
        file=file.replace({"answer_label":up_answers[i]},answers[i])
        file.to_csv("../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence/up/"+"Covid_Evidencev1-Task-2224-DataHunt.csv")

        out_path = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_ordinal_evidence"
        texts_dir = '../../data/texts/'
        config_path = '../config/'
        schema = "../../data/schemas//45dce5251bd3ea6e908fa33ac9e6a8e17e6830215912ce1626cf4206e159819c.csv"
        file = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence/Covid_Evidencev1-Task-2224-DataHunt.csv"
        a = calc_scores(file, config_path, texts_dir, schemaFile=schema, outDirectory=out_path,
                        directory='../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence')
        for root, dir, files in os.walk(a):
            for f in files:
                if f.endswith('.csv'):
                    result = pd.read_csv(a + f)
        result = result[result["question_Number"] == qn[i]]
        agreement_score = result["agreement_score"].iloc[0]

        out2 = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_ordinal_evidence/up"
        file2 = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence/up/Covid_Evidencev1-Task-2224-DataHunt.csv"
        c = calc_scores(file2, config_path, texts_dir, schemaFile=schema, outDirectory=out2,
                        directory='../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence/up')
        for root, dir, files in os.walk(c):
            for f in files:
                if f.endswith('.csv'):
                    result2 = pd.read_csv(c + f)
        result2 = result2[result2["question_Number"] == qn[i]]
        agreement_score2 = result2["agreement_score"].iloc[0]
        assert agreement_score<agreement_score2

        file3 = pd.read_csv("../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence/" + "Covid_Evidencev1-Task-2224-DataHunt.csv")
        file3 = file3[file3["question_label"] == questions[i]]
        file3 = file3.replace({"answer_label": up_answers[i]}, down_answers[i])
        file3.to_csv("../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence/down/" + "Covid_Evidencev1-Task-2224-DataHunt.csv")

        out3 = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_ordinal_evidence/down"
        file4 = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence/down/Covid_Evidencev1-Task-2224-DataHunt.csv"
        b = calc_scores(file4, config_path, texts_dir, schemaFile=schema, outDirectory=out3,
                        directory='../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_evidence/down')
        for root, dir, files in os.walk(b):
            for f in files:
                if f.endswith('.csv'):
                    result3 = pd.read_csv(b + f)
        result3 = result3[result3["question_Number"] == qn[i]]
        agreement_score3 = result3["agreement_score"].iloc[0]
        assert agreement_score3 < agreement_score
        assert agreement_score < agreement_score2


def test_coding_ordinal_probability():
    questions = ["T1.Q1", "T1.Q11"]
    qn = [1, 11]
    answers = ["T1.Q1.A3", "T1.Q11.A4"]
    up_answers = ["T1.Q1.A2", "T1.Q11.A3"]
    down_answers = ["T1.Q1.A1", "T1.Q11.A2"]

    for i in range(len(answers)):
        file = pd.read_csv(
            "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability/" + "Covid_Probabilityv1-Task-2231-DataHunt.csv")
        file = file[file["question_label"] == questions[i]]
        file = file.replace({"answer_label": up_answers[i]}, answers[i])

        file.to_csv(
            "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability/up/" + "Covid_Probabilityv1-Task-2231-DataHunt.csv")

        out_path = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_ordinal_probability"
        texts_dir = '../../data/texts/'
        config_path = '../config/'
        schema = "../../data/schemas//d5f24573a7077ee3ebb203ca64f955a8c5d5a6c6b25aa65e1bfa6fa71d1311f2.csv"
        file = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability/Covid_Probabilityv1-Task-2231-DataHunt.csv"
        a = calc_scores(file, config_path, texts_dir, schemaFile=schema, outDirectory=out_path,
                        directory='../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability')
        for root, dir, files in os.walk(a):
            for f in files:
                if f.endswith('.csv'):
                    result = pd.read_csv(a + f)
        result = result[result["question_Number"] == qn[i]]
        agreement_score = result["agreement_score"].iloc[0]

        out2 = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_ordinal_probability/up"
        file2 = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability/up/Covid_Probabilityv1-Task-2231-DataHunt.csv"
        print(i)
        c = calc_scores(file2, config_path, texts_dir, schemaFile=schema, outDirectory=out2,
                        directory='../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability/up')
        for root, dir, files in os.walk(c):
            for f in files:
                if f.endswith('.csv'):
                    result2 = pd.read_csv(c + f)
        result2 = result2[result2["question_Number"] == qn[i]]
        agreement_score2 = result2["agreement_score"].iloc[0]
        print(i)
        assert agreement_score <= agreement_score2

        file3 = pd.read_csv(
            "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability/" + "Covid_Probabilityv1-Task-2231-DataHunt.csv")
        file3 = file3[file3["question_label"] == questions[i]]
        file3 = file3.replace({"answer_label": up_answers[i]}, down_answers[i])
        file3.to_csv(
            "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability/down/" + "Covid_Probabilityv1-Task-2231-DataHunt.csv")

        out3 = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_ordinal_probability/down"
        file4 = "../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability/down/Covid_Probabilityv1-Task-2231-DataHunt.csv"
        b = calc_scores(file4, config_path, texts_dir, schemaFile=schema, outDirectory=out3,
                        directory='../../test_data/calcscore_iaa_distance_ordinal/test_coding_input_probability/down')
        for root, dir, files in os.walk(b):
            for f in files:
                if f.endswith('.csv'):
                    result3 = pd.read_csv(b + f)
        result3 = result3[result3["question_Number"] == qn[i]]
        agreement_score3 = result3["agreement_score"].iloc[0]
        assert agreement_score3 <= agreement_score


