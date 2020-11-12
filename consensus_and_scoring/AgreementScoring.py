import pandas as pd
import numpy as np
import re
from dataV3 import create_dependencies_dict

def AgreementScore(iaaData, schemaPath):
    print("AGREEMENT SCORING TIME!!!")
    print("OLD AGREEMENT SCORES:")
    print(iaaData[['question_Number', 'agreed_Answer', 'agreement_score']])
    #TODO: AGREEMENT SCORE CHANGES HERE
    schemData = pd.read_csv(schemaPath, encoding = 'utf-8')
    dependencies = create_dependencies_dict(schemData)
    iaaQuestions = iaaData['question_Number'].tolist()
    for child in dependencies.keys():
        if child not in iaaQuestions:
            continue
        parents = dependencies[child].keys()
        #TODO: clean this up
        temp = []
        for parent in parents:
            answers = dependencies[child][parent]
            parentScores = iaaData[(iaaData['question_Number'] == parent)]
            parentScores = parentScores[parentScores['agreed_Answer'].astype(int).isin(answers)]
            temp.append(np.mean(parentScores['agreement_score']))
        avgParentScores = np.mean(temp)
        iaaData['agreement_score'] = np.where(iaaData['question_Number'] == child, iaaData['agreement_score'] * avgParentScores, iaaData['agreement_score'])
        #iaaData['agreement_score'] = np.zeros(3)
    print("NEW AGREEMENT SCORES:")
    print(iaaData[['question_Number', 'agreed_Answer', 'agreement_score']])
    return iaaData

# Creates a dictionary of Parent Question: Answer: Child Questions
# ex. {1: {1: [2], 2: [2]}, 2: {1: [4], 5: [4, 5], 8: [3]}, 5: {1: [6], 2: [6], 3: [6]}, 9: {1: [10, 11], 2: [10, 11]}}
# T1.Q1.A1 changes T1.Q2, etc.
# def create_parents_dict(schemadata):
#     df = schemadata[schemadata['answer_next_questions'].notna()]
#     parents = df['answer_label'].tolist()
#     children = df['answer_next_questions'].tolist()
#     dependencies = {}
#     for i in range(len(parents)):
#         parent_q = int(re.findall(r"Q(\d+)", parents[i])[0])
#         parent_a = int(re.findall(r"A(\d+)", parents[i])[0])
#         child_q = [int(q) for q in re.findall(r"Q(\d+)", children[i])]
#         if parent_q not in dependencies:
#             dependencies[parent_q] = {parent_a:child_q}
#         else:
#             dependencies[parent_q][parent_a] = child_q
#     return dependencies
