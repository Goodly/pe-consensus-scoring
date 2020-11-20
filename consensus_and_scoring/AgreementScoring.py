import pandas as pd
import numpy as np
import re
from dataV3 import create_dependencies_dict
from nltk import agreement

#Takes in starts and ends of highlights for a specific question answer, returns factor to scale answer's agreement score by
def highlightAgreementScore(starts, ends):
    assert len(starts) == len(ends)
    if len(starts) == 0:
        return 0
    if len(starts) == 1:
        return 1

    print("HIGHLIGHT AGREEMENT SCORING TIME!!!")
    first_start = min(starts)
    last_end = max(ends) + 1
    coders = []
    #Creates a list of each annotator's highlights as a list where 0 is an unhighlighted index and 1 is a highlighted index
    #e.g highlightAgreementScore([4, 3, 2], [6, 7, 5]) becomes [[0,0,1,1,1,0], [0,1,1,1,1,1], [1,1,1,1,0,0]]
    for i in range(len(starts)):
        highlights = np.zeros(last_end - first_start)
        highlights[[x for x in range(starts[i] - first_start, ends[i] - first_start + 1)]] = 1
        print("Highlights " + str(i+1) + ": ", highlights)
        coders.append(highlights)

    #Formats the codes properly as (coder,item,label) tuples
    formatted_codes = []
    for annotator_num in range(len(coders)):
        coder = coders[annotator_num]
        formatted_codes += [[annotator_num+1, ind, coder[ind]] for ind in range(len(coder))]
    ratingtask = agreement.AnnotationTask(data=formatted_codes)

    avgAg = ratingtask.avg_Ao()
    print('AVERAGE PAIRWISE AGREEMENT: ',avgAg)
    # alpha = ratingtask.alpha()
    # print('Krippendorff\'s alpha:',alpha)
    # if alpha != 1: #other metrics error if alpha is 1
    #     print('Fleiss\'s Kappa:',ratingtask.multi_kappa())
    #     print('Scott\'s pi:',ratingtask.pi())
    return avgAg

highlightAgreementScore([2, 2, 2, 2, 2], [15, 15, 15, 15, 15])

#Parent Agrement Scoring
def AgreementScore(iaaData, schemaPath):
    print("PARENT AGREEMENT SCORING TIME!!!")
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

#Agreement scoring but scores of parents of parents don't affect children
def AgreementScoreReverse(iaaData, schemaPath):
    print("PARENT AGREEMENT SCORING TIME!!!")
    print("OLD AGREEMENT SCORES:")
    print(iaaData[['question_Number', 'agreed_Answer', 'agreement_score']])
    #TODO: AGREEMENT SCORE CHANGES HERE
    schemData = pd.read_csv(schemaPath, encoding = 'utf-8')
    dependencies = create_dependencies_dict(schemData)
    iaaQuestions = iaaData['question_Number'].tolist()
    reversed_keys = list(dependencies.keys())[::-1]
    for child in reversed_keys:
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

#Just for testing out the differences betwene metrics
def metricTest():
    coder1 = [1,0,2,0,1,1,2,0,1,1]
    coder2 = [1,1,0,0,1,1,2,1,1,0]
    coder3 = [1,2,2,1,2,1,2,1,1,0]
    formatted_codes = [[1,i,coder1[i]] for i in range(len(coder1))] + [[2,i,coder2[i]] for i in range(len(coder2))]  + [[3,i,coder3[i]] for i in range(len(coder3))]
    print('RUNNING METRIC TEST')
    ratingtask = agreement.AnnotationTask(data=formatted_codes)
    print('Average pairwise agreement: ',ratingtask.avg_Ao())
    print('Cohen\'s Kappa:',ratingtask.kappa())
    print('Fleiss\'s Kappa:',ratingtask.multi_kappa())
    print('Krippendorff\'s alpha:',ratingtask.alpha())
    print('Scott\'s pi:',ratingtask.pi())
#metricTest()

# Creates a dictionary of Parent Question: Answer: Child Questions
# ex. {1: {1: [2], 2: [2]}, 2: {1: [4], 5: [4, 5], 8: [3]}, 5: {1: [6], 2: [6], 3: [6]}, 9: {1: [10, 11], 2: [10, 11]}}
# T1.Q1.A1 changes T1.Q2, etc.
# I wrote this function and it works but didn't actually end up using it since create_dependencies_dict was better
def create_parents_dict(schemadata):
    df = schemadata[schemadata['answer_next_questions'].notna()]
    parents = df['answer_label'].tolist()
    children = df['answer_next_questions'].tolist()
    dict = {}
    for i in range(len(parents)):
        parent_q = int(re.findall(r"Q(\d+)", parents[i])[0])
        parent_a = int(re.findall(r"A(\d+)", parents[i])[0])
        child_q = [int(q) for q in re.findall(r"Q(\d+)", children[i])]
        if parent_q not in dict:
            dict[parent_q] = {parent_a:child_q}
        else:
            dict[parent_q][parent_a] = child_q
    return dict
