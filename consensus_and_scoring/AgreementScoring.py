import pandas as pd
import numpy as np
import re
from dataV3 import create_dependencies_dict
from nltk import agreement

#Changing Agreement Scores based on Highlights
#To enable, set use=True
#To dimnish the value it scales by, set weight to a lower value
#e.g. if score = 0.5 and weight = 0.5, it scales agscore by 0.75 instead of 0.5
def highlightAgreementScore(starts, ends, weight=1, use=True):
    if not use:
        return 1
    if (not isinstance(starts, list) or not isinstance(ends, list)):
        print("INVALID HIGHLIGHTS")
        return 1
    if len(starts) != len(ends):
        print("INVALID HIGHLIGHTS")
        return 1
    if len(starts) <= 1:
        return 1

    # print("HIGHLIGHT AGREEMENT SCORING TIME!!!")
    first_start = min(starts)
    last_end = max(ends) + 1
    coders = []
    #Creates a list of each annotator's highlights as a list where 0 is an unhighlighted index and 1 is a highlighted index
    #e.g highlightAgreementScore([4, 3, 2], [6, 7, 5]) becomes [[0,0,1,1,1,0], [0,1,1,1,1,1], [1,1,1,1,0,0]]
    for i in range(len(starts)):
        highlights = np.zeros(last_end - first_start)
        highlights[[x for x in range(starts[i] - first_start, ends[i] - first_start + 1)]] = 1
        #print("Highlights for Annotator " + str(i+1) + ": ", highlights)
        coders.append(highlights)

    #Formats the codes properly as (coder,item,label) tuples (required by avg_Ao)
    formatted_codes = []
    for annotator_num in range(len(coders)):
        coder = coders[annotator_num]
        formatted_codes += [[annotator_num+1, ind, coder[ind]] for ind in range(len(coder))]
    ratingtask = agreement.AnnotationTask(data=formatted_codes)

    #Return the weighted average agreement score of all highlights
    avgAg = ratingtask.avg_Ao()
    weighted_avgAg = 1 - ((1 - avgAg) * weight)
    print('Average Pairwise Agreement: ' + str(avgAg) + ', Weighted: ' + str(weighted_avgAg))
    return weighted_avgAg

#Changing Agreement Scores based on Parent Agreement Scores
#To enable, set use=True
#To dimnish the value it scales by, set weight to a lower value
def parentAgreementScore(iaaData, schemaPath, weight=1, use=True):
    if not use:
        return iaaData
    print("PARENT AGREEMENT SCORING TIME!!!")
    print("OLD AGREEMENT SCORES:")
    print(iaaData[['question_Number', 'agreed_Answer', 'agreement_score']])

    #Get a dictionary of children and parents
    schemData = pd.read_csv(schemaPath, encoding = 'utf-8')
    dependencies = create_dependencies_dict(schemData)
    iaaQuestions = iaaData['question_Number'].tolist()

    #For each child, if present in the iaaData, calculate a new agreement score
    for child in dependencies.keys():
        if child not in iaaQuestions:
            continue
        parents = dependencies[child].keys()

        #TODO: clean this bit up?
        #Children can have multiple parent questions that each can have multiple parent answers
        #For each parent question, assign each parent answer score to parentScores, then append the mean score to temp
        temp = []
        for parent in parents:
            answers = dependencies[child][parent]
            parentScores = iaaData[(iaaData['question_Number'] == parent)]
            parentScores = parentScores[parentScores['agreed_Answer'].astype(int).isin(answers)]
            temp.append(np.mean(parentScores['agreement_score']))
        avgParentScores = np.mean(temp)
        weighted_avgParentScores = 1 - ((1 - avgParentScores) * weight)
        iaaData['agreement_score'] = np.where(iaaData['question_Number'] == child,
        iaaData['agreement_score'] * weighted_avgParentScores,  iaaData['agreement_score'])
    print("NEW AGREEMENT SCORES:")
    print(iaaData[['question_Number', 'agreed_Answer', 'agreement_score']])
    return iaaData
