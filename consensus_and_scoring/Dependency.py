import math
import pandas as pd
import numpy as np
import os
import json
from dataV3 import *

def eval_dependency(directory, iaa_dir, schema_dir, out_dir):
    print("DEPENDENCY STARTING")
    schema = []
    iaa = []
    for dirpath, dirnames, files in os.walk(schema_dir):
        for file in files:
            # minimal check here; everything in the schema directory should be a schema csv
            if file.endswith('.csv'):
                file_path = os.path.join(dirpath, file)
                print("found schema " + file_path)
                schema.append(file_path)
    print("looking for IAA", iaa_dir)
    for dirpath, dirnames, files in os.walk(iaa_dir):
        for file in files:
            print("IAA OUTPUT",file)
            if file.endswith('.csv'):
                    file_path = os.path.join(dirpath, file)
                    print("evaluating dependencies for " + file_path)
                    iaa.append(file_path)

    temp = []
    print("IAA files found", iaa)
    for h in iaa:
        hdf = pd.read_csv(h, encoding = 'utf-8')

        if len(hdf.index) == 0:
            raise Exception("TOFIX: eval_dependency has S_IAA with length 0.")
        schem_sha = hdf['schema_sha256'].iloc[0]
        matched_schema = False
        for sch in schema:
            if schem_sha in sch:
                temp.append(sch)
                matched_schema = True
                break
        if not matched_schema:
            raise NameError("No schema matching file:", h)
    schema = temp
    print(schema)
    print(iaa)
    ins = []
    for i in range(len(iaa)):
        ins.append((schema[i], iaa[i], out_dir))
        handleDependencies(schema[i], iaa[i], out_dir)


def unpack_dependency_ins(input):
    return handleDependencies(input[0], input[1], input[2])

def handleDependencies(schemaPath, iaaPath, out_dir):
    print(out_dir)
    print("+++++++")
    schemData = pd.read_csv(schemaPath, encoding = 'utf-8')
    iaaData = pd.read_csv(iaaPath,encoding = 'utf-8')
    #we don't know if it'll get read in as int or str, but forcing str resolves edge cases when failed IAA
    iaaData['agreed_Answer'] = iaaData['agreed_Answer'].apply(str)
    assert schemData['namespace'].iloc[0] == iaaData['namespace'].iloc[0], "schema IAA mismatch_"+schemData['namespace'].iloc[0]+"\\/"+iaaData['namespace'].iloc[0]
    dependencies = create_dependencies_dict(schemData)
    tasks = np.unique(iaaData['source_task_uuid'].tolist())
    iaaData['prereq_passed'] = iaaData['agreed_Answer']

    iaaData = iaaData.sort_values(['question_Number'])

    #filter out questions that should never of been asksed because no agreement on prerequisites
    for q in range(len(iaaData)):
        qnum = iaaData['question_Number'].iloc[q]
        ans = iaaData['agreed_Answer'].iloc[q]
        tsk = iaaData['source_task_uuid'].iloc[q]
        iaaData['prereq_passed'].iloc[q] = checkPassed(qnum, dependencies, iaaData, tsk, ans)
    iaaData = iaaData.sort_values(["article_sha256",'prereq_passed','question_Number'])

    iaaData = iaaData[iaaData['prereq_passed'] == True]




    for t in tasks:
        iaaTask = iaaData[iaaData['source_task_uuid'] == t]
        #childQuestions
        #TODO: speed this up by only checking the
        for ch in dependencies.keys():

            child = dependencies[ch]
            needsLove = checkNeedsLove(iaaTask, ch)
            if needsLove:
                indices = np.zeros(0)
                #check if this question even got a score
                iaaQ = iaaTask[(iaaTask['question_Number']) == (ch)]
                answers = iaaQ['agreed_Answer'].tolist()
                answers = find_real_answers(answers)
                rows = find_index(iaaQ, answers, 'agreed_Answer')
                #refersh out her eso children can pull highlights from multiple parentes, if they exist
                validParent = False
                newInds = []

                if len(answers)>0:
                    #questions the child depends on
                    for par in child.keys():
                        iaaPar = iaaTask[iaaTask['question_Number'] == (par)]
                        neededAnswers = child[par]
                        #Potential for multiple answers from parent to lead to same child question
                        #We don't want to favor one prerequisite's highlight over another
                        for ans in neededAnswers:

                            for i in range(len(iaaPar)):
                                if iaaPar['agreed_Answer'].iloc[i].isdigit():
                                    if int(iaaPar['agreed_Answer'].iloc[i]) == ans:
                                        validParent = True
                                        inds_str = iaaPar['highlighted_indices'].iloc[i]
                                        inds = get_indices_hard(inds_str)
                                        newInds.append(inds)


                            if validParent:
                                for i in range(len(newInds)):
                                    indices = np.append(indices, newInds[i])

                #If parent didn't pass, this question should not of been asked
                #This should be handled by the previous step; the below if statemnt is an artifact of older version
                #could be useful for debugging if we make changes
                if not validParent:
                    for row in rows:
                        iaaData.at[row,'agreed_Answer'] = -1
                        iaaData.at[row, 'coding_perc_agreement'] = -1
                indices = np.unique(indices).tolist()
                for row in rows:
                    row_indices = get_indices_hard(iaaData.at[row, 'highlighted_indices'])
                    indices = merge_indices(row_indices, indices).tolist()
                    iaaData.at[row, 'highlighted_indices'] = json.dumps(indices)

    print('exporting to csv')
    path, name = get_path(iaaPath)
    outputpath  = os.path.join(out_dir, 'Dep_'+name)
    print("outputting dependency to", outputpath)
    iaaData.to_csv(outputpath,  encoding = 'utf-8', index = False)

    print("Table complete")
    return out_dir



def checkNeedsLove(df, qNum):
    #Checks if the question's parent prompts users for a highlight
    #true if it does
    qdf = df[df['question_Number'] == qNum]
    hls = (qdf['highlighted_indices'])
    #If no rows correspond to the child question
    if qdf.empty:
        return False
    for h in hls:
        if len(json.dumps(h))>3:
            return True
    return False

def checkPassed(qnum, dependencies, iaadata, task, answer):
    """
    checks if the question passed and if a prerequisite question passed
    """
    iaatask = iaadata[iaadata['source_task_uuid'] == task]
    qdata = iaatask[iaatask['question_Number'] == qnum]
    if not checkIsVal(answer):
        return False
    if not checkIsNum(qnum) or pd.isna(qnum):
        return False
    if qnum in dependencies.keys():
        #this loop only triggered if child question depends on a prereq
        for parent in dependencies[qnum].keys():
            #Can't ILOC because checklist questions have many answers
            pardata = iaatask[iaatask['question_Number'] == parent]
            parAns = pardata['agreed_Answer'].tolist()
            valid_answers = dependencies[qnum][parent]
            for v in valid_answers:
                #cast to string because all answers(even numeric) were forced to be strings
                strv = str(v)
                #Won't be found if it doesn't pass
                if strv in parAns:
                    par_ans_data = pardata[pardata['agreed_Answer'] == strv]
                    #print(len(par_ans_data['prereq_passed']), 'ppassed', par_ans_data['prereq_passed'])
                    #In case the parent's prereq didn't pass
                    if par_ans_data['prereq_passed'].iloc[0] == True:
                       return True
            return False
    return True


def checkIsVal(value):
    #returns true if value is a possible output from IAA that indicates the child q had user highlights
    if value == "M" or value == "L":
        return True
    #if its NAN
    if pd.isna(value):
        return False
    try:
        j = float(value) + 1
        ans = math.isnan(j)
        if ans:
            return False
        return True
    except:
        pass
    return False

def checkIsNum(value):
    #if its NAN
    if pd.isna(value):
        return False
    try:
        j = float(value) + 1
        ans = math.isnan(j)
        if ans:
            return False
        return True
    except:
        pass
    return False
def find_real_answers(answers):
    out = []
    for a in answers:
        if isinstance(a, int) or a.isdigit():
            out.append(int(a))
    return out


def find_index(df, targetVals,col):
    indices = []
    for v in targetVals:
        for i in range(len(df[col])):
            if type(df[col].iloc[i])==str:
                try:
                    df[col].iloc[i] = int(df[col].iloc[i])
                except ValueError:
                    #print("VALUE ERROR CAUGHT: invalid row")
                    continue


        shrunk = df[df[col] == v]
        if len(shrunk)>0:
            inds = []
            for i in shrunk.index:
                inds.append(i)
            for i in inds:
                indices.append(i)
    return indices

