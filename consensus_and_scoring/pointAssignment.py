import numpy as np
import pandas as pd
import os
from dataV3 import make_directory
from dataV3 import get_indices_hard
import json
import math

def pointSort(scoring_directory, input_dir = None, weights = None,
              scale_guide_dir = "./config/point_assignment_scaling_guide.csv", reporting = False, rep_direc = False,
              tua_dir = None):

    print('tua_dir', tua_dir)
    if input_dir != None:
        dir_path = os.path.dirname(os.path.realpath(input_dir))
        input_path = os.path.join(dir_path, input_dir)
    if not tua_dir:
        tua_path = os.path.join(input_path, 'tua')
        tua_location = ''
        for file in os.walk(input_dir):
            if 'tua' in file and os.path.join(input_dir, file).isdir():
                tua_path = os.path.join(input_dir, file)
                print("FOUND TUA", tua_path)
                break
        for file in os.listdir(input_dir+'/tua'):
            print('file in tua',file)
            tua_location = os.path.join(tua_path, file)
            try:
                tuas = tuas.append(pd.read_csv(tua_location))
            except UnboundLocalError:
                tuas = pd.read_csv(tua_location)
    else:
        for file in os.listdir(tua_dir):
            tua_location = os.path.join(tua_dir, file)
            try:
                tuas = tuas.append(pd.read_csv(tua_location))
            except UnboundLocalError:
                tuas = pd.read_csv(tua_location)
    #Load everything so that it's a pandas dataframe

    tuas_raw = tuas
    dirname = os.path.dirname(__file__)
    # can't use os.path.join, probably because windows uses \ as a  separator instead of /
    scale_guide_path = dirname + os.sep + 'config' + os.sep + 'point_assignment_scaling_guide.csv'
    scale_guide = pd.read_csv(scale_guide_path)
    if len(tua_location)<3:
        raise FileNotFoundError("TUA file not found")
    files = getFiles(scoring_directory)

    if not rep_direc and reporting:
        rep_direc = scoring_directory + "_report"
    # marker booleans that will make corner cases nicer down the road
    hasSource = False
    hasArg = False
    source_file = files[0]
    arg_file = files[1]
    argRel = None
    sourceRel = None
    if source_file:
        hasSource = True
        sourceRel = pd.read_csv(files[0])
        slen = len(sourceRel)
        sourceRel = sourceRel.dropna(subset=['tua_uuid'])
        if len(sourceRel)< slen:
            print("Warning, nan sourcerel tua_uuids")
    if arg_file:
        hasArg = True
        argRel = pd.read_csv(files[1])
        alen = len(argRel)
        argRel = argRel.dropna(subset=['tua_uuid'])
        if len(argRel)< alen:
            print("Warning, nan argrel tua_uuids")

    if weights is None:
        weightFiles = files[2]
        weight_list = []

        for i in range(len(weightFiles)):
            wf = pd.read_csv(weightFiles[i])
            weight_list.append(wf)

        weights = pd.concat(weight_list)
    print("WEIGHTS_CONCAT")
    print(weights)
    print("COLUMNS")
    for col in weights.columns:
        print(col)
    weights['agreement_adjusted_points'] = weights['agreement_adjusted_points'].apply(float)

    weights = weights[weights['agreement_adjusted_points'] != 0]
    weights = weights[~weights['agreement_adjusted_points'].isnull()]
    if weights['Label'].isnull().any():
        raise Exception("Couldn't find labels for a weight")

    if reporting:
        make_directory(rep_direc)
        weights.to_csv(rep_direc+'/weightsStacked'+'.csv')
    if hasArg or hasSource:
        tuas = collapse_all_tuas(tuas, hasArg, argRel, hasSource, sourceRel, reporting)
        if reporting:
            tuas.to_csv(rep_direc+'/collapsed_All_TUAS'+'.csv')
        tuas = enhance_all_tuas(tuas, scale_guide, hasArg, argRel, hasSource, sourceRel)
        if reporting:
            tuas.to_csv(rep_direc+'/enhanced_All_TUAS'+'.csv')
        tuas, weights = find_tua_match(tuas, weights)
        if reporting:
            tuas.to_csv(rep_direc+'/matched_All_TUAS'+'.csv')
            weights.to_csv(rep_direc + '/weightsMatched' +  '.csv')
        weights = apply_point_adjustments(weights, scale_guide)
        if reporting:
            weights.to_csv(rep_direc + '/weightsAdjusted' +  '.csv')
    else:
        weights['points'] = weights['agreement_adjusted_points']
    #BUG: Someehere in there we're getting duplicates of everything: the following line shouldprevent it from hapening but should
    #investigate the root
    weights = weights.drop_duplicates(subset=['source_task_uuid', 'Answer_Number', 'Question_Number'])
    if reporting:
        weights.to_csv(scoring_directory + '/SortedPts.csv')
    return tuas, weights, tuas_raw

def apply_point_adjustments(weights, scale_guide):
    """
    Scales the previously suggested weights based on the adjustments recommended by the point scaling guide
    throughout the function; abs_point = 0 meaning to do not replace the previsouly recommended weight with a new
    arbitrarily decided point value
    To do nothing, point_scale should be 1 because of ID property of multiplication
    """
    scale_guide = scale_guide.sort_values('priority')
    weights['points'] = weights['agreement_adjusted_points']
    for i in range(weights.shape[0]):
        w = weights.iloc[i]
        if w['arg_match_score'] > 0 or w['source_match_score'] > 0:
            abs_point, point_scale = checkForScale(w, scale_guide)
            if abs_point != 0:
                weights.iloc[i, weights.columns.get_loc("points")] = abs_point
            weights.iloc[i, weights.columns.get_loc("points")] = w['points']*point_scale
    return weights


def checkForScale(weight, scale_guide):
    """compares the row from weights to each row of the scaling guide; returns the suggested absolute point replacement
    or the value to scale the previously recommended weight by"""
    for i in range(scale_guide.shape[0]):
        scale = scale_guide.iloc[i]
        found, abs_point, point_scale = check_scale_match(weight, scale)
        if found:
            return abs_point, point_scale
    return 0,1


def check_scale_match(weight, scale):
    """compares a single row of the weights csv with a single row of the scale csv
    if it finds a match returns True, abs_adjustment, point_scale"""
    topicn = math.floor(scale['arg_topic'])
    topics = str(topicn)
    ## <3 unit conversions
    arg_col = "arg_T"+str(math.floor(scale['arg_topic']))+'.Q'+str(math.floor(scale['arg_question_num']))
    arg_ans = int(scale['arg_answer_num'])
    found = False
    abs_point = 0
    point_scale = 1
    # handle the case when no weight column for it because there is no arg tasks
    if arg_ans != -1 and arg_col not in weight.keys():
        return found, abs_point, point_scale
    if arg_ans == -1 or arg_ans == weight[arg_col]:
        src_col = "source_T"+str(math.floor(scale['source_topic']))+'.Q'+str(math.floor(scale['source_question_num']))
        src_ans = int(scale['source_answer_num'])
        #spec_ans = weight[src_col]
        #handle the case when no weight column for it because there is no source tasks
        if src_ans != -1 and src_col not in weight.keys():
            return found, abs_point, point_scale
        if src_ans == -1 or src_ans == int(weight[src_col]):
            found = True
            abs_point = scale['absolute_points']
            point_scale = scale['scaling_factor']
    return found, abs_point, point_scale


def getFiles(directory):
    #NEEDS: WeightingOutputs, sourceTriagerIAA, arg Source IAA
    sourceFile = None
    argRelevanceFile = None
    if directory[-1]!='/':
        directory = directory + '/'
    for root, dir, files in os.walk(directory):
        for file in files:
            if 'Dep' in file:
                if file.endswith('.csv')  and 'ource' in file:
                    sourceFile = directory+file
                if file.endswith('.csv')   and ('Arg' in file or 'arg' in file):
                    argRelevanceFile = directory+file
    return sourceFile, argRelevanceFile #, weightOutputs

def find_tua_match(all_tuas, weights, arg_threshold = .8, source_threshold = .6):
    """Compares the highlighted indices of specialist tasks to those generated by the arg/source triager
    If there's a match data from the allTuas corresponding ot the triager that generated the arg specialist task
    gets transferred to the correspoding row of the weights csv"""
    all_tuas = add_indices_column(all_tuas)
    weights['arg_match_score'] = np.zeros(weights.shape[0])
    weights['source_match_score'] = np.zeros(weights.shape[0])
    weights['arg_offsets'] = np.zeros(weights.shape[0])
    weights['arg_case_number'] = np.zeros(weights.shape[0])
    weights['src_offsets'] = np.zeros(weights.shape[0])
    weights['src_case_number'] = np.zeros(weights.shape[0])
    new_arg_cols = []
    new_source_cols = []
    for col in all_tuas.columns:
        if col not in weights.columns and 'arg' in col:
            new_arg_cols.append(col)
            weights[col] = np.zeros(weights.shape[0])
        if col not in weights.columns and 'source' in col:
            new_source_cols.append(col)
            weights[col] = np.zeros(weights.shape[0])
    for i in range(weights.shape[0]):
        arg_best_ind = -1
        arg_best_score = 0
        src_best_ind = -1
        src_best_score = 0
        w = weights.iloc[i]
        w_art = w['article_sha256']
        #make sure the comparisons we find are to the right article
        art_tuas = all_tuas[all_tuas['article_sha256'] == w_art]
        w_h = w['highlighted_indices']
        if isinstance(w_h, str):
            w_h = json.loads(w_h)
        if not (isinstance(w_h, float)) and (not (isinstance(w_h, str)) or len(w_h)>1):
            weight_unit = get_indices_hard(w['highlighted_indices'])
            if len(weight_unit) > 0:
                for t in range(art_tuas.shape[0]):
                    tua_unit = json.loads(art_tuas.iloc[t]['indices'])
                    matches = np.intersect1d(np.array(weight_unit), np.array(tua_unit))
                    num_match = len(matches)
                    score = max(num_match/len(weight_unit), num_match/len(tua_unit))
                    if 'gumen' in all_tuas['topic_name'].iloc[t]:
                        if score>arg_best_score:
                            arg_best_score = score
                            arg_best_ind = t
                    else:
                        if score > src_best_score:
                            src_best_score = score
                            src_best_ind = t
        #Now add the data from the best tua columns to the weights csv
        weights.iloc[i, weights.columns.get_loc('arg_match_score')] = arg_best_score
        weights.iloc[i, weights.columns.get_loc('source_match_score')] = src_best_score
        if arg_best_ind>=0 and arg_best_score>arg_threshold:
            arg_row = all_tuas.iloc[arg_best_ind]
            weights.iloc[i, weights.columns.get_loc('arg_offsets')] = arg_row['indices']
            weights.iloc[i, weights.columns.get_loc('arg_case_number')] = arg_row['case_number']
            for col in new_arg_cols:
                weights.iloc[i, weights.columns.get_loc(col)] = arg_row[col]
        if src_best_ind >=0 and src_best_score>source_threshold:
            src_row = all_tuas.iloc[src_best_ind]
            weights.iloc[i, weights.columns.get_loc('src_offsets')] = src_row['indices']
            weights.iloc[i, weights.columns.get_loc('src_case_number')] = src_row['case_number']
            weights = add_a_columns(src_row, weights, i, 'source')
    return all_tuas, weights
def add_a_columns(tua_row, weights, i, prefix):
    """Transfer question answers from matched TUAs to matched Weights"""

    ans_col = [col for col in tua_row.axes[0] if prefix+'_T' in col]
    for col in ans_col:
        weights.iloc[i, weights.columns.get_loc(col)] = tua_row[col]
    return weights



def add_indices_column(all_tuas):
    all_tuas['indices'] = np.zeros(all_tuas.shape[0])
    all_tuas['full_text'] = np.zeros(all_tuas.shape[0])
    for i in range(all_tuas.shape[0]):
        ind = []
        text = ""
        r = all_tuas.iloc[i]
        start = all_tuas['start_pos'].iloc[i]
        end = all_tuas['end_pos'].iloc[i]
        text += all_tuas['target_text'].iloc[i]+'//'

        for k in range(int(start), int(end+1)):
            ind.append(k)
        all_tuas.iloc[i,all_tuas.columns.get_loc('indices')] = json.dumps(ind)
        all_tuas.iloc[i, all_tuas.columns.get_loc('full_text')] = json.dumps(text)

    return all_tuas
def collapse_all_tuas(all_tuas, has_arg, arg_spec, has_source, source_spec, reporting = False):
    """
    collapses the alltuas dataframe to only include the rows that have a corresponding argument/source relevance task
    source_task_uuid is unique and is shared by the allTUAs csv and all the IAA csvs.
    """
    #get list of all the task_uuids we care about
    collapsed = None
    real_tasks = pd.Series()
    if has_arg:
        real_tasks = real_tasks.append(arg_spec['tua_uuid'])

    if has_source:
        real_tasks = real_tasks.append(source_spec['tua_uuid'])

    real_tasks = real_tasks.unique()
    if len(real_tasks)>0:
        collapsed = all_tuas[all_tuas['tua_uuid'] == real_tasks[0]]
        for i in range(1,len(real_tasks)):
            extra = all_tuas[all_tuas['tua_uuid'] == real_tasks[i]]
            collapsed = collapsed.append(all_tuas[all_tuas['tua_uuid'] == real_tasks[i]])

    return collapsed

def enhance_all_tuas(all_tuas, scaling_guide, has_arg, arg_spec, has_source, source_spec):
    """ add columns for every T?.Q? that's relevant.  Put the specialist answers in the correct plces corresponding
    to the column header"""
    if has_arg:
        pref = "arg_"
        all_tuas = add_q_columns(all_tuas, scaling_guide, pref)
        all_tuas = input_specialist_answers(all_tuas, arg_spec, pref)
    if has_source:
        pref = "source_"
        all_tuas = add_q_columns(all_tuas, scaling_guide, pref)
        all_tuas = input_specialist_answers(all_tuas, source_spec, pref)

    return all_tuas

def input_specialist_answers(all_tuas, spec, prefix):
    """fill in the added T?.Q? columns with the data from the dep_s_iaa"""
    for i in range(all_tuas.shape[0]):
        id = all_tuas.iloc[i]['tua_uuid']
        spec_crop = spec[spec['tua_uuid'] == id]
        if len(spec_crop )>0:
            schema = spec_crop['namespace'].iloc[0]
            #this check shouldn't be necessary but just in case
            if ('gumen' in schema.lower() and 'arg' in prefix) or ('ource' in schema.lower() and 'source' in prefix):
                for s in range(spec_crop.shape[0]):
                    qNum = spec_crop['question_Number'].iloc[s]
                    aNum = spec_crop['agreed_Answer'].iloc[s]
                    tq = convert_to_tq_format(1, qNum)
                    col_name = prefix+tq
                    #check the column exists; many times it won't; especially for the 'how did this go' questions at the
                    #the end of each task
                    if col_name in all_tuas.columns:
                        all_tuas.iloc[i, all_tuas.columns.get_loc(col_name)] = aNum

    return all_tuas


def add_q_columns(all_tuas, scaling_guide, prefix):
    """Addd columns to csv to correspond to every question that possibly could affect the way points are scaled"""
    topics = scaling_guide[prefix+"topic"]
    max_topic = topics.max()
    questions = scaling_guide[prefix + "question_num"]
    max_question = questions.max()
    length = all_tuas.shape[0]
    for t in range(1,max_topic+1):
        for q in range(1,max_question+1):
            name = convert_to_tq_format(t,q)
            all_tuas[prefix+name] = np.zeros(length)
    return all_tuas

def convert_to_tq_format(topic, question):
    """creates a string that is uniform in the "T?.Q?.A? format; without the A"""
    return "T"+str(topic)+".Q"+str(question)

#pointSort('../data/out_scoring/', input_dir = None, weights = weights, tua_dir=tua_dir, reporting=reporting
#pointSort('scoring_covid', 'covid/')
