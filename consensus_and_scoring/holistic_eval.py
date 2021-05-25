import pandas as pd
import os


def eval_triage_scoring(tua, pointsdf, scoring_dir, threshold_func='logis_0', reporting = False):
    '''
    Calculates point additions/deductions based off of the TUAs used to generate tasks
    In the future I'll set up a csv to tune this.  For now I can't think of a good way to do that so I'll try to make
    the code super clear
    :param tua: dataframe of both tuas
    :param pointsdf: dataframe of sortedpts
    :return:
    '''
    overallChange = pd.DataFrame(
        columns=("Schema", 'agreement_adjusted_points', 'Label', 'article_num', 'article_sha256','schema_sha256'))
    quoted_sources = get_dep_iaa(scoring_dir, schema="source")
    holistic = get_dep_iaa(scoring_dir, schema="holistic")
    for art_sha256 in tua['article_sha256'].unique():
        if holistic is not None:
            art_holistic = holistic[holistic['article_sha256'] == art_sha256]
        else:
            art_holistic  =None
        art_tua = tua[tua['article_sha256'] == art_sha256]
        art_num = art_tua['article_number'].iloc[0]
        art_id = threshold_func + art_sha256
        art_length = art_tua['article_text_length'].iloc[0]
        if quoted_sources is not None:
            art_sources = quoted_sources[quoted_sources['article_sha256'] == art_sha256]
        else:
            art_sources = None
        if art_length < 800:
            continue
        # This is a constant, relatively arbitrary, try different values
        base_article_len = 2800
        article_size_index = art_length / base_article_len
        num_assertions = count_cases(art_tua, 'Assertions')
        num_args = count_cases(art_tua, 'Arguments')
        num_sources = count_cases(art_tua, 'Quoted Sources')
        num_evidence = count_cases(art_tua, 'Evidence')
        # Handle Quoted Sources Schema here:
        num_vague_quals = 0
        num_vague_sources = 0
        if art_sources is not None:
            for task in art_sources['source_task_uuid'].unique():
                task_df = art_sources[art_sources['source_task_uuid'] == task]
                task_df['question_Number'] = task_df['question_Number'].apply(int)
                # handle Q2 (qualifications)
                # Can't be done on weightin.py because has to count number of occurrences of the same error.
                q2_df = task_df[task_df['question_Number'] == 2]
                if len(q2_df) > 0:
                    ans = str(q2_df['agreed_Answer'].iloc[0])
                    # q2.a5
                    if ans == '5':
                        num_vague_quals += 1
                    elif ans == '6':
                        num_vague_quals += 1
                    # q2.a7

                # Handle q5 (identification)

                # >> Scoring note: If ONLY 1.05.07, 1.05.08, or 1.05.09 (i.e not any of the others) then -2 points for each
                # source ... and if there are 2 or more such vague sources in a short article (or 3 in a long article), the
                # article should be dinged -5pts and tagged as 'vague sourcing'
                q5_df = task_df[task_df['question_Number'] == 5]
                q5_df = q5_df.loc[q5_df.agreed_Answer != 'U']
                q5_df = q5_df.loc[q5_df.agreed_Answer != 'M']
                q5_df = q5_df.loc[q5_df.agreed_Answer != 'L']
                if len(q5_df) > 0:
                    ans = q5_df['agreed_Answer'].apply(int).tolist()
                    if min(ans) > 6:
                        tua_uuid = q5_df['tua_uuid'].iloc[0]
                        indices = get_indices_by_uuid(tua, tua_uuid)
                        num_vague_sources += 1
                        overallChange = addPoints(overallChange, -2, 'Vague Sourcing', art_num, art_sha256, art_id,
                                                  indices=str(indices), schema='Sourcing')
        vagueness_index = (num_vague_sources + num_vague_quals) / article_size_index
        print(num_vague_quals, num_vague_sources, article_size_index, vagueness_index)
        if vagueness_index > 4:
            overallChange = addPoints(overallChange, -10, 'Vague Sourcing', art_num, art_sha256, art_id, schema='Sourcing')



        if (num_assertions - num_args - num_sources - num_evidence) > -1:
            # 5aff36a3-f8c5-4e24-b28f-6b1bc7527694 T1.Q1.A1 News report
            if checkArtType(1,1, art_holistic):
                overallChange = addPoints(overallChange, -5, 'Low Information', art_num, art_sha256, art_id, schema='Holistic')
            else:
                overallChange = addPoints(overallChange, -2, 'Low Information', art_num, art_sha256, art_id, schema='Holistic')
        # a2f97bce-2512-43e0-9605-0d137d30d8e6 T1.Q1.A3 Op-Ed
        if not checkArtType(1,3, art_holistic):
            indexVal = (1 + num_assertions) / (1 + num_evidence + num_evidence)
            if indexVal > 1:
                overallChange = addPoints(overallChange, -2, 'Low Information', art_num, art_sha256, art_id, schema='Holistic')

        if not (
                # 251e628c-2cd1-467a-9204-6f1b7c80cf79: T1.Q1.A6 Interview;
                # a2f97bce-2512-43e0-9605-0d137d30d8e6 T1.Q1.A3 Op-Ed
                # 0f15553b-95da-4eec-84f7-6809f5205ff2: T1.Q6.A5 Scientific study or discovery;
                # ad87bdb1-2247-4660-b0fd-64b19aa050fb T1.Q10.2 Report of what some person, body, or group said
            checkArtType(1,6, art_holistic) or checkArtType(1,3, art_holistic)  or
            checkArtType(6,5, art_holistic) or checkArtType(10,2, art_holistic)):  # T1.Q6.A5 and T1.Q10.2
            if (num_sources < 2 and num_evidence < num_assertions + num_args):
                overallChange = addPoints(overallChange, -2, 'Low Information', art_num, art_sha256, art_id, schema='Holistic')
    print("POINTS_DF \n ", pointsdf.shape, '\n',pointsdf)
    print("OVERALL \n", overallChange.shape, '\n', overallChange)

    pointsdf = pd.concat([pointsdf, overallChange], axis=0, ignore_index=True)
    if reporting:
        pointsdf.to_csv(scoring_dir + '/AssessedPoints.csv')
    return pointsdf

def checkArtType(question_number, answer_number, holistic_df):
    if holistic_df is None:
        return False
    ques_df = holistic_df[holistic_df['question_Number'] == question_number]
    ans_df = ques_df[ques_df['agreed_Answer'] == answer_number]
    if len(ans_df):
        return True
    return False


def get_indices_by_uuid(tua, tua_uuid):
    df = tua[tua['tua_uuid'] == tua_uuid]
    indices = []
    for i in range(len(df)):
        start = df['start_pos'].iloc[i]
        end = df['end_pos'].iloc[i]
        for n in range(start, end):
            indices.append(n)
    return indices


def get_dep_iaa(directory, schema='sources'):
    """
    :param directory: scoring directory, holds dep_iaa files
    :param schema: 'sources' or 'holistic' or ...
    :return: dataframe of the dep_s_ia fo the schema
    """
    if schema == 'sources' or schema == 'source':
        search_term = "ource"
    elif schema == 'holistic' or schema == 'overall':
        search_term = "olis"
    else:
        print("AAAHHHHH, can't evaluate get_dep_iaa in holistic_eval.py; directory:", directory, "schema:", schema)
    df_list  = []
    for root, dir, files in os.walk(directory):
        for file in files:
            print(file)
            if file.endswith('.csv'):
                if 'Dep_' in file and search_term in file:
                    df = pd.read_csv(directory + '/' + file)
                    if df.shape[0] >0 and search_term in df['namespace'].iloc[0]:
                        df_list.append(df)
                    else:
                        df = None
    if len(df_list) == 0:
        print("HOLISTIC EVAL, No specialist agreement for :", schema, "task")
        return None
    df = pd.concat(df_list)
    return df


def points_by_distance(value, target, scale, min=0, max=20):
    """
    finds points based on distance from target value.  Assumes value is in the wrong direction; ie if we want target of
    at least 4, value is 3.5; or if we want target of at most 5, value is 4.2
    can't go higher than the max
    Commonly, scale gets steeper for more extreme infractions
    """
    points = abs(target - value) * scale
    if points > max:
        return max
    return max(points, min) * -1


def getIndices(tua, topic):
    topic_tua = tua[tua['topic_name'] == topic]
    indices = []
    for i in range(len(topic_tua)):
        start = topic_tua['start_pos'].iloc[i]
        end = topic_tua['end_pos'].iloc[i]
        for n in range(start, end):
            indices.append(n)
    return indices


def count_cases(tua, topic):
    topical_tua = tua[tua['topic_name'] == topic]
    if len(topical_tua) < 1:
        return 0
    # in current implementation highest case number is the # of cases; always starts at 1 counts up
    case_nums = topical_tua['case_number']
    return len(case_nums.unique())


def addPoints(df, points, label, art_num, art_sha, art_id, indices='[]', schema = 'Holistic'):
    if schema == 'Holistic':
        df = df.append({'Schema': 'Holistic', 'schema_sha256': 'Holistic', 'points': points, 'Label': label, 'article_num': art_num,
                    'article_sha256': art_sha, 'article_id': art_id, 'highlighted_indices': indices}, ignore_index=True)
    elif schema == 'Sourcing':
        df = df.append({'Schema': 'Sourcing', 'schema_sha256': 'Sourcing', 'points': points, 'Label': label,
                        'article_num': art_num,
                        'article_sha256': art_sha, 'article_id': art_id, 'highlighted_indices': indices},
                       ignore_index=True)
    else:
        raise Exception("Invalid Schema for holistic evaluation {}".format(schema))
    return df

if __name__ == '__main__':
    tua = pd.read_csv('../test_data/he_tua_low_info_true/TUA_tua_task_id-Task.csv')
    pointsdf = pd.read_csv('../test_data/he_scoring_low_info_true/SortedPts.csv')
    scoring_dir = '../test_data/he_scoring_low_info_true'
    eval_triage_scoring(tua, pointsdf, scoring_dir, threshold_func='logis_0', reporting=False)