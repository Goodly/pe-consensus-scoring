import numpy as np
import pandas as pd
import os


def launch_Weighting(directory, out_directory = None, reporting = False):
    print("WEIGHTING STARTING")
    if out_directory == None:
        out_directory=directory
    iaaFiles = []
    for root, dir, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv') and 'Dep' in file:
                iaaFiles.append(directory+'/'+file)
    print("IAA files found:", iaaFiles)
    weight_list = []
    #get holistic so different weight keys can be used for different types of articles
    weight_col = 'Point_Recommendation'
    for f in iaaFiles:
        if 'olistic' in f:
            holistic = pd.read_csv(f, encoding='utf-8')
            q1 = holistic[holistic['question_Number'] == 1]
            if len(q1) > 0 and int(q1.iloc[0]['agreed_Answer']) == 3:
                weight_col = 'Op-Ed'
            break
    dirname = os.path.dirname(__file__)
    # can't use os.path.join, probably because windows uses \ as a  separator instead of /
    weight_key_path = dirname + os.sep + 'config' + os.sep + 'weight_key.csv'
    weight_scaling_path = dirname + os.sep + 'config' + os.sep + 'weight_key_scaling_guide.csv'
    for f in iaaFiles:

        weight = weighting_alg(f, weight_key_path, weight_scaling_path, out_directory,reporting=reporting,
                               weight_col = weight_col)
        if weight is not None and not weight.empty:
            weight_list.append(weight)
    if len(weight_list) == 0:
        print("No weights")
        file = pd.read_csv(iaaFiles[0], encoding = 'utf-8')
        columns =  file.columns.tolist()
        weight_key_cols = pd.read_csv(weight_key_path, encoding= 'utf-8').columns.tolist()
        columns = columns + weight_key_cols + ['agreement_adjusted_points', 'Schema']
        weights = pd.DataFrame(columns = columns)
        weights = weights.loc[:, ~weights.columns.duplicated()]

    else:
        weights = pd.concat(weight_list)
    return weights

def weighting_alg(IAA_csv_file, credibility_weights_csv_file, weight_scale_csv, directory = './', reporting = False,
                  weight_col = 'Point_Recommendation'):

    IAA_csv = pd.read_csv(IAA_csv_file)
    #IndexError when the csv is empty
    try:
        IAA_csv_schema_name = IAA_csv.namespace.iloc[0]
    except IndexError:
        if IAA_csv.shape[0]<1:
            return
        else:
            print(len(IAA_csv))
            print(IAA_csv)
            raise Exception('Index Error')

    if "uage" in IAA_csv_schema_name:
        IAA_csv_schema_type = "Language"
    elif "Reason" in IAA_csv_schema_name:
        IAA_csv_schema_type = "Reasoning"
    elif "Evidence" in IAA_csv_schema_name:
        IAA_csv_schema_type = "Evidence"
    elif "Probability" in IAA_csv_schema_name:
        IAA_csv_schema_type = "Probability"
    elif 'olistic' in IAA_csv_schema_name:
        IAA_csv_schema_type = "Holistic"
    elif 'ource' in IAA_csv_schema_name:
        IAA_csv_schema_type = "Sourcing"
    else:
        print("unweighted IAA", IAA_csv_file, "aborting")
        return

    IAA_csv = IAA_csv.rename(columns={ "question_Number": "Question_Number", 'agreed_Answer': 'Answer_Number'})
    IAA_csv['Schema'] = IAA_csv_schema_type
    credibility_weights_csv = pd.read_csv(credibility_weights_csv_file)
    if IAA_csv_schema_name not in credibility_weights_csv.values:
        raise Exception("Couldn't find weights for schema namespace {}".format(IAA_csv_schema_name))
    weight_scale_table = pd.read_csv(weight_scale_csv)

    IAA_csv["Question_Number"] = IAA_csv["Question_Number"].apply(int)

    IAA_csv['Answer_Number'] \
        = IAA_csv['Answer_Number'].apply(convertToInt)
    IAA_csv = IAA_csv.loc[IAA_csv.Answer_Number != -1]
    for_visualization = pd.DataFrame()
    #uncomment when we want to scale question scores based on answers to other questions
    for task in np.unique(IAA_csv['source_task_uuid']):
        task_IAA = IAA_csv[IAA_csv['source_task_uuid'] == task]
        scaled_cred_weights = scale_weights_csv(credibility_weights_csv, weight_scale_table, task_IAA,
                                                    IAA_csv_schema_type)

    new_csv = pd.merge(scaled_cred_weights, IAA_csv, on =["namespace", "Question_Number", 'Answer_Number'])
    points = new_csv[weight_col] * new_csv["agreement_score"]
    new_csv = new_csv.assign(agreement_adjusted_points = points)
    for_visualization = for_visualization.append(new_csv)
    if reporting:
        out_file = directory+"/Point_recs_"+IAA_csv_schema_name+".csv"
        print(out_file)
        for_visualization.to_csv(out_file, encoding = 'utf-8', index = False)
    return for_visualization

def weighted_q6(num):
    if num >= 160:
        score = 0
    elif 150 <= num < 160:
        score = 0.5
    elif 100 <= num <150:
        score = 2
    elif 50 <= num <100:
        score = 3
    elif num < 50:
        score = 4
    else:
        score = 5
    return score

def scale_weights_csv(weight_df, scale_df, iaa_df, schema):
    '''

    :param weight_df: weight_key
    :param scale_df: weight_scale_key
    :return: scaled weights dataframe
    '''
    if schema not in scale_df['if_schema']:
        return weight_df
    weight_df = weight_df[weight_df['Schema'] == schema]
    scale_df = scale_df[scale_df['if_schema'==schema]]
    scaled = weight_df
    for a in scale_df['if_ans_uuid']:
        #Gotta make this not be uuid, not stable enough, for now its fine cuase this isn't used
        if a in iaa_df['answer_uuid']:
            row = iaa_df[iaa_df['answer_uuid' == a]]
            #guaranteed to only happen once
            q = int(row['question_Number'].iloc[0])
            a = convertToInt(row['agreed_Answer'])
            mulrow = scale_df[scale_df['if_ans_uuid'] == a]
            mul = mulrow['mult'].iloc[0]
            scaled.loc[['Question_Number' == q, 'Answer_Number' == a, ['Point_Recommendation']]] = \
                scaled.loc[['Question_Number' == q, 'Answer_Number' == a, ['Point_Recommendation']]]*mul
    return scaled

def convertToInt(string):
    try:
        out = int(string)
        return out
    except:
        return -1

if __name__ == '__main__':
        launch_Weighting('../test_data/out_mn_scoring')
