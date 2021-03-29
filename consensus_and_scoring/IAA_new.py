import csv
import pandas as pd
from ChecklistCoding import *
from ExtraInfo import *
import json
from dataV3 import *
from repScores import *
import os
import re

def calc_agreement_directory(directory, schema_dir, config_path, texts_path, repCSV=None, outDirectory = None, useRep = False, threshold_func = 'raw_30'):
    """
    Calculates inter-annotator agreement in datahunts and transforms into properly formatted files.

    Args:
        directory: path to directory with datahunts
        schema_dir: path to directory with schema files
        config_path: path to directory with config files
        texts_path: path to directory with text files
        repCSV: path to information for user monitoring (optional)
        outDirectory: path to directory for outputs
        useRep: boolean for using reputation scores
        threshold_func: function for determining if a question passes IAA

    Returns:
        outDirectory: path to directory containing all output IAA csv files
    """
    #Create a list of datahunt paths and used schemas
    datahunts = []
    used_schemas = set()
    for root, dir, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv') and 'IAA' not in file:
                hdf = pd.read_csv(directory+'/'+file, encoding = 'utf-8')
                if len(hdf.index) > 0: #Do not add the datahunt if it is empty
                    datahunts.append(directory+'/'+file)
                    schem_sha = hdf['schema_sha256'].iloc[0] #The schema associated with the datahunt
                    used_schemas.add(schem_sha)

    #Create a list of schema paths
    schemas = []
    for root, dir, files in os.walk(schema_dir):
        for file in files:
            file_name = file.split('.')[0] #Remove file extension
            if file_name in used_schemas: #Only add schema paths for used schemas
                schemas.append(schema_dir+'/'+file)

    #Ensure that all used schemas were found
    if len(datahunts) != len(schemas):
        raise NameError("Missing schema for a file")

    #For each datahunt, create an output file
    for i in range(len(datahunts)):
        calc_scores(datahunts[i], config_path, texts_path, repCSV = repCSV, schemaFile=schemas[i], outDirectory=outDirectory, useRep=useRep, directory=directory, threshold_func = threshold_func)
    return outDirectory

def calc_scores(highlightfilename, config_path, texts_path, repCSV=None, schemaFile = None, outDirectory = None, useRep = False, directory = None, threshold_func = 'logis_0'):
    """
    Calculates inter-annotator agreement for a single datahunt and creates an output IAA file.

    Args:
        highlightfilename: path to datahunt file
        config_path: path to directory with config files
        texts_path: path to directory with text files
        repCSV: path to information for user monitoring (optional)
        schemaFile: path to schema file
        outDirectory: path to directory for outputs
        useRep: boolean for using reputation scores
        directory: path to directory with datahunts
        threshold_func: function for determining if a question passes IAA

    Returns:
        outDirectory: path to directory containing the output IAA csv file
    """
    #Create structures for user monitoring
    uberDict = dataStorer(highlightfilename, schemaFile)
    repDF = None
    if useRep: repDF = create_user_reps(uberDict, repCSV)

    #Read in the datahunt file
    datahunt = pd.read_csv(highlightfilename, encoding = 'utf-8')

    #Create the output dataframe and its column names (appending to list is faster than to Pandas dataframe)
    data = [["article_num", "article_sha256", "article_id", "article_filename", "source_task_uuid", "tua_uuid", "namespace", "schema_sha256", "question_Number", "answer_uuid", "question_type", "agreed_Answer", "coding_perc_agreement", "highlighted_indices", "agreement_score", "num_users", "num_answer_choices", "target_text", "question_text", "answer_text", "article_text_length"]]

    #Iterate through each article in the datahunt
    tasks = datahunt['quiz_task_uuid'].unique()
    for task in tasks:
        datahunt_task = datahunt[datahunt['quiz_task_uuid'] == task]
        questions = datahunt_task['question_label'].apply(getQuestionNumberFromLabel).unique()
        article_num = datahunt_task['article_number'].iloc[0]
        article_sha =  datahunt_task['article_sha256'].iloc[0]
        article_id = threshold_func + article_sha
        article_filename = datahunt_task['article_filename'].iloc[0]
        source_task_uuid = task
        tua_uuid = datahunt_task['tua_uuid'].iloc[0]
        namespace = datahunt_task['namespace'].iloc[0]
        schema_sha = datahunt_task['schema_sha256'].iloc[0]
        text_file = os.path.join(texts_path, article_sha + ".txt")

        if not(os.path.exists(text_file)):
            raise Exception("Couldn't find text_file for article {}".format(text_file))

        # Iterate through each question in an article and score
        for ques_num in sorted(questions):
            datahunt_question = datahunt_task[datahunt_task['question_label'] == "T1.Q%d"%ques_num]
            question_text = datahunt_question['question_text'].iloc[0]
            agreements = score(task, ques_num, datahunt_question, uberDict, config_path, text_file, schemaFile, repDF, useRep=useRep, threshold_func=threshold_func)

            #Iterate through agreements and add to output data
            if type(agreements) is not list: #agreements will only be a list for checkput questions
                agreements = [agreements]
            for i in range(len(agreements)):
                agreed_answer = agreements[i][0]
                highlighted_indices = agreements[i][1]
                codingPercentAgreement = agreements[i][4]
                num_users = agreements[i][5]
                target_text = agreements[i][6]
                firstSecondScoreDiff = agreements[i][7]
                question_type = agreements[i][8]
                num_choices = agreements[i][9]
                length = agreements[i][10]

                ans_uuid, has_hl = get_answer_data(1, ques_num, agreed_answer, schemaFile)
                if int(has_hl) == 0:
                    highlighted_indices = []
                answer_text = get_answer_content(uberDict, task, ques_num, agreed_answer)

                totalScore = codingPercentAgreement #For now total score is just agreement score
                highlighted_indices = json.dumps(np.array(highlighted_indices).tolist())

                data.append([article_num, article_sha, article_id, article_filename, source_task_uuid, tua_uuid, namespace, schema_sha, ques_num, ans_uuid, question_type, agreed_answer, codingPercentAgreement, highlighted_indices, totalScore, num_users, num_choices, target_text, question_text, answer_text, length])

    #Create structures for output data
    outDirectory = make_directory(outDirectory)
    path, name = get_path(highlightfilename)
    task_name = re.match(r'(.*?)-Task', name).group()[:-5]
    out_name = task_name + '.IAA-' + source_task_uuid + '-Tags.csv'
    print("IAA outputs to:", outDirectory + out_name)

    #Write the output IAA file
    with open(os.path.join(outDirectory, out_name), 'w', encoding='utf-8') as scores:
        writer = csv.writer(scores)
        writer.writerows(data)

    if useRep:
        user_rep_task(uberDict, outDirectory + 'S_IAA_' + name, repDF)
        print("user_rep_df updated and saved as UserRepScores.csv")

    return outDirectory

def score(article, ques, quesDF, data, config_path, text_file, schemaFile, repDF = None, useRep = False, threshold_func = 'logis_0'):
    """
    Calculates the relevant scores of the specific question of an article.

    Args:
        article: uuid of the article
        ques: question number
        quesDF: dataframe for question of article
        config_path: path to directory with config files
        text_file: path to text file
        schemaFile: path to schema file
        repDF: dataframe for reputation scores
        useRep: boolean for using reputation scores
        threshold_func: function for determining if a question passes IAA

    Returns:
        out: a mess of variables
    """

    starts = get_question_start(data,article, ques)
    print(starts)
    ends = get_question_end(data, article, ques)
    length = get_text_length(data, article, ques)
    if len(ends)>0 and max(ends)>0:
        hlUsers = get_question_hlUsers(data, article,ques)
        hlAns = get_question_hlAns(data, article, ques)

        #Now load the source_text
        if text_file == None:
            raise Exception("Couldn't find text_file for article", article)

        with open(text_file, 'r', encoding='utf-8') as file:
            sourceText = file.read()
    else:
        sourceText = []
        hlUsers = []
        hlAns = []

    schema = get_schema_topic(data, article)
    if schema_has_dist_function(schemaFile):
        question_type, num_choices = schema_to_type_and_num(ques, schemaFile, config_path)
    else:
        question_type, num_choices = get_type_json(schema, ques, config_path)

    answers = get_question_answers(data, article, ques)
    users =get_question_userid(data, article, ques)
    numUsers = get_num_users(data, article, ques)

    assert (len(answers) == len(users))
    if num_choices == 1:
        return 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    if question_type == 'ordinal':
        out = evaluateCoding(answers, users, starts, ends, numUsers, length, sourceText, hlUsers, hlAns, repDF = repDF, dfunc='ordinal', num_choices=num_choices, useRep=useRep, threshold_func=threshold_func)
        out = out+(question_type, num_choices, length)
    elif question_type == 'nominal':
        out = evaluateCoding(answers, users, starts, ends, numUsers, length, sourceText,hlUsers, hlAns, repDF = repDF, num_choices=num_choices, useRep=useRep, threshold_func=threshold_func)
        out = out+(question_type, num_choices, length)
    elif question_type == 'checklist':
        out = evaluateChecklist(answers, users, starts, ends, numUsers, length, repDF, sourceText, hlUsers, hlAns, num_choices = num_choices, useRep=useRep, threshold_func = threshold_func)
    return out

def get_answer_data(topic, question, answer, schema_file):
    """
    Returns data about a question/answer pair from a specific schema.

    Args:
        topic: always 1 for now
        question: the question number
        answer: the agreed answer number
        schema_file: the file path of the schema

    Returns:
        ans_uuid: unique schema identifier for the answer/question pair
        has_hl: boolean for if the answer involves highlights
    """
    #answer is string if LMU
    if isinstance(answer, str):
        return 0,0
    schema_data = pd.read_csv(schema_file, encoding='utf-8')
    tqa = "T"+str(topic)+".Q"+str(question)+".A"+str(answer)
    row = schema_data[schema_data['answer_label'] == tqa]
    if row.shape[0]<1:
        return 'XXX',0
    return row['answer_uuid'].iloc[0], row['highlight'].iloc[0]

def getQuestionNumberFromLabel(label):
    return int(re.search(r'(?<=.Q)\d+', label).group(0))

if __name__ == '__main__':
    config_path = './config/'
    input_dir = '../test_data/test_diff_schemas/'
    texts_dir = '../test_data/texts/'
    metadata_dir = '../data/metadata/'
    tua_dir = '../data/tags/'
    schema_dir = '../data/schemas/'
    iaa_output_dir = make_directory('../test_data/out_test_diff_schemas/')
    calc_agreement_directory(input_dir, schema_dir, config_path, texts_dir, repCSV=None, outDirectory=iaa_output_dir,
                                 useRep=False, threshold_func='raw_30')
