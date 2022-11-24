import csv
import pandas as pd
from ChecklistCoding import *
from ExtraInfo import *
import json
from dataV3 import *
from ThresholdMatrix import check_threshold
from repScores import *
import os
import re


def calc_agreement_directory(directory, schema_dir, config_path, texts_path, repCSV=None, outDirectory=None,
                             useRep=False, threshold_func='raw_30'):
    print("IAA STARTING")
    if outDirectory is None:
        x = directory.rfind("/")
        x += 1

        outDirectory = '../../s_iaa_' + directory[x:]

    highlights = []
    schema = []
    for root, dir, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                highlights.append(directory + '/' + file)

    for root, dir, files in os.walk(schema_dir):
        for file in files:
            # no safety check here; everything in the schema directory shoul dbe a schema csv
            schema.append(schema_dir + '/' + file)
    # pick out the schemas actually being used
    temp = []
    i = 0
    while i < len(highlights):
        h = highlights[i]
        hdf = pd.read_csv(h, encoding='utf-8')
        if len(hdf.index) == 0:
            # remove h from highlights
            highlights.remove(h)
        else:
            schem_sha = hdf['schema_sha256'].iloc[0]
            matched_schema = False
            for sch in schema:
                if schem_sha in sch:
                    temp.append(sch)
                    matched_schema = True
                    break
            if not matched_schema:
                raise NameError("No schema matching file:", h)
            i += 1
    schema = temp

    assert (len(schema) == len(highlights))
    for i in range(len(highlights)):
        calc_scores(highlights[i], config_path, texts_path, schema[i], reputation_csv=repCSV,
                    output_directory=outDirectory, use_reputation=useRep,
                    datahunt_directory=directory, threshold_function=threshold_func)
    return outDirectory


def calc_scores(highlight_filename, config_path, texts_path, schema_file, reputation_csv=None,
                output_directory=None, use_reputation=False, datahunt_directory=None, threshold_function='logis_0'):
    highlight_df = pd.read_csv(highlight_filename, encoding='utf-8')
    if len(highlight_df) == 0:
        raise Exception("Datahunt file is empty")

    schema_sha = highlight_df["schema_sha256"].iloc[0]
    schema_df = pd.read_csv(schema_file)
    highlight_df["question_type"] = highlight_df.apply(find_question_type(schema_df), axis=1)
    # For most workflows there's only 1 task per file, but some tests will have multiple tasks in a file
    for task in highlight_df["quiz_task_uuid"].unique():
        task_df = highlight_df[highlight_df["quiz_task_uuid"] == task]
        task_df["rep_score"] = task_df.apply(find_user_reputation(use_reputation, reputation_csv), axis=1)
        # this gets a row per unique answer
        output_df = task_df.groupby(["answer_uuid"]).filter(lambda x: True)
        output_df["num_users"] = output_df.apply(count_rep_from_task(task_df, "question_label"), axis=1)
        output_df["answer_count"] = output_df.apply(count_rep_from_task(task_df, "answer_uuid"), axis=1)
        #   todo: ordinal scoring
        output_df["percent_agreement"] = output_df["answer_count"]/output_df["num_users"]

        output_df["agreement_score"] = output_df.apply(count_rep_from_task(task_df, "answer_uuid"), axis=1)
        print(output_df)
    return 0


def find_question_type(schema_df):
    def get(row):
        schema_answer_row = schema_df[schema_df["answer_uuid"] == row["answer_uuid"]]
        return schema_answer_row["question_type"].iloc[0]
    return get


def find_user_reputation(use_rep, rep_df):
    def get(row):
        if not use_rep:
            return 1
        if rep_df is None:
            raise Exception("Expected a user reputation dataframe, but none was found")
        rep_row = rep_df[rep_df["Users"] == row["contributor_uuid"]]
        return rep_row["Scores"].iloc[0]
    return get


def check_threshold_passed(output_df, threshold_func):
    def get(row):
        q_rows = output_df[output_df["question_label"] == row["question_label"]]
        if row["question_type"] == "CHECKBOX" or row["percent_agreement"] >= q_rows["percent_agreement"].max():
            return check_threshold(row["percent_agreement"], row["num_users"], func=threshold_func)
    return get

# Gets sum of rep for all task rows matching the field from the output_df
# User will only be counted once, even if there's multiple rows for the user, such as the case
# 1 user submitted multiple highlights
def count_rep_from_task(task_df, field):
    def get(row):
        matching_rows = task_df[task_df[field] == row[field]]
        matching_rows = matching_rows.drop_duplicates("contributor_uuid")
        # take the sum of the rep scores
        # if rep isn't being used then rep score is 1
        return matching_rows["rep_score"].sum()

    return get


def calc_scores_dep(highlightfilename, config_path, texts_path, repCSV=None, schemaFile=None,
                    fileName=None, thirtycsv=None, outDirectory=None, useRep=False, directory=None,
                    threshold_func='logis_0'):
    uberDict = dataStorer(highlightfilename, schemaFile)
    if directory.startswith('./'):
        directory = directory[2:]
    if not outDirectory:
        outDirectory = 's_iaa' + directory
        print(outDirectory)
        if outDirectory[0] == '.':
            outDirectory == outDirectory[1:]
    data = [["article_num", "article_sha256", "article_id", "article_filename",
             "source_task_uuid", "tua_uuid", "namespace", "schema_sha256",
             "question_Number", "answer_uuid", "question_type", "agreed_Answer",
             "coding_perc_agreement",
             "highlighted_indices", "agreement_score",
             "num_users", "num_answer_choices",
             "target_text", "question_text", "answer_text", "article_text_length"]]

    if useRep:
        repDF = create_user_reps(uberDict, repCSV)
        print('initialized repScores')
    else:
        repDF = None
    for task in uberDict.keys():  # Iterates throuh each article
        task_id = task
        article_num = get_article_num(uberDict, task_id)
        article_sha = get_article_sha(uberDict, task_id)
        article_filename = get_article_filename(uberDict, task_id)
        article_id = threshold_func + article_sha
        schema_namespace = get_schema(uberDict, task_id)
        schema_sha = get_schema_sha256(uberDict, task_id)
        tua_uuid = get_tua_uuid(uberDict, task_id)
        questions = uberDict[task]['quesData'].keys()
        # get the textfile
        text_file = os.path.join(texts_path, article_sha + ".txt")
        if not (os.path.exists(text_file)):
            for root, dir, files in os.walk(texts_path):
                for file in files:
                    print(file)
            raise Exception("Couldn't find text_file for article {}".format(text_file))

        # print("checking agreement for "+schema_namespace+" task "+task_id)
        # has to be sorted for questions depending on each other to be handled correctly
        for ques in sorted(questions):  # Iterates through each question in an article

            agreements = score(task, ques, uberDict, config_path, text_file, schemaFile, repDF, useRep=useRep,
                               threshold_func=threshold_func)
            if agreements == None:
                continue
            question_text = get_question_text(uberDict, task, ques)
            # if it's a list then it was a checklist question

            if type(agreements) is list:
                # Checklist Question
                for i in range(len(agreements)):
                    codingPercentAgreement, unitizingScore = agreements[i][4], agreements[i][2]
                    winner, units = agreements[i][0], agreements[i][1]
                    selectedText, firstSecondScoreDiff = agreements[i][6], agreements[i][7]
                    question_type, num_choices = agreements[i][8], agreements[i][9]
                    num_users = agreements[i][5]
                    length = agreements[i][10]
                    ques_num = ques
                    ans_uuid, has_hl = get_answer_data(schema_sha, 1, ques_num, winner, schemaFile)
                    if int(has_hl) == 0:
                        units = []
                        unitizingScore = 'NA'
                        inclusiveUnitizing = 'NA'
                    totalScore = calcAgreement(codingPercentAgreement, unitizingScore)
                    answer_content = get_answer_content(uberDict, task, ques, agreements[i][0])

                    units = json.dumps(np.array(units).tolist())

                    # TODO: when separate topics implemented; replace the 1 with th the topicnum

                    data.append(
                        [article_num, article_sha, article_id, article_filename, task_id, tua_uuid, schema_namespace,
                         schema_sha, ques_num, ans_uuid, agreements[i][8], winner,
                         codingPercentAgreement, units,
                         totalScore, num_users, num_choices, selectedText,
                         question_text, answer_content, length])

            else:
                winner, units = agreements[0], agreements[1]
                inclusiveUnitizing, numUsers = agreements[3], agreements[5]
                selectedText, firstSecondScoreDiff = agreements[6], agreements[7]
                question_type, num_choices = agreements[8], agreements[9]
                codingPercentAgreement, unitizingScore = agreements[4], agreements[2]
                length = agreements[10]
                num_users = agreements[5]

                answer_content = get_answer_content(uberDict, task, ques, agreements[0])
                ques_num = ques
                ans_uuid, has_hl = get_answer_data(schema_sha, 1, ques_num, winner, schemaFile)
                if int(has_hl) == 0:
                    units = []
                    unitizingScore = 'NA'
                    inclusiveUnitizing = 'NA'
                totalScore = calcAgreement(codingPercentAgreement, unitizingScore)

                units = json.dumps(np.array(units).tolist())

                data.append(
                    [article_num, article_sha, article_id, article_filename, task_id, tua_uuid, schema_namespace,
                     schema_sha,
                     ques_num, ans_uuid, question_type, winner, codingPercentAgreement,
                     units,
                     totalScore, num_users, num_choices, selectedText,
                     question_text, answer_content, length])

    outDirectory = make_directory(outDirectory)
    path, name = get_path(highlightfilename)
    task_name = re.match(r'(.*?)-Task', name).group()[:-5]
    out_name = task_name + '.IAA-' + task_id + '-Tags.csv'
    print("IAA outputs to:", outDirectory + out_name)

    scores = open(os.path.join(outDirectory, out_name), 'w', encoding='utf-8')
    with scores:
        writer = csv.writer(scores)
        writer.writerows(data)

    if useRep:
        user_rep_task(uberDict, outDirectory + 'S_IAA_' + name, repDF)
        print("user_rep_df updated and saved as UserRepScores.csv")

    return outDirectory


def adjustForJson(units):
    units = str(units)
    out = '['
    prev = False
    for u in range(len(units)):
        if units[u].isdigit():
            out += units[u]
            prev = True
        elif units[u] == ' ' and prev == True:
            out += ', '
        else:
            prev = False
    out += ']'
    return out


def score(article, ques, data, config_path, text_file, schemaFile, repDF=None, useRep=False, threshold_func='logis_0'):
    """calculates the relevant scores for the article
    returns a tuple (question answer most chosen, units passing the threshold,
        the Unitizing Score of the users who highlighted something that passed threshold, the unitizing score
        among all users who coded the question the same way (referred to as the inclusive unitizing score),
         the percentage agreement of the category with the highest percentage agreement """

    starts = get_question_start(data, article, ques)
    ends = get_question_end(data, article, ques)
    length = get_text_length(data, article, ques)
    if len(ends) > 0 and max(ends) > 0:
        hlUsers = get_question_hlUsers(data, article, ques)
        hlAns = get_question_hlAns(data, article, ques)

        # Now load the source_text
        if text_file == None:
            raise Exception("Couldn't find text_file for article", article)

        with open(text_file, 'r', encoding='utf-8') as file:
            sourceText = file.read()
    else:
        sourceText = []
        hlUsers = []
        hlAns = []

    schema = get_schema(data, article)
    if schema_has_dist_function(schemaFile):
        question_type, num_choices = schema_to_type_and_num(ques, schemaFile, config_path)
    else:
        question_type, num_choices = get_type_json(schema, ques, config_path)

    answers = get_question_answers(data, article, ques)
    users = get_question_userid(data, article, ques)
    numUsers = get_num_users(data, article, ques)

    assert (len(answers) == len(users))
    if num_choices == 1:
        return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    if question_type == 'ordinal':
        out = evaluateCoding(answers, users, starts, ends, numUsers, length, sourceText, hlUsers, hlAns, repDF=repDF,
                             dfunc='ordinal', num_choices=num_choices, useRep=useRep, threshold_func=threshold_func)

        out = out + (question_type, num_choices, length)
    elif question_type == 'nominal':
        out = evaluateCoding(answers, users, starts, ends, numUsers, length, sourceText, hlUsers, hlAns, repDF=repDF,
                             num_choices=num_choices, useRep=useRep, threshold_func=threshold_func)
        out = out + (question_type, num_choices, length)
    elif question_type == 'checklist':
        out = evaluateChecklist(answers, users, starts, ends, numUsers, length, repDF, sourceText, hlUsers, hlAns,
                                num_choices=num_choices, useRep=useRep, threshold_func=threshold_func)
    elif question_type == 'none':
        out = None
    return out


def calcAgreement(codingScore, unitizingScore):
    return codingScore
    # below method is not being used anymore due to inconsistencies in unitizing score
    # future updates will incorporate unitizationa greement into the agreement scores of articles
    # """averages coding and unitizing agreement scores to create a final agreement score to be used elsewhere in the
    # Public Editor algorithm"""
    # if codingScore == 'NA':
    #     return unitizingScore
    # elif codingScore == 'L' or codingScore == 'M' or codingScore == 'U':
    #     return codingScore
    # elif unitizingScore == 'NA':
    #     return codingScore
    # elif unitizingScore == 'L' or unitizingScore == 'M' or unitizingScore == 'U':
    #     unitizingScore = 0
    # elif math.isnan(unitizingScore):
    #     return codingScore
    #
    # return (float(codingScore) + float(unitizingScore)) / 2


def run_2step_unitization(data, article, question, repDF):
    starts, ends, length, numUsers, users = get_question_start(data, article, question).tolist(), get_question_end(data,
                                                                                                                   article,
                                                                                                                   question).tolist(), \
                                            get_text_length(data, article, question), get_num_users(data, article,
                                                                                                    question), get_question_userid(
        data, article, question).tolist()
    uqU = np.unique(users)
    userWeightDict = {}
    # for u in uqU:
    # userWeightDict[u] = get_user_rep(u, repDF)
    score, indices, iScore = scoreNuUnitizing(starts, ends, length, numUsers, users, userWeightDict)

    return 'NA', indices, score, score, 'NA'


def get_answer_data(schema_sha, topic, question, answer, schema_file):
    # answer is string if LMU
    if isinstance(answer, str):
        return 0, 0
    schema_data = pd.read_csv(schema_file, encoding='utf-8')
    tqa = "T" + str(topic) + ".Q" + str(question) + ".A" + str(answer)
    row = schema_data[schema_data['answer_label'] == tqa]
    if row.shape[0] < 1:
        return 'XXX', 0
    return row['answer_uuid'].iloc[0], row['highlight'].iloc[0]


if __name__ == '__main__':
    config_path = './config/'
    input_dir = '../data/datahunts/'
    # input_dir = '../data/dh1'
    texts_dir = '../test_data/texts/'
    metadata_dir = '../data/metadata/'
    tua_dir = '../data/tags/'
    schema_dir = '../data/schemas/'
    # output
    iaa_output_dir = make_directory('../test_data/out_test_diff_schemas/')
    calc_agreement_directory(input_dir, schema_dir, config_path, texts_dir, repCSV=None, outDirectory=iaa_output_dir,
                             useRep=False, threshold_func='raw_30')
