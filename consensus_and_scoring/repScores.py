from math import exp
import numpy as np
import pandas as pd
from dataV3 import *
import os.path
from os import path
#from scipy.stats import norm
import math
import datetime
from dateutil.parser import parse


def create_user_reps(uberDict, csvPath=None):
    """When this function is called, if there is no existing UserRepScore.csv, will create and save a DataFrame with the columns
    Users, Score, Questions, Influence, Index, and LastDate. Each row corresponds to a unique UserID and their influence, score, and
    the 30 most recent question scores. This function also gets rid of any user that has not answered a question for 60 days, so that
    their scores do not affect the standard distribution of scores."""
    if path.exists(csvPath):
        user_rep = CSV_to_userid(csvPath)

    else:
        user_rep = pd.DataFrame(
            columns=['Users', 'Score', 'Questions', 'Influence', 'Index', 'LastDate'] + [str(x) for x in range(30)])
    date = datetime.datetime.today().date()

    for article_sha256 in uberDict.keys():

        for question_num in uberDict[article_sha256]['quesData'].keys():
            # TODO make this not hardCoded, it's identical to data_utils get quesiton userid but we had importerror
            # users_q = data[article_num][question_num][1][1][0]
            users_q = get_question_userid(uberDict, article_sha256, question_num)

            for ids in users_q:
                if ids == 0:
                    continue
                if ids not in user_rep.loc[:, 'Users'].tolist():
                    user_rep = user_rep.append(pd.Series([ids, 1, 1, 1, 0, date] + list(np.zeros(30)),
                                                         index=['Users', 'Score', 'Questions', 'Influence',
                                                                'Index', 'LastDate'] + [str(x) for x in range(30)]),
                                               ignore_index=True)

    for id in user_rep.loc[:, 'Users'].tolist():
        if (date - (user_rep.loc[user_rep['Users'] == id, 'LastDate'].iloc[0])).days > 60:
            user_rep = user_rep[user_rep.Users != id]

    return user_rep


def do_rep_calculation_nominal(userID, answers, answer_choice, highlight_answer, hlUsers, starts, ends, article_length,
                               user_rep_df,
                               checkListScale=1):
    """Using the same DataFrame of UserIDs, Rep Scores, and number of questions, changes the values of the DataFrame
    such that if the user in the list of UserIDs gets their answer right, 1 is added to their score, and 0 if they are
    wrong."""
    if type(answer_choice) == str or type(highlight_answer) == str or len(highlight_answer) == 0:
        return 0

    checked = zip(answers, userID)
    highlight_answer_array = np.zeros(article_length)
    winners = []

    for t in checked:
        user = t[1]
        answer = t[0]

        if (answer == answer_choice):

            do_math(user_rep_df, user, checkListScale)
            winners.append(user)
        else:
            do_math(user_rep_df, user, 0)
    for h in highlight_answer:
        highlight_answer_array[h] = 1

    for x in userID:
        user_highlight = np.zeros(article_length)
        for user_h, user_s, user_e in zip(hlUsers, starts, ends):
            incrementer = user_s
            if user_h == x:
                while incrementer <= user_e:
                    user_highlight[incrementer] = 1
                    incrementer += 1
        score = 1 - np.sum(np.absolute(highlight_answer_array - user_highlight)) / article_length
        do_math(user_rep_df, x, score)


def gaussian_mean(answers):
    """This function is used to find the gaussian mean for calculating the reward for a user who answered
     ordinal questions."""
    result = []
    std = np.std(answers)
    mean = np.mean(answers)
    total = 0
    for i in answers:
        gauss = 1 / (std * np.sqrt(2 * np.pi)) * np.exp(-0.5 * ((i - mean) / std) ** 2)
        result.append(i * (gauss * 10) ** 2)
        total += (gauss * 10) ** 2

    return sum(result) / total


def do_rep_calculation_ordinal(userID, answers, answer_aggregated, num_of_choices, highlight_answer, hlUsers, starts,
                               ends,
                               article_length, user_rep_df):
    """Using the same dataframe of userIDs, rep scores, and number of questions, changes the vals of the dataframe
    such that the they receive the distance calculated by gaussian_mean function from the average answer chosen as a ratio of 0 to 1,
    and that is added to their rep score."""

    if type(answer_aggregated) == str or type(highlight_answer) == str:
        return 0
    checked = zip(answers, userID)
    highlight_answer_array = np.zeros(article_length)
    score_dict = {}

    answer_choice = gaussian_mean(answers)

    for h in highlight_answer:
        highlight_answer_array[h] = 1

    for t in range(len(userID)):
        user = userID[t]
        answer = answers[t]

        points = (1 - abs(answer_choice - answer) / num_of_choices)
        do_math(user_rep_df, user, points)
        score_dict[user] = points

    for x in userID:
        points = score_dict[x]
        user_highlight = np.zeros(article_length)

        for user_h, user_s, user_e in zip(hlUsers, starts, ends):
            incrementer = user_s
            if user_h == x:
                while incrementer <= user_e:
                    user_highlight[incrementer] = 1
                    incrementer += 1

        score = points * (1 - np.sum(np.absolute(highlight_answer_array - user_highlight)) / article_length)

        do_math(user_rep_df, x, score)


def getUserHighlights(userId, hlUsers, starts, ends, length):
    """Gets the UserHighlights for a specific user."""
    hlUsers = np.array(hlUsers)
    starts = np.array(starts)
    ends = np.array(ends)
    userMask = hlUsers == userId

    uStarts = starts[userMask]
    uEnds = ends[userMask]
    hlArr = startsToBool(uStarts, uEnds, length)
    return hlArr


def startsToBool(starts, ends, length):
    """Turns highlights into boolean lists"""
    out = np.zeros(length)
    for i in range(len(starts)):
        for x in range(starts[i], ends[i]):
            out[x] = 1
    return out


def checkDuplicates(answers, userID, starts, ends, article_length):
    checked = []
    int_users = {}
    # Changed this to just starts, ends; guaranteed no duplicates of answers or userID
    for i in range(len(starts)):
        if [starts[i], ends[i]] not in checked:
            checked.append([starts[i], ends[i]])
    for x in checked:
        article_array = np.zeros(article_length).tolist()
        if x[0] not in int_users.keys():

            article_array[x[2]:x[3] + 1] = np.ones(x[3] - x[2] + 1).tolist()
            int_users[x[0]] = article_array
        else:
            article_array = int_users[x[0]]
            article_array[x[2]:x[3] + 1] = np.ones(x[3] - x[2] + 1).tolist()
            int_users[x[0]] = article_array

    return checked, int_users


def do_math(data, userID, reward):
    """This function takes in the points added to one user and updates the UserRep DataFrame to update that one user's score
    using the equations set for calculating reputation."""
    if userID == 0:
        return
    oldlast30mean = np.mean(np.array(data.loc[data['Users'] == userID, [str(x) for x in range(30)]]))

    user = data.loc[data['Users'] == userID]

    data.loc[data['Users'] == userID, 'LastDate'] = datetime.datetime.today().date()
    index = data.loc[data['Users'] == userID, 'Index'].tolist()[0]
    data.loc[data['Users'] == userID, str(int(index))] = reward

    last30 = data.loc[data['Users'] == userID, [str(x) for x in range(30)]]
    last30mean = np.mean(np.array(last30.dropna(axis=1)))

    if (index < 29):
        data.loc[data['Users'] == userID, 'Index'] = index + 1
    else:
        data.loc[data['Users'] == userID, 'Index'] = 0
    n = float(user['Questions'].iloc[0])
    if (n > 29):
        r = (float(user['Score'].iloc[0]) - oldlast30mean * .5) * 2

    else:
        r = float(user['Score'].iloc[0])

    points = r * n
    points = points + reward

    n = n + 1
    data.loc[data['Users'] == userID, 'Questions'] = n

    if n > 29:
        data.loc[data['Users'] == userID, 'Score'] = (points / n) * .5 + last30mean * .5
    else:

        data.loc[data['Users'] == userID, 'Score'] = (points / n)

    influence, data = get_user_rep(userID, data, True)
    userid_to_CSV(data)


def calc_influence(data, userID):
    """Taking in a list of UserID's, this will take the repuation score of each User and output a list of their influence
    based on their reputation score."""
    return_vals = list()
    for u in userID:
        r = data.loc[data['Users'] == u]['Score']
        inf = 2 / (1 + exp(-r + 5))
        return_vals.append(inf)
    return return_vals


def user_rep_task(uberDict, task_csv, user_rep_df):
    """This is the function every time agreement is calculated. To use, input the master data structure, the agreed upon
    answers csv, and the pre-existing user_rep dataframe created by function create_user_reps."""

    agreement_output = pd.read_csv(task_csv)
    for i in agreement_output.itertuples():
        article_sha = i[2]
        agreement = i[9]
        task_num = i[3]
        question_num = i[6]
        question_type = i[8]
        highlights = i[12]
        if highlights == 'L' or highlights == 'M' or highlights == 'U':
            None
        else:

            highlights = get_indices_hard(i[12])
        num_of_users = i[16]
        users = get_question_userid(uberDict, task_num, question_num)
        answers = get_question_answers(uberDict, task_num, question_num)

        user_highlights_id = get_question_hlUsers(uberDict, task_num, question_num)
        user_highlights_start = get_question_start(uberDict, task_num, question_num)
        user_highlights_end = get_question_end(uberDict, task_num, question_num)
        user_highlights_answer = get_question_hlAns(uberDict, task_num, question_num)
        article_len = get_text_length(uberDict, task_num, question_num)
        number_of_answers = i[17]
        if agreement == 'L' or agreement == 'M' or agreement == 'U' or agreement == 0 or type(article_len) == pd.Series:
            pass
        else:

            agreement = (agreement)
            if question_type == 'nominal':
                do_rep_calculation_nominal(users, answers, agreement, highlights, user_highlights_id,
                                           user_highlights_start, user_highlights_end, article_len,
                                           user_rep_df=user_rep_df)

            if question_type == 'ordinal':
                hl_users = get_question_hlUsers(uberDict, task_num, question_num)

                do_rep_calculation_ordinal(users, answers, agreement, number_of_answers, highlights, hl_users,
                                           user_highlights_start, user_highlights_end, article_len,
                                           user_rep_df=user_rep_df)
            elif question_type == 'checklist':
                i = 0
                user_array = [0] * num_of_users
                answer_array = [0] * num_of_users
                highlight_users_array = []
                highlight_starts = []
                highlight_ends = []
                for a, u in zip(answers, users):
                    if i == num_of_users:
                        break
                    if int(a) == int(agreement):
                        user_array[i] = u
                        answer_array[i] = 1
                    else:
                        if u not in user_array:
                            user_array[i] = u
                    i += 1

                for h_a, h_u, h_s, h_e in zip(user_highlights_answer, user_highlights_id, user_highlights_start,
                                              user_highlights_end):
                    if h_a == agreement:
                        highlight_users_array.append(h_u)
                        highlight_starts.append(h_s)
                        highlight_ends.append(h_e)
                do_rep_calculation_nominal(user_array, answer_array, 1, highlights, highlight_users_array,
                                           highlight_starts, highlight_ends, article_len, user_rep_df)


def userid_to_CSV(dataframe):
    """This function will save the User Rep Score dataframe as UserRepScores.csv"""

    dataframe.to_csv("UserRepScores.csv")


def CSV_to_userid(path):
    """This function will take in the path name of the UserRepScore.csv and output the dataframe corresponding."""

    csv = pd.read_csv(path, index_col=False)
    dataframe = csv.loc[:,
                ['Users', 'Score', 'Questions', 'Influence', 'Index', 'LastDate'] + [str(x) for x in range(30)]]
    dataframe['LastDate'] = [x.date() for x in pd.to_datetime(dataframe['LastDate'], infer_datetime_format=True)]

    return dataframe


def last30_to_CSV(dataframe):
    """This function will save the last 30 questions rep points dataframe as last30.csv"""
    dataframe.to_csv("last30.csv")


def CSV_to_last30(path):
    """This function opens the csv of the last 30 questions rep points dataframe as last30.csv"""
    return pd.read_csv(path, index_col=False).loc[:, ['Users', 'Index'] + list(range(30))]


def get_user_rep(id, repDF, useRep=False):
    """This function takes in the existing UserRep DataFrame and calculates the influence of a user by taking in
    every users' score into a CDF and calculate the number of standard deviations that user is from the average,
    and using that multiplied by a multiplier X."""
    if repDF is None or not useRep:
        return 1, repDF

    else:
        x = 2
        users = repDF.loc[:, 'Users']
        question = np.array(repDF.loc[:, "Questions"])

        scores = np.array(repDF.loc[:, 'Score'])
        avg_scores = np.mean(scores)
        std_scores = math.sqrt(np.mean((scores - avg_scores) ** 2))
        for u in users.tolist():
            user_score = float(repDF.loc[repDF['Users'] == u]['Score'].iloc[0])
            infl = norm.cdf(user_score, avg_scores, std_scores)

            repDF.loc[repDF['Users'] == u, 'Influence'] = infl * x
            if (u == id):
                influence = repDF.loc[repDF['Users'] == id, 'Influence']
                if repDF.loc[repDF['Users'] == id]['Questions'].iloc[0] < 30:
                    influence = .8

    return influence, repDF

