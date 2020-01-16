# -*- coding: utf-8 -*-
"""
Exploratory Analysis Tools for CDR Dataset
November 2019
Joel Pires
"""

__author__ = 'Joel Pires'
__date__ = 'November 2019'

import time
import psycopg2
import configparser
import numpy as np
import scipy.stats as st
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import proj3d
import pandas as pd

""" function that will parser the database.ini """


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = configparser.ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


""" Helper function just to obtain a certain collumn of a DB table in a form of a list
    Inputs: listToParse - list of lists, in which each list is a collumn of a DB table
            collumn: specify the index of the collumn that we want to extract
            _constructor: specifiy the type of the data contained in the specidifed collumn
    Outputs: a list with the values of the collumn
"""


def parseDBColumns(listToParse, collumn, _constructor):
    constructor = _constructor
    collumnList = []
    for i in range(0, len(listToParse)):
        collumnList.append(constructor(listToParse[i][collumn]))

    return collumnList


""" Procedure to elaborate basic statistics on a list of data"""


def stats(data):
    statistics = {}
    statistics["min"] = np.amin(data)
    statistics["max"] = np.amax(data)
    statistics["mean"] = np.mean(data)
    statistics["median"] = np.median(data)
    statistics["mode"] = st.mode(data)
    statistics["std"] = np.std(data)
    statistics["var"] = np.var(data)

    return statistics


""" Main Method """


def main():
    start_time = time.time()

    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        ax = plt.axes()

        cur.execute('SELECT "Tower Density (Km2 per Cell)" FROM public.altice_statsmunicipals')
        fetched = cur.fetchall()
        towerDensities = parseDBColumns(fetched, 0, float)

        print("Statistics of Tower Density Values")
        plt.title("Different Tower Density Values for the Various Municipals")
        plt.xlabel("municipal")
        plt.ylabel("Tower Density (Km2 per Cell)")
        plt.plot(towerDensities, 'gx')
        plt.grid(True)
        plt.show()

        print(stats(towerDensities))
        # ----------------------------------------------------------------------------------------------
        classes = [0, 1, 2, 4, 8, 16, 32, 64, 128, 277]
        classNames = ["[0 to 1[", "]1 to 2]", "]2 to 4]", "]4 to 8]", "]8, 16]", "]16, 32]", "]32, 64]", "]64, 128]",
                      "]128, 277]"]
        out = pd.cut(list(towerDensities), bins=classes, include_lowest=True)
        ax = out.value_counts().plot.bar(rot=0, color="g", figsize=(30, 20), fontsize=25)
        ax.set_xticklabels(classNames)
        ax.margins(y=0.0, tight=True)
        plt.ylim(0, 80)
        plt.grid(True)
        rects = ax.patches

        values = []

        allInfo = str(pd.Categorical.value_counts(out))
        lines = allInfo.split("\n")

        for i in range(len(lines) - 1):
            line = lines[i].split("]")
            values.append(int(line[1].strip()))

        for rect, label in zip(rects, values):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label, 2), ha='center', va='bottom', fontsize=19)

        plt.title("Different Tower Density Values for the Various Municipals", fontsize=40)
        plt.ylabel("Number of Users", fontsize=30)
        plt.xlabel("Range of Travells Distance (in Kms)", fontsize=30)
        plt.grid(True)
        plt.show()
        # ----------------------------------------------------------------------------------------------

        cur.execute(
            'SELECT "Average Calls Per Day", "Average of Days Until Call","Nº Active Days" FROM public.altice_region_users_characterization')
        fetched = cur.fetchall()

        averageCallsPerDay = parseDBColumns(fetched, 0, float)
        regularity = parseDBColumns(fetched, 1, float)
        numberActiveDays = parseDBColumns(fetched, 2, int)

        print("Statistics of Average Calls Per Day Values")
        plt.title("Different Average Calls Per Day Values for the Various User")
        plt.xlabel("user")
        plt.ylabel("Average Calls Per Day")
        plt.plot(averageCallsPerDay, 'bx')
        plt.grid(True)
        plt.show()

        print(stats(averageCallsPerDay))

        # ----------------------------------------------------------------------------------------------
        classes = [0, 1, 2, 4, 8, 16, 32, 64, 128, 236]
        classNames = ["[0 to 1[", "]1 to 2]", "]2 to 4]", "]4 to 8]", "]8, 16]", "]16, 32]", "]32, 64]", "]64, 128]",
                      "]128, 236]"]
        out = pd.cut(list(averageCallsPerDay), bins=classes, include_lowest=True)
        ax = out.value_counts().plot.bar(rot=0, color="b", figsize=(30, 20), fontsize=25)
        ax.set_xticklabels(classNames)
        ax.margins(y=0.0, tight=True)
        plt.ylim(0, 190000)
        plt.grid(True)
        rects = ax.patches

        values = []

        allInfo = str(pd.Categorical.value_counts(out))
        lines = allInfo.split("\n")

        for i in range(len(lines) - 1):
            line = lines[i].split("]")
            values.append(int(line[1].strip()))

        for rect, label in zip(rects, values):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label, 2), ha='center', va='bottom', fontsize=19)

        plt.title("Different Average Calls Per Day Values for the Various User", fontsize=40)
        plt.ylabel("user", fontsize=30)
        plt.xlabel("Average Calls Per Day", fontsize=30)
        plt.grid(True)
        plt.show()
        # ----------------------------------------------------------------------------------------------

        print("Statistics of Average of Days Until Call Values")
        plt.title("Different Average of Days Until Call Values for the Various Users")
        plt.xlabel("user")
        plt.ylabel("Average of Days Until Call")
        plt.plot(regularity, 'rx')
        plt.grid(True)
        plt.show()
        print(stats(regularity))

        # ----------------------------------------------------------------------------------------------
        classes = [0, 1, 2, 4, 8, 16, 32, 64, 128, 226]
        classNames = ["[0 to 1[", "]1 to 2]", "]2 to 4]", "]4 to 8]", "]8, 16]", "]16, 32]", "]32, 64]", "]64, 128]",
                      "]128, 226]"]
        out = pd.cut(list(regularity), bins=classes, include_lowest=True)
        ax = out.value_counts().plot.bar(rot=0, color="r", figsize=(30, 20), fontsize=25)
        ax.set_xticklabels(classNames)
        ax.margins(y=0.0, tight=True)
        plt.ylim(0, 250000)
        plt.grid(True)
        rects = ax.patches

        values = []

        allInfo = str(pd.Categorical.value_counts(out))
        lines = allInfo.split("\n")

        for i in range(len(lines) - 1):
            line = lines[i].split("]")
            values.append(int(line[1].strip()))

        for rect, label in zip(rects, values):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label, 2), ha='center', va='bottom', fontsize=19)

        plt.title("Different Average of Days Until Call Values for the Various Users", fontsize=40)
        plt.ylabel("user", fontsize=30)
        plt.xlabel("Average of Days Until Call", fontsize=30)
        plt.grid(True)
        plt.show()
        # ----------------------------------------------------------------------------------------------

        print("Statistics of Nº Active Days Values Values")
        plt.title("Different Nº Active Days Values for the Various Users")
        plt.xlabel("user")
        plt.ylabel("Nº Active Days")
        plt.plot(numberActiveDays, 'yx')
        plt.grid(True)
        plt.show()
        print(stats(numberActiveDays))

        # ----------------------------------------------------------------------------------------------
        classes = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 405]
        classNames = ["[0 to 1[", "]1 to 2]", "]2 to 4]", "]4 to 8]", "]8, 16]", "]16, 32]", "]32, 64]", "]64, 128]",
                      "]128, 256]", "]256, 405]"]
        out = pd.cut(list(numberActiveDays), bins=classes, include_lowest=True)
        ax = out.value_counts().plot.bar(rot=0, color="y", figsize=(30, 20), fontsize=25)
        ax.set_xticklabels(classNames)
        ax.margins(y=0.0, tight=True)
        plt.ylim(0, 160000)
        plt.grid(True)
        rects = ax.patches

        values = []

        allInfo = str(pd.Categorical.value_counts(out))
        lines = allInfo.split("\n")

        for i in range(len(lines) - 1):
            line = lines[i].split("]")
            values.append(int(line[1].strip()))

        for rect, label in zip(rects, values):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label, 2), ha='center', va='bottom', fontsize=19)

        plt.title("Different Nº Active Days Values for the Various Users", fontsize=40)
        plt.ylabel("user", fontsize=30)
        plt.xlabel("Nº Active Days", fontsize=30)
        plt.grid(True)
        plt.show()

        # --------------------------------------------------------
        cur.execute('SELECT * FROM public.altice_experiment5')
        fetched = cur.fetchall()

        regularity = parseDBColumns(fetched, 0, float)
        averageCalls = parseDBColumns(fetched, 1, float)
        numberDays = parseDBColumns(fetched, 2, float)

        fig = plt.figure(figsize=(16, 12))
        plt.subplots_adjust(left=0, bottom=0, right=1, top=1, wspace=0, hspace=0)
        ax = fig.add_subplot(111, projection='3d')

        theta = np.linspace(-4 * np.pi, 4 * np.pi, 100)
        z = np.linspace(-2, 2, 100)
        r = z ** 2 + 1
        x = 25 * r * np.sin(theta)
        y = 350 * r * np.cos(theta)
        plt.rcParams['agg.path.chunksize'] = 10000
        print("Drawing the Chart...")

        # ran
        plt.grid(True)
        ax.set_xlim(-1, 75)
        ax.set_ylim(-1, 100)
        ax.set_zlim(-1, 400)
        plt.gca().invert_xaxis()

        ax.plot(averageCalls, regularity, numberDays, "gx")
        ax.set_xlabel('Call Activity Every x Days', fontsize=18, labelpad=14)
        ax.set_ylabel('Average Calls Per Day', fontsize=18, labelpad=14)
        ax.set_zlabel('Nº of Active Days', fontsize=18, labelpad=14)

        f = lambda x, y, z: proj3d.proj_transform(x, y, z, ax.get_proj())[:2]
        ax.legend(loc="upper left",
                  bbox_transform=ax.transData,
                  prop={'size': 13})

        plt.show()

        # --------------------------------------------------------
        cur.execute('SELECT * FROM public.altice_experiment_3_1')
        fetched = cur.fetchall()
        numberDays = parseDBColumns(fetched, 0, float)
        numberDays_questao1 = [1] * len(numberDays)

        numberDays_racioHome = parseDBColumns(fetched, 1, float)
        numberDays_racioWorkplace = parseDBColumns(fetched, 2, float)
        numberDays_racioHome_Workplace = parseDBColumns(fetched, 3, float)

        cur.execute('SELECT * FROM public.altice_experiment_3_2')
        fetched = cur.fetchall()

        numberDays_questao2 = [2] * len(numberDays)
        numberDays_racioHome_Morning = parseDBColumns(fetched, 1, float)
        numberDays_racioWorkplace_Morning = parseDBColumns(fetched, 2, float)
        numberDays_racioHome_Workplace_Morning = parseDBColumns(fetched, 3, float)

        cur.execute('SELECT * FROM public.altice_experiment_3_3')
        fetched = cur.fetchall()

        numberDays_questao3 = [3] * len(numberDays)
        numberDays_racioHome_Evening = parseDBColumns(fetched, 1, float)
        numberDays_racioWorkplace_Evening = parseDBColumns(fetched, 2, float)
        numberDays_racioHome_Workplace_Evening = parseDBColumns(fetched, 3, float)

        cur.execute('SELECT * FROM public.altice_experiment_3_4')
        fetched = cur.fetchall()

        numberDays_questao4 = [4] * len(numberDays)
        numberDays_racioH_W = parseDBColumns(fetched, 1, float)
        numberDays_racioW_H = parseDBColumns(fetched, 2, float)
        numberDays_racioH_W_or_W_H = parseDBColumns(fetched, 3, float)
        numberDays_racioH_W_and_W_H = parseDBColumns(fetched, 4, float)

        cur.execute('SELECT * FROM public.altice_experiment_3_5')
        fetched = cur.fetchall()

        numberDays_questao5 = [5] * len(numberDays)
        numberDays_racioWeekdays = parseDBColumns(fetched, 1, float)

        fig = plt.figure(figsize=(12, 8))
        ax = plt.axes()

        plt.xlabel("Number of Days", fontsize=18)
        plt.ylabel("Percentage of Users", fontsize=18)
        ax.set_xlim(-1, 420)
        ax.set_ylim(-1, 100)
        plt.xticks(np.arange(min(numberDays) - 1, 420, 20), fontsize=14)
        plt.yticks(np.arange(0, 100, 5), fontsize=14)

        ax.plot(numberDays, numberDays_racioWeekdays, color="#663300", linewidth=4, label="Call Activity on Weekdays")

        ax.plot(numberDays, numberDays_racioHome_Workplace, 'y', linewidth=4, label="Indentified Home and Workplace")

        f = lambda x, y, z: proj3d.proj_transform(x, y, z, ax.get_proj())[:2]
        ax.legend(loc="upper right",
                  bbox_transform=ax.transData,
                  prop={'size': 14})

        plt.grid(True)
        plt.show()

        # --------------------------------------------------------
        cur.execute('SELECT * FROM public.altice_experiment_2_1')
        fetched = cur.fetchall()
        regularity = parseDBColumns(fetched, 0, float)
        regularity_questao1 = [1] * len(regularity)

        regularity_racioHome = parseDBColumns(fetched, 1, float)
        regularity_racioWorkplace = parseDBColumns(fetched, 2, float)
        regularity_racioHome_Workplace = parseDBColumns(fetched, 3, float)

        cur.execute('SELECT * FROM public.altice_experiment_2_2')
        fetched = cur.fetchall()

        regularity_questao2 = [2] * len(regularity)
        regularity_racioHome_Morning = parseDBColumns(fetched, 1, float)
        regularity_racioWorkplace_Morning = parseDBColumns(fetched, 2, float)
        regularity_racioHome_Workplace_Morning = parseDBColumns(fetched, 3, float)

        cur.execute('SELECT * FROM public.altice_experiment_2_3')
        fetched = cur.fetchall()

        regularity_questao3 = [3] * len(regularity)
        regularity_racioHome_Evening = parseDBColumns(fetched, 1, float)
        regularity_racioWorkplace_Evening = parseDBColumns(fetched, 2, float)
        regularity_racioHome_Workplace_Evening = parseDBColumns(fetched, 3, float)

        cur.execute('SELECT * FROM public.altice_experiment_2_4')
        fetched = cur.fetchall()

        regularity_questao4 = [4] * len(regularity)
        regularity_racioH_W = parseDBColumns(fetched, 1, float)
        regularity_racioW_H = parseDBColumns(fetched, 2, float)
        regularity_racioH_W_or_W_H = parseDBColumns(fetched, 3, float)
        regularity_racioH_W_and_W_H = parseDBColumns(fetched, 4, float)

        cur.execute('SELECT * FROM public.altice_experiment_2_5')
        fetched = cur.fetchall()

        regularity_questao5 = [5] * len(regularity)
        regularity_racioWeekdays = parseDBColumns(fetched, 1, float)

        fig = plt.figure(figsize=(14, 10))
        ax = plt.axes()

        plt.xlabel("Regularity (Call Activity in Every x Days)", fontsize=18)
        plt.ylabel("Percentage of Users", fontsize=18)
        ax.set_xlim(-1, 210)
        ax.set_ylim(-1, 100)
        # plt.xticks(np.arange(min(regularity)-1, 210, 10), fontsize=14)
        # plt.yticks(np.arange(0, 100, 2.5), fontsize=14)
        ax.plot(regularity[:-42], regularity_racioWeekdays[:-42], "k", linewidth=3, label="Call Activity on Weekdays")

        ax.plot(regularity[:-42], regularity_racioHome_Workplace[:-42], 'r', linewidth=3,
                label="Indentified Home and Workplace")

        ax.plot(regularity[:-42], regularity_racioHome_Workplace_Morning[:-42], 'k', linewidth=3,
                label="Activity at Home and Workplace in the Morning")

        ax.plot(regularity[:-42], regularity_racioHome_Workplace_Evening[:-42], 'b', linewidth=3,
                label="Activity at Home and Workplace in the Morning")

        ax.plot(regularity[:-42], regularity_racioH_W_or_W_H[:-42], 'g', linewidth=3,
                label="Call Activity During Home->Work or Vice-Versa")
        ax.plot(regularity[:-42], regularity_racioH_W_and_W_H[:-42], 'c', linewidth=3,
                label="Call Activity During Home->to Work and Vice-Versa")

        ax.legend(loc="right",
                  bbox_transform=ax.transData,
                  fontsize=14, labelspacing=2,
                  prop={'size': 14})

        plt.grid(True)
        plt.show()

        stats1 = stats(regularity_racioWeekdays[:-42])
        stats2 = stats(regularity_racioHome_Workplace[:-42])
        stats3 = stats(regularity_racioHome_Workplace_Morning[:-42])
        stats4 = stats(regularity_racioHome_Workplace_Evening[:-42])
        stats5 = stats(regularity_racioH_W_or_W_H[:-42])
        stats6 = stats(regularity_racioH_W_and_W_H[:-42])

        res1 = next(x for x, val in enumerate(regularity_racioWeekdays[:-42])
                    if val < stats1["mean"])
        res2 = next(x for x, val in enumerate(regularity_racioHome_Workplace[:-42])
                    if val < stats2["mean"])
        res3 = next(x for x, val in enumerate(regularity_racioHome_Workplace_Morning[:-42])
                    if val < stats3["mean"])
        res4 = next(x for x, val in enumerate(regularity_racioHome_Workplace_Evening[:-42])
                    if val < stats4["mean"])
        res5 = next(x for x, val in enumerate(regularity_racioH_W_or_W_H[:-42])
                    if val < stats5["mean"])
        res6 = next(x for x, val in enumerate(regularity_racioH_W_and_W_H[:-42])
                    if val < stats6["mean"])

        print(regularity[res1])
        print(regularity[res2])
        print(regularity[res3])
        print(regularity[res4])
        print(regularity[res5])
        print(stats5["mean"])
        print(regularity[res6])
        print(stats6["mean"])
        print(regularity)

        # --------------------------------------------------------
        cur.execute('SELECT * FROM public.altice_experiment_1_1')
        fetched = cur.fetchall()
        averageCalls = parseDBColumns(fetched, 0, float)

        averageCalls_racioHome = parseDBColumns(fetched, 1, float)
        averageCalls_racioWorkplace = parseDBColumns(fetched, 2, float)
        averageCalls_racioHome_Workplace = parseDBColumns(fetched, 3, float)

        cur.execute('SELECT * FROM public.altice_experiment_1_2')
        fetched = cur.fetchall()

        averageCalls_racioHome_Morning = parseDBColumns(fetched, 1, float)
        averageCalls_racioWorkplace_Morning = parseDBColumns(fetched, 2, float)
        averageCalls_racioHome_Workplace_Morning = parseDBColumns(fetched, 3, float)

        cur.execute('SELECT * FROM public.altice_experiment_1_3')
        fetched = cur.fetchall()

        averageCalls_racioHome_Evening = parseDBColumns(fetched, 1, float)
        averageCalls_racioWorkplace_Evening = parseDBColumns(fetched, 2, float)
        averageCalls_racioHome_Workplace_Evening = parseDBColumns(fetched, 3, float)

        cur.execute('SELECT * FROM public.experiment_1_4')
        fetched = cur.fetchall()

        averageCalls_racioH_W = parseDBColumns(fetched, 1, float)
        averageCalls_racioW_H = parseDBColumns(fetched, 2, float)
        averageCalls_racioH_W_or_W_H = parseDBColumns(fetched, 3, float)
        averageCalls_racioH_W_and_W_H = parseDBColumns(fetched, 4, float)

        cur.execute('SELECT * FROM public.altice_experiment_1_5')
        fetched = cur.fetchall()

        averageCalls_racioWeekdays = parseDBColumns(fetched, 1, float)

        fig = plt.figure(figsize=(12, 8))
        ax = plt.axes()

        plt.xlabel("Average Number of CallsMade/Received Per Day", fontsize=18)
        plt.ylabel("Percentage of Users", fontsize=18)
        ax.set_xlim(-1, 42)
        ax.set_ylim(-1, 100)
        plt.xticks(np.arange(min(averageCalls), 42, 2), fontsize=14)
        plt.yticks(np.arange(0, 100, 5), fontsize=14)
        ax.plot(averageCalls[:400], averageCalls_racioWeekdays[:400], color='#000000', linewidth=3,
                label="Call Activity on Weekdays")

        ax.plot(averageCalls[:400], averageCalls_racioHome_Workplace[:400], color='#003300', linewidth=3,
                label="Indentified Home and Workplace")

        ax.plot(averageCalls[:400], averageCalls_racioHome_Workplace_Morning[:400], color='#ffb3b3', linewidth=3,
                label="Activity at Home and Workplace in the Morning")

        ax.plot(averageCalls[:400], averageCalls_racioHome_Workplace_Evening[:400], color='#ff1a1a', linewidth=3,
                label="Activity at Home and Workplace in the Evening")

        ax.plot(averageCalls[:400], averageCalls_racioH_W_or_W_H[:400], color='#ff6600', linewidth=3,
                label="Call Activity During Home->Work or Vice-Versa")
        ax.plot(averageCalls[:400], averageCalls_racioH_W_and_W_H[:400], color='#009973', linewidth=3,
                label="Call Activity During Home->to Work and Vice-Versa")

        plt.grid(True)
        plt.show()

        stats1 = stats(averageCalls_racioWeekdays[:400])
        stats2 = stats(averageCalls_racioHome_Workplace[:400])
        stats3 = stats(averageCalls_racioHome_Workplace_Morning[:400])
        stats4 = stats(averageCalls_racioHome_Workplace_Evening[:400])
        stats5 = stats(averageCalls_racioH_W_or_W_H[:400])
        stats6 = stats(averageCalls_racioH_W_and_W_H[:400])

        res1 = next(x for x, val in enumerate(averageCalls_racioWeekdays[:400])
                    if val > stats1["mean"])
        res2 = next(x for x, val in enumerate(averageCalls_racioHome_Workplace[:400])
                    if val > stats2["mean"])
        res3 = next(x for x, val in enumerate(averageCalls_racioHome_Workplace_Morning[:400])
                    if val > stats3["mean"])
        res4 = next(x for x, val in enumerate(averageCalls_racioHome_Workplace_Evening[:400])
                    if val > stats4["mean"])
        res5 = next(x for x, val in enumerate(averageCalls_racioH_W_or_W_H[:400])
                    if val > stats5["mean"])
        res6 = next(x for x, val in enumerate(averageCalls_racioH_W_and_W_H[:400])
                    if val > stats6["mean"])

        print(averageCalls[res1])
        print(averageCalls[res2])
        print(averageCalls[res3])
        print(averageCalls[res4])
        print(averageCalls[res5])
        print(stats5["mean"])
        print(averageCalls[res6])
        print(stats6["mean"])

        # EXPERIMENT 5 --------------------------------------------------------------------------------------------------------------------

        cur.execute('SELECT * FROM public.altice_experiment5')
        fetched = cur.fetchall()

        regularity = parseDBColumns(fetched, 0, float)
        averageCalls = parseDBColumns(fetched, 1, float)
        numberDays = parseDBColumns(fetched, 2, float)

        fig = plt.figure(figsize=(16, 12))
        plt.subplots_adjust(left=0, bottom=0, right=1, top=1, wspace=0, hspace=0)
        ax = fig.add_subplot(111, projection='3d')

        plt.rcParams['agg.path.chunksize'] = 10000
        print("Drawing the Chart...")

        ax.set_xlim(0, 400)
        ax.set_ylim(-1, 210)
        ax.set_zlim(-1, 45)
        plt.gca().invert_xaxis()

        ax.plot(numberDays, regularity, averageCalls, '*')
        ax.set_xlabel('Nº of Active Days', fontsize=18, labelpad=14)
        ax.set_ylabel('Regularity', fontsize=18, labelpad=14)
        ax.set_zlabel('Average Number of calls', fontsize=18, labelpad=14)

        f = lambda x, y, z: proj3d.proj_transform(x, y, z, ax.get_proj())[:2]
        ax.legend(loc="upper left",
                  bbox_transform=ax.transData,
                  prop={'size': 13})

        plt.show()

        # EXPERIMENT 4_2

        cur.execute('SELECT * FROM public.altice_experiment_4_2')
        fetched = cur.fetchall()

        towerdensities = parseDBColumns(fetched, 1, float)
        racioH_W = parseDBColumns(fetched, 2, float)
        racioW_H = parseDBColumns(fetched, 3, float)
        racioH_W_or_W_H = parseDBColumns(fetched, 4, float)
        racioH_W_and_W_H = parseDBColumns(fetched, 5, float)

        fig = plt.figure(figsize=(12, 8))
        ax = plt.axes()

        plt.xlabel("Tower Density (Km2 per cell)")
        plt.ylabel("Percentage of Users")

        plt.xticks(np.arange(0, 607, 20))
        plt.yticks(np.arange(0, 100, 10))
        ax.plot(towerdensities, racioH_W, "*", label="Intermediate Towers Home->Work")
        ax.plot(towerdensities, racioW_H, "*", label="Intermediate Towers Work->Home")
        ax.plot(towerdensities, racioH_W_or_W_H, "*", label="Intermediate Towers Home->Work or Work->Home")
        ax.plot(towerdensities, racioH_W_and_W_H, "*", label="Intermediate Towers Home->Work and Work->Home")

        f = lambda x, y, z: proj3d.proj_transform(x, y, z, ax.get_proj())[:2]
        ax.legend(loc="upper right",
                  bbox_transform=ax.transData,
                  prop={'size': 13})

        plt.grid(True)
        plt.show()

        fig = plt.figure(figsize=(16, 12))
        plt.subplots_adjust(left=0, bottom=0, right=1, top=1, wspace=0, hspace=0)
        ax = fig.add_subplot(111, projection='3d')

        plt.rcParams['agg.path.chunksize'] = 10000
        print("Drawing the Chart...")

        ax.set_yticks(np.arange(0, 600, 40))
        plt.gca().invert_yaxis()
        plt.xticks(np.arange(4),
                   ('Home->Work', 'Work->Home', 'Home->Work \nor Work->Home', 'Home->Work \nand Work->Home'),
                   fontsize=14)
        ax.plot([0] * len(towerdensities), towerdensities, racioH_W, '*')
        ax.plot([1] * len(towerdensities), towerdensities, racioW_H, '*')
        ax.plot([2] * len(towerdensities), towerdensities, racioH_W_or_W_H, '*')
        ax.plot([3] * len(towerdensities), towerdensities, racioH_W_and_W_H, '*')

        ax.set_ylabel('Tower Density (Km2 Per Cell)', fontsize=18, labelpad=14)
        ax.set_zlabel('Percentage of Users', fontsize=18, labelpad=14)

        f = lambda x, y, z: proj3d.proj_transform(x, y, z, ax.get_proj())[:2]
        ax.legend(loc="upper left",
                  bbox_transform=ax.transData,
                  prop={'size': 13})

        plt.show()

        elapsed_time = time.time() - start_time
        print("EXECUTION TIME: " + str(elapsed_time / 60) + " MINUTES")
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    main()

