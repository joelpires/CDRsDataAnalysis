# -*- coding: utf-8 -*-
"""
Exploratory Analysis Tools for CDR Dataset
March 2019
Joel Pires
"""
import time
import os
import urllib, json
import psycopg2
import configparser
import polyline
import arcpy
#from orderedset import OrderedSet
import matplotlib.pyplot as plt
import unidecode
import numpy as np
import os, sys
import shutil

from matplotlib.collections import PolyCollection
from matplotlib.colors import colorConverter

reload(sys)
sys.setdefaultencoding('utf8')

__author__ = 'Joel Pires'
__date__ = 'January 2019'

import psycopg2
import configparser
import numpy as np
import scipy.stats as st
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.mplot3d import Axes3D, proj3d

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


def stats(data):
    alldata = data
    statistics = {}
    statistics["min"] = np.amin(data)
    statistics["max"] = np.amax(data)
    statistics["mean"] = np.mean(data)
    statistics["median"] = np.median(data)
    statistics["mode"] = st.mode(data)
    statistics["std"] = np.std(data)

    return statistics


"""  
Receives a List resulted from a DB Query
Returns a list with the content parsed
"""


def parseDBColumns(listToParse, collumn, _constructor):
    constructor = _constructor
    collumnList = []
    for i in range(0, len(listToParse)):
        collumnList.append(constructor(listToParse[i][collumn]))

    return collumnList


def stats(data):
    alldata = data
    statistics = {}
    statistics["min"] = np.amin(data)
    statistics["max"] = np.amax(data)
    statistics["mean"] = np.mean(data)
    statistics["median"] = np.median(data)
    statistics["mode"] = st.mode(data)
    statistics["std"] = np.std(data)
    statistics["var"] = np.var(data)
    statistics["q_25"], statistics["q_50"], statistics["q_75"] = np.percentile(data, [25, 50, 75])

    return statistics



def connect():
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
        query = "SELECT * FROM public.finalroutes_stats_typemode"
        cur.execute(query)

        fetched = cur.fetchall()

        for i in fetched:
            print(i)

        return

        municipals = parseDBColumns(fetched, 0, str)
        population = parseDBColumns(fetched, 1, float)
        datasetUsers = parseDBColumns(fetched, 2, float)
        number = parseDBColumns(fetched, 3, float)
        density = parseDBColumns(fetched, 4, float)

        municipals = [unidecode.unidecode(line.decode('utf-8').strip()) for line in municipals]

        for index, elem in enumerate(municipals):
            municipals[index] = elem.replace(" ", "\n")

        array = np.arange(0, len(municipals), 1)
        array0 = [x - 0.4 for x in array]
        array2 = [x - 0.2 for x in array]
        array3 = [x + 0.2 for x in array]

        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-1, 12)
        ax.set_ylim(1, 1000000)
        # plt.yticks(np.arange(0, 560000, 50000), fontsize=14)
        plt.yscale("log")
        rects1 = ax.bar(array[:12], number[:12], width=0.2, color='b', align='center')
        rects2 = ax.bar(array3[:12], density[:12], width=0.2, color='g', align='center')
        rects3 = ax.bar(array0[:12], population[:12], width=0.2, color='r', align='center')
        rects4 = ax.bar(array2[:12], datasetUsers[:12], width=0.2, color='k', align='center')

        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), (
        "Number of Users to Infer Commuting Patterns", "Tower Density - Number of Towers per 20 Km2",
        "Number of Inhabitants", "Number of Users in the Dataset"))
        plt.xticks(array[:12], municipals[:12])

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 1, 1.01 * height,
                    '%d' % int(height),
                    ha='center', va='bottom')

        plt.xlabel("Municipal", fontsize=20)
        plt.grid(True)
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


def main():
    connect()


if __name__ == '__main__':
    main()

