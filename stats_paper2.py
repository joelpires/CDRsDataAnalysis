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


        cities = parseDBColumns(fetched, 0, str)

        percentunimodal = parseDBColumns(fetched, 2, float)
        percentmultimodal = parseDBColumns(fetched, 3, float)
        percentpublic = parseDBColumns(fetched, 4, float)
        percentprivate = parseDBColumns(fetched, 5, float)

        cities = [unidecode.unidecode(line.decode('utf-8').strip()) for line in cities]
        commutingtypes = ["Home <-> Workplace", "Home -> Workplace", "Workplace -> Home"]

        array = np.arange(0, len(commutingtypes), 1)

        array2 = [x - 0.3 for x in array]
        array0 = [x - 0.1 for x in array]
        array3 = [x + 0.1 for x in array]
        array4 = [x + 0.3 for x in array]

        # COIMBRA
        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-0.7, 2.7)
        ax.set_ylim(0, 105)
        plt.yticks(np.arange(0, 105, 3), fontsize=16)
        #plt.yscale("log")

        rects1 = ax.bar(array0, percentprivate[0:3], width=0.2, color='b', align='center')
        rects2 = ax.bar(array2, percentunimodal[0:3], width=0.2, color='g', align='center')
        rects3 = ax.bar(array3, percentpublic[0:3], width=0.2, color='r', align='center')
        rects4 = ax.bar(array4, percentmultimodal[0:3], width=0.2, color='k', align='center')

        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), (
        "Private Travel Mode", "Unimodal Travel Mode",
        "Public Travel Mode", "Multimodal Travel Mode"))
        plt.xticks(array, commutingtypes, fontsize=16)

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, 1.01 * height,
                    '%.2f' % float(round(height,2)),
                    ha='center', va='bottom', fontsize=16)

        plt.xlabel("Type of Commuting Route", fontsize=20)
        plt.grid(True)
        plt.show()

        #LISBOA

        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-0.7, 2.7)
        ax.set_ylim(0, 105)
        plt.yticks(np.arange(0, 105, 3), fontsize=16)
        #plt.yscale("log")

        rects1 = ax.bar(array0, percentprivate[3:6], width=0.2, color='b', align='center')
        rects2 = ax.bar(array2, percentunimodal[3:6], width=0.2, color='g', align='center')
        rects3 = ax.bar(array3, percentpublic[3:6], width=0.2, color='r', align='center')
        rects4 = ax.bar(array4, percentmultimodal[3:6], width=0.2, color='k', align='center')

        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), (
            "Private Travel Mode", "Unimodal Travel Mode",
            "Public Travel Mode", "Multimodal Travel Mode"))
        plt.xticks(array, commutingtypes, fontsize=16)

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, 1.01 * height,
                    '%.2f' % float(round(height,2)),
                    ha='center', va='bottom', fontsize=16)

        plt.xlabel("Type of Commuting Route", fontsize=20)
        plt.grid(True)
        plt.show()

        # PORTO
        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-0.7, 2.7)
        ax.set_ylim(0, 105)
        plt.yticks(np.arange(0, 105, 3), fontsize=16)
        #plt.yscale("log")

        rects1 = ax.bar(array0, percentprivate[6:9], width=0.2, color='b', align='center')
        rects2 = ax.bar(array2, percentunimodal[6:9], width=0.2, color='g', align='center')
        rects3 = ax.bar(array3, percentpublic[6:9], width=0.2, color='r', align='center')
        rects4 = ax.bar(array4, percentmultimodal[6:9], width=0.2, color='k', align='center')

        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), (
        "Private Travel Mode", "Unimodal Travel Mode",
        "Public Travel Mode", "Multimodal Travel Mode"))
        plt.xticks(array, commutingtypes, fontsize=16)

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, 1.01 * height,
                    '%.2f' % float(round(height,2)),
                    ha='center', va='bottom', fontsize=16)

        plt.xlabel("Type of Commuting Route", fontsize=20)
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

