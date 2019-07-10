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
        plt.ylabel("Percentage of Commuting Routes", fontsize=20)
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
        plt.ylabel("Percentage of Commuting Routes", fontsize=20)
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
        plt.ylabel("Percentage of Commuting Routes", fontsize=20)
        plt.grid(True)
        plt.show()


        """
        query = "SELECT * FROM public.finalroutes_stats_travel_modes"
        cur.execute(query)

        fetched = cur.fetchall()

        cities = parseDBColumns(fetched, 0, str)

        transportmodes = parseDBColumns(fetched, 2, str)
        transportmodes = transportmodes[:12]
        percentage = parseDBColumns(fetched, 3, float)

        array = np.arange(0, len(transportmodes), 1)

        array2 = [x - 0.3 for x in array]
        array0 = [x - 0 for x in array]
        array3 = [x + 0.3 for x in array]
        """
        #COIMBRA
        """
        hw_percentage = percentage[0:12]
        hwh_percentage = percentage[12:24]
        wh_percentage = percentage[24:36]

        ingeneral = []
        unimodal = []
        multimodal = []
        hw_ingeneral = []
        hwh_ingeneral = []
        wh_ingeneral = []
        hw_unimodal = []
        hwh_unimodal = []
        wh_unimodal = []
        hw_multimodal = []
        hwh_multimodal = []
        wh_multimodal = []

        for index, value in enumerate(transportmodes):
            temp = value + " "
            temp = temp.replace(",\"", "")
            temp = temp.replace("\"", "")
            temp = temp.replace("(", "")
            temp = temp.replace(")", "")
            temp = temp.replace(",", "_")
            temp = temp.replace("COMMUTER_TRAIN", "TRAIN")
            temp = temp.replace("_", "\nAND\n")
            temp = temp.replace(" IN GENERAL", "\n(IN GENERAL)")
            if(temp.find('\n') == -1):
                temp = temp.replace("BUS ", "BUS\n(UNIMODAL)")
                temp = temp.replace("SUBWAY ", "SUBWAY\n(UNIMODAL)")
                temp = temp.replace("TRAIN ", "TRAIN\n(UNIMODAL)")
                temp = temp.replace("DRIVING ", "DRIVING\n(UNIMODAL)")
                temp = temp.replace("WALKING ", "WALKING\n(UNIMODAL)")
            else:
                temp = temp.replace("BUS ", "BUS\n(MULTIMODAL)")
                temp = temp.replace("SUBWAY ", "SUBWAY\n(MULTIMODAL)")
                temp = temp.replace("TRAIN ", "TRAIN\n(MULTIMODAL)")



            if (temp.find("IN GENERAL") != -1):
                ingeneral.append(temp)
                hw_ingeneral.append(hw_percentage[index])
                hwh_ingeneral.append(hwh_percentage[index])
                wh_ingeneral.append(wh_percentage[index])

            elif (temp.find("MULTIMODAL") != -1):
                multimodal.append(temp)
                hw_multimodal.append(hw_percentage[index])
                hwh_multimodal.append(hwh_percentage[index])
                wh_multimodal.append(wh_percentage[index])

            else:
                unimodal.append(temp)
                hw_unimodal.append(hw_percentage[index])
                hwh_unimodal.append(hwh_percentage[index])
                wh_unimodal.append(wh_percentage[index])



        transportmodes = unimodal + multimodal + ingeneral
        hw_percentage = hw_unimodal + hw_multimodal + hw_ingeneral
        hwh_percentage = hwh_unimodal + hwh_multimodal + hwh_ingeneral
        wh_percentage = wh_unimodal + wh_multimodal + wh_ingeneral

        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-0.7, 12)
        ax.set_ylim(0, 87)
        plt.yticks(np.arange(0, 87, 2), fontsize=16)
        #plt.yscale("log")


        rects1 = ax.bar(array0, hw_percentage, width=0.3, color='b', align='center')
        rects2 = ax.bar(array2, hwh_percentage, width=0.3, color='g', align='center')
        rects3 = ax.bar(array3, wh_percentage, width=0.3, color='r', align='center')


        ax.legend((rects1[0], rects2[0], rects3[0]), (
        "Home <-> Workplace", "Home -> Workplace",
        "Workplace -> Home"))
        plt.xticks(array, transportmodes, fontsize=12)

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, 1.01 * height,
                    '%.1f' % float(round(height,2)),
                    ha='center', va='bottom', fontsize=12)

        plt.xlabel("Travel Modes", fontsize=20)
        plt.ylabel("Percentage of Commuting Routes", fontsize=20)
        plt.grid(True)
        plt.show()
        """
        """
        #LISBOA

        hw_percentage = percentage[36:48]
        hwh_percentage = percentage[48:60]
        wh_percentage = percentage[60:72]

        ingeneral = []
        unimodal = []
        multimodal = []
        hw_ingeneral = []
        hwh_ingeneral = []
        wh_ingeneral = []
        hw_unimodal = []
        hwh_unimodal = []
        wh_unimodal = []
        hw_multimodal = []
        hwh_multimodal = []
        wh_multimodal = []

        for index, value in enumerate(transportmodes):
            temp = value + " "
            temp = temp.replace(",\"", "")
            temp = temp.replace("\"", "")
            temp = temp.replace("(", "")
            temp = temp.replace(")", "")
            temp = temp.replace(",", "_")
            temp = temp.replace("COMMUTER_TRAIN", "TRAIN")
            temp = temp.replace("_", "\nAND\n")
            temp = temp.replace(" IN GENERAL", "\n(IN GENERAL)")
            if(temp.find('\n') == -1):
                temp = temp.replace("BUS ", "BUS\n(UNIMODAL)")
                temp = temp.replace("SUBWAY ", "SUBWAY\n(UNIMODAL)")
                temp = temp.replace("TRAIN ", "TRAIN\n(UNIMODAL)")
                temp = temp.replace("DRIVING ", "DRIVING\n(UNIMODAL)")
                temp = temp.replace("WALKING ", "WALKING\n(UNIMODAL)")
            else:
                temp = temp.replace("BUS ", "BUS\n(MULTIMODAL)")
                temp = temp.replace("SUBWAY ", "SUBWAY\n(MULTIMODAL)")
                temp = temp.replace("TRAIN ", "TRAIN\n(MULTIMODAL)")



            if (temp.find("IN GENERAL") != -1):
                ingeneral.append(temp)
                hw_ingeneral.append(hw_percentage[index])
                hwh_ingeneral.append(hwh_percentage[index])
                wh_ingeneral.append(wh_percentage[index])

            elif (temp.find("MULTIMODAL") != -1):
                multimodal.append(temp)
                hw_multimodal.append(hw_percentage[index])
                hwh_multimodal.append(hwh_percentage[index])
                wh_multimodal.append(wh_percentage[index])

            else:
                unimodal.append(temp)
                hw_unimodal.append(hw_percentage[index])
                hwh_unimodal.append(hwh_percentage[index])
                wh_unimodal.append(wh_percentage[index])
            #debug
            transportmodes[index] = temp
        """
        """
        transportmodes = unimodal + multimodal + ingeneral
        hw_percentage = hw_unimodal + hw_multimodal + hw_ingeneral
        hwh_percentage = hwh_unimodal + hwh_multimodal + hwh_ingeneral
        wh_percentage = wh_unimodal + wh_multimodal + wh_ingeneral
        """
        """
        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-0.7, 12)
        ax.set_ylim(0, 87)
        plt.yticks(np.arange(0, 87, 2), fontsize=16)
        #plt.yscale("log")


        rects1 = ax.bar(array0, hw_percentage, width=0.3, color='b', align='center')
        rects2 = ax.bar(array2, hwh_percentage, width=0.3, color='g', align='center')
        rects3 = ax.bar(array3, wh_percentage, width=0.3, color='r', align='center')


        ax.legend((rects1[0], rects2[0], rects3[0]), (
        "Home <-> Workplace", "Home -> Workplace",
        "Workplace -> Home"))
        plt.xticks(array, transportmodes, fontsize=12)

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, 1.01 * height,
                    '%.1f' % float(round(height,2)),
                    ha='center', va='bottom', fontsize=12)

        plt.xlabel("Travel Modes", fontsize=20)
        plt.ylabel("Percentage of Commuting Routes", fontsize=20)
        plt.grid(True)
        plt.show()


        #PORTO

        hw_percentage = percentage[72:84]
        hwh_percentage = percentage[84:96]
        wh_percentage = percentage[96:108]

        ingeneral = []
        unimodal = []
        multimodal = []
        hw_ingeneral = []
        hwh_ingeneral = []
        wh_ingeneral = []
        hw_unimodal = []
        hwh_unimodal = []
        wh_unimodal = []
        hw_multimodal = []
        hwh_multimodal = []
        wh_multimodal = []

        for index, value in enumerate(transportmodes):
            temp = value + " "
            temp = temp.replace(",\"", "")
            temp = temp.replace("\"", "")
            temp = temp.replace("(", "")
            temp = temp.replace(")", "")
            temp = temp.replace(",", "_")
            temp = temp.replace("COMMUTER_TRAIN", "TRAIN")
            temp = temp.replace("_", "\nAND\n")
            temp = temp.replace(" IN GENERAL", "\n(IN GENERAL)")
            if(temp.find('\n') == -1):
                temp = temp.replace("BUS ", "BUS\n(UNIMODAL)")
                temp = temp.replace("SUBWAY ", "SUBWAY\n(UNIMODAL)")
                temp = temp.replace("TRAIN ", "TRAIN\n(UNIMODAL)")
                temp = temp.replace("DRIVING ", "DRIVING\n(UNIMODAL)")
                temp = temp.replace("WALKING ", "WALKING\n(UNIMODAL)")
            else:
                temp = temp.replace("BUS ", "BUS\n(MULTIMODAL)")
                temp = temp.replace("SUBWAY ", "SUBWAY\n(MULTIMODAL)")
                temp = temp.replace("TRAIN ", "TRAIN\n(MULTIMODAL)")



            if (temp.find("IN GENERAL") != -1):
                ingeneral.append(temp)
                hw_ingeneral.append(hw_percentage[index])
                hwh_ingeneral.append(hwh_percentage[index])
                wh_ingeneral.append(wh_percentage[index])

            elif (temp.find("MULTIMODAL") != -1):
                multimodal.append(temp)
                hw_multimodal.append(hw_percentage[index])
                hwh_multimodal.append(hwh_percentage[index])
                wh_multimodal.append(wh_percentage[index])

            else:
                unimodal.append(temp)
                hw_unimodal.append(hw_percentage[index])
                hwh_unimodal.append(hwh_percentage[index])
                wh_unimodal.append(wh_percentage[index])
        """
        """
        transportmodes = unimodal + multimodal + ingeneral
        hw_percentage = hw_unimodal + hw_multimodal + hw_ingeneral
        hwh_percentage = hwh_unimodal + hwh_multimodal + hwh_ingeneral
        wh_percentage = wh_unimodal + wh_multimodal + wh_ingeneral
        """
        """
        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-0.7, 12)
        ax.set_ylim(0, 87)
        plt.yticks(np.arange(0, 87, 2), fontsize=16)
        #plt.yscale("log")


        rects1 = ax.bar(array0, hw_percentage, width=0.3, color='b', align='center')
        rects2 = ax.bar(array2, hwh_percentage, width=0.3, color='g', align='center')
        rects3 = ax.bar(array3, wh_percentage, width=0.3, color='r', align='center')


        ax.legend((rects1[0], rects2[0], rects3[0]), (
        "Home <-> Workplace", "Home -> Workplace",
        "Workplace -> Home"))
        plt.xticks(array, transportmodes, fontsize=12)

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, 1.01 * height,
                    '%.1f' % float(round(height,2)),
                    ha='center', va='bottom', fontsize=12)

        plt.xlabel("Travel Modes", fontsize=20)
        plt.ylabel("Percentage of Commuting Routes", fontsize=20)
        plt.grid(True)
        plt.show()

        """

        """
        query = "SELECT * FROM public.finalroutes_stats_durations"
        cur.execute(query)

        fetched = cur.fetchall()

        cities = parseDBColumns(fetched, 0, str)

        lessthan15mins = parseDBColumns(fetched, 2, float)
        _16minsto30mins = parseDBColumns(fetched, 3, float)
        _31minsto60mins = parseDBColumns(fetched, 4, float)
        _61minsto90mins = parseDBColumns(fetched, 5, float)
        morethan90mins = parseDBColumns(fetched, 6, float)

        cities = [unidecode.unidecode(line.decode('utf-8').strip()) for line in cities]
        commutingtypes = ["Home <-> Workplace", "Home -> Workplace", "Workplace -> Home"]

        array = np.arange(0, len(commutingtypes), 1)

        array2 = [x - 0.35 for x in array]
        array0 = [x - 0.20 for x in array]
        array3 = [x - 0.05 for x in array]
        array4 = [x + 0.1 for x in array]
        array5 = [x + 0.25 for x in array]

        # COIMBRA
        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-0.7, 2.7)
        ax.set_ylim(0, 105)
        plt.yticks(np.arange(0, 105, 3), fontsize=16)
        # plt.yscale("log")

        rects1 = ax.bar(array0, _61minsto90mins[0:3], width=0.15, color='b', align='center')
        rects2 = ax.bar(array2, lessthan15mins[0:3], width=0.15, color='g', align='center')
        rects3 = ax.bar(array3, _31minsto60mins[0:3], width=0.15, color='r', align='center')
        rects4 = ax.bar(array4, _16minsto30mins[0:3], width=0.15, color='k', align='center')
        rects5 = ax.bar(array5, morethan90mins[0:3], width=0.15, color='y', align='center')

        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0], rects5[0]), (
            "Less than 15 mins", "16 mins to 30 mins",
            "31 mins to 60 mins", "61 mins to 90 mins", "more than 90 mins"))
        plt.xticks(array, commutingtypes, fontsize=16)

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, 1.01 * height,
                    '%.2f' % float(round(height, 2)),
                    ha='center', va='bottom', fontsize=16)

        plt.xlabel("Type of Commuting Route", fontsize=20)
        plt.ylabel("Percentage of Commuting Routes", fontsize=20)
        plt.grid(True)
        plt.show()

        # LISBOA

        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-0.7, 2.7)
        ax.set_ylim(0, 105)
        plt.yticks(np.arange(0, 105, 3), fontsize=16)
        # plt.yscale("log")

        rects1 = ax.bar(array0, _61minsto90mins[3:6], width=0.15, color='b', align='center')
        rects2 = ax.bar(array2, lessthan15mins[3:6], width=0.15, color='g', align='center')
        rects3 = ax.bar(array3, _31minsto60mins[3:6], width=0.15, color='r', align='center')
        rects4 = ax.bar(array4, _16minsto30mins[3:6], width=0.15, color='k', align='center')
        rects5 = ax.bar(array5, morethan90mins[3:6], width=0.15, color='y', align='center')

        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0], rects5[0]), (
            "Less than 15 mins", "16 mins to 30 mins",
            "31 mins to 60 mins", "61 mins to 90 mins", "more than 90 mins"))
        plt.xticks(array, commutingtypes, fontsize=16)

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, 1.01 * height,
                    '%.2f' % float(round(height, 2)),
                    ha='center', va='bottom', fontsize=16)

        plt.xlabel("Type of Commuting Route", fontsize=20)
        plt.ylabel("Percentage of Commuting Routes", fontsize=20)
        plt.grid(True)
        plt.show()

        # PORTO
        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-0.7, 2.7)
        ax.set_ylim(0, 105)
        plt.yticks(np.arange(0, 105, 3), fontsize=16)
        # plt.yscale("log")

        rects1 = ax.bar(array0, _61minsto90mins[6:9], width=0.15, color='b', align='center')
        rects2 = ax.bar(array2, lessthan15mins[6:9], width=0.15, color='g', align='center')
        rects3 = ax.bar(array3, _31minsto60mins[6:9], width=0.15, color='r', align='center')
        rects4 = ax.bar(array4, _16minsto30mins[6:9], width=0.15, color='k', align='center')
        rects5 = ax.bar(array5, morethan90mins[6:9], width=0.15, color='y', align='center')

        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0], rects5[0]), (
            "Less than 15 mins", "16 mins to 30 mins",
            "31 mins to 60 mins", "61 mins to 90 mins", "more than 90 mins"))
        plt.xticks(array, commutingtypes, fontsize=16)

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, 1.01 * height,
                    '%.2f' % float(round(height, 2)),
                    ha='center', va='bottom', fontsize=16)

        plt.xlabel("Type of Commuting Route", fontsize=20)
        plt.ylabel("Percentage of Commuting Routes", fontsize=20)
        plt.grid(True)
        plt.show()
        """

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

