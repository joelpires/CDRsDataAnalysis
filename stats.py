"""
Exploratory Analysis Tools for CDR Dataset
January 2019
Joel Pires
"""
from itertools import chain

__author__ = 'Joel Pires'
__date__ = 'January 2019'

import psycopg2
from ConfigParser import ConfigParser
from collections import defaultdict
import collections
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy import stats

""" function that will parser the database.ini """
def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()

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

def degreesToRadians(degrees):
    return degrees * np.pi / 180;


def distanceInKmBetweenEarthCoordinates(lat1, lon1, lat2, lon2):
    earthRadiusKm = 6371.0
    dLat = degreesToRadians(lat2-lat1)
    dLon = degreesToRadians(lon2-lon1)
    lat1 = degreesToRadians(lat1)
    lat2 = degreesToRadians(lat2)
    a = np.sin(dLat/2) * np.sin(dLat/2) + np.sin(dLon/2) * np.sin(dLon/2) * np.cos(lat1) * np.cos(lat2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    return earthRadiusKm * c

""" Connect to the PostgreSQL database server """
def connect():

    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)


        # create a cursor
        cur = conn.cursor()

        cur.execute('SELECT cell_id, latitude, longitude FROM public.call_dim')
        fetched = cur.fetchall()

        cellIDs = parseDBColumns(fetched, 0, int)
        cellIDsLats= parseDBColumns(fetched, 1, float)
        cellIDsLons = parseDBColumns(fetched, 2, float)

        coordsByCellIDs = defaultdict(list)
        for index, val in enumerate(cellIDs):
            coordsByCellIDs[val] = [cellIDsLats[index], cellIDsLons[index]]


        cur.execute('SELECT originating_id, originating_cell_id, duration_amt, terminating_cell_id FROM public.call_fct WHERE duration_amt != -1 ORDER BY date_id ')

        fetched = cur.fetchall()

        allOriginatingIDs = parseDBColumns(fetched, 0, int)
        allOriginatingCellIDs = parseDBColumns(fetched, 1, int)
        allDurations = parseDBColumns(fetched, 2, int)
        allTerminatingCellIDs = parseDBColumns(fetched, 3, int)


        numberDifferentPlacesByUser = defaultdict(int)
        terminatingCellIDsByUser = defaultdict(list)
        originatingCellIDsByUser = defaultdict(list)
        differentPlacesByUser = defaultdict(list)
        cellIDsPairsByUser = defaultdict(list)

        for index, val in enumerate(allOriginatingIDs):
            if allTerminatingCellIDs[index] in cellIDs and allOriginatingCellIDs[index] in cellIDs:             #this is becaus there are some originatingCellIDs and terminatingCellIDs that don't happear in cellID
                cellIDsPairsByUser[val].append([allOriginatingCellIDs[index], allTerminatingCellIDs[index]])
            else:
                if allTerminatingCellIDs[index] in cellIDs:
                    terminatingCellIDsByUser[val].append(allTerminatingCellIDs[index])
                if allOriginatingCellIDs[index] in cellIDs:
                    originatingCellIDsByUser[val].append(allOriginatingCellIDs[index])
                    if allOriginatingCellIDs[index] not in differentPlacesByUser[val]:
                        numberDifferentPlacesByUser[val] += 1
                        differentPlacesByUser[val].append(allOriginatingCellIDs[index])

        """ ------  number of different active users  (calls) x calls activity throughout the year ------ """
        """
        frequenciesOfCallsByUser = dict(collections.Counter(allOriginatingIDs))
        numberOfCallsByNumberOfUsers = collections.Counter(frequenciesOfCallsByUser.values()).most_common()
        numberOfCalls, NumberOfUsersCalls = zip(*numberOfCallsByNumberOfUsers)
          
        fig = plt.figure()
        ax = plt.axes()
        plt.title("Call Frequency per Different Number of Users Throughout the Year")
        plt.xlabel("Number of Users")
        plt.ylabel("Number of Calls")
        plt.xscale("log")
        plt.yscale("log")
        ax.plot( NumberOfUsersCalls, numberOfCalls,'x')
        plt.grid(True)
        plt.show()
        """


        """ ------ number of different active users (calls) x duration calls throughout the year ------ """

        """
        usersDurationsDict = defaultdict(float)
        for index, val in enumerate(allOriginatingIDs):
            usersDurationsDict[val] += allDurations[index]/60.0

        durationsByNumberOfUsers = collections.Counter(usersDurationsDict.values()).most_common()
        differentDurations, NumberOfUsersDurations = zip(*durationsByNumberOfUsers)

        fig = plt.figure()
        ax = plt.axes()
        plt.title("Duration of the Calls per Different Number of Users Throughout the Year")
        plt.xlabel("Number of Users")
        plt.ylabel("Duration of the Calls (in minutes)")
        ax.plot(NumberOfUsersDurations, differentDurations,'ro')
        plt.grid(True)
        plt.show()
        """

        """------  number of different visited cells (only calls) x number of subjects throughout the year ------ """

        """
        numberDifferentPlacesByNumberOfUsers = collections.Counter(numberDifferentPlacesByUser.values()).most_common()
        numberDifferentPlaces, NumberOfUsersDifferentPlaces = zip(*numberDifferentPlacesByNumberOfUsers)
        
        fig = plt.figure()
        ax = plt.axes()
        plt.title("Duration of the Calls per Different Number of Users Throughout the Year")
        plt.xlabel("Number of Users")
        plt.ylabel("Number of Different Visited Places")
        ax.plot(NumberOfUsersDifferentPlaces, numberDifferentPlaces,'ro')
        plt.grid(True)
        plt.show()
        """


        """ Range of Distances that cover all the different visited places (only people who call) x number of subjects throughout the year (cuidado que by year nao faz muito sentido)"""
        """
        distanceTravelledByUser = defaultdict(float)

        for user, differentPlaces in differentPlacesByUser.items():

            for index in range(len(differentPlaces)):
                if index >= 1:
                    distanceTravelledByUser[user] += distanceInKmBetweenEarthCoordinates(coordsByCellIDs[differentPlaces[index-1]][0], coordsByCellIDs[differentPlaces[index-1]][1], coordsByCellIDs[differentPlaces[index]][0], coordsByCellIDs[differentPlaces[index]][1])
                else:
                    distanceTravelledByUser[user] += 0

        classes = [0, 0.000001, 5, 10, 20, 50, 100, 500, 1000]
        classNames = ["[0]", "]0 to 5]",  "]5 to 10]", "]10 to 20]", "]20 to 50]", "]100 to 500]", "]500, 1000]"]
        out = pd.cut(distanceTravelledByUser.values(), bins=classes, include_lowest=True)
        ax = out.value_counts().plot.bar(rot=0, color="g",  figsize=(10, 10))
        ax.set_xticklabels(classNames)

        plt.title("Range of Travells Distance")
        plt.ylabel("Number of Users")
        plt.xlabel("Range of Travells Distance")
        plt.grid(True)
        plt.show()
        """

        """Average Distance between receivers and callers throughout the year"""
        """
        averageDistancesByUser = defaultdict(float)

        for user, cellIDPairs in cellIDsPairsByUser.items():
            for index, cellIDPair in enumerate(cellIDPairs):
                originatingCoords = coordsByCellIDs[cellIDPair[0]]
                terminatingCoords = coordsByCellIDs[cellIDPair[1]]

                averageDistancesByUser[user] += distanceInKmBetweenEarthCoordinates(originatingCoords[0], originatingCoords[1], terminatingCoords[0], terminatingCoords[1])

            averageDistancesByUser[user] /= len(cellIDPairs)

        classes = [0, 0.000001, 5, 10, 20, 50, 100, 500, 700]
        classNames = ["[0]", "]0 to 5]",  "]5 to 10]", "]10 to 20]", "]20 to 50]", "]100 to 500]", "]500, 700]"]
        out = pd.cut(averageDistancesByUser.values(), bins=classes, include_lowest=True)
        ax = out.value_counts().plot.bar(rot=0, color="r",  figsize=(10, 10))
        ax.set_xticklabels(classNames)

        plt.title("Average Distance between the Callers and Receivers")
        plt.xlabel("Average Distance (in Kms)")
        plt.ylabel("Number of Users")
        plt.grid(True)
        plt.show()
        """

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def unique(list1):
    # intilize a null list
    unique_list = []
    # traverse for all elements
    for x in list1:
        # check if exists in unique_list or not
        if x not in unique_list:
            unique_list.append(x)
            # print list
    for x in unique_list:
        print (x)


def main():
    connect()


if __name__ == '__main__':
    main()
