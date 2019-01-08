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
def parseDBColumns(listToParse, collumn):
    collumnList = []
    for i in range(0, len(listToParse)):
        collumnList.append(int(listToParse[i][collumn]))

    return collumnList



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

        """ number of different active users  (calls) x calls activity throughout the year """

        cur.execute('SELECT originating_id, duration_amt FROM public.call_fct WHERE duration_amt != -1')
        fetched = cur.fetchall()

        allOriginatingIDs = parseDBColumns(fetched, 0)
        allDurations = parseDBColumns(fetched, 1)

        """
        frequenciesOfCallsByUser = dict(collections.Counter(allOriginatingIDs))
        numberOfCallsByNumberOfUsers = collections.Counter(frequenciesOfCallsByUser.values()).most_common()
        numberOfCalls, NumberOfUsersCalls = zip(*numberOfCallsByNumberOfUsers)
          
        fig = plt.figure()
        ax = plt.axes()
        plt.title("Call Frequency per Different Number of Users Throughout the Year")
        plt.xlabel("Number of Different Users")
        plt.ylabel("Number of Calls")
        plt.xscale("log")
        plt.yscale("log")
        ax.plot( NumberOfUsersCalls, numberOfCalls,'x')
        plt.grid(True)
        plt.show()
        """


        """ number of different active users (calls) x duration calls throughout the year """


        allOriginatingIDsCopy = allOriginatingIDs

        usersDurationsDict = defaultdict(int)
        for index, val in enumerate(allOriginatingIDsCopy):
            usersDurationsDict[val] += allDurations[index]/60.0

        durationsByNumberOfUsers = collections.Counter(usersDurationsDict.values()).most_common()
        differentDurations, NumberOfUsersDurations = zip(*durationsByNumberOfUsers)

        print differentDurations
        print NumberOfUsersDurations


        fig = plt.figure()
        ax = plt.axes()
        plt.title("Duration of the Calls per Different Number of Users Throughout the Year")
        plt.xlabel("Number of Different Users")
        plt.ylabel("Duration of the Calls (in minutes)")
        ax.plot(NumberOfUsersDurations, differentDurations,'ro')
        plt.grid(True)
        plt.show()



        """ number of different visited cells (received or call) x number of subjects throughout the year"""

        """ user's radius (received or call) x number of subjects throughout the year"""

        """ number of social ties x number of subjects"""

        """ social ties strength x number of subjects"""

        """ number of subjects throughout the year x social ties strength"""




        """
        cur.execute('SELECT DISTINCT originating_id FROM public.call_fct')
        differentUsers = cur.fetchall()
        


        callFrequencies = []
        for item in differentUsers:
            frequency = cur.execute('SELECT COUNT (originating_id) FROM public.call_fct WHERE originating_id = ' + str(item))
            frequency = cur.fetchall()
            parsed = parseDBQueries(frequency)[0]
            print(parsed)
            if str(parsed) == 'None':
                print("ENTRO??")
                callFrequencies += [0]
            else:
                callFrequencies += [parsed]
        #minhaLista = list(unique(callFrequencies))
        #print(minhaLista)
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
        print x


def main():
    connect()


if __name__ == '__main__':
    main()
