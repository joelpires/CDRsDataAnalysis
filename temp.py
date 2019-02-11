"""
Exploratory Analysis Tools for CDR Dataset
January 2019
Joel Pires
"""
import time

__author__ = 'Joel Pires'
__date__ = 'January 2019'

import psycopg2
import configparser
from collections import defaultdict
import datetime
import operator
import collections
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import pandas as pd
import numpy as np
import scipy.stats as st

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

def getExactTime (dateID, type):
    unixTimestamp = int(((732677 + dateID/100000 - 1) - 719528) * 86400 + np.floor(((((float(dateID) - np.floor(dateID / 100000.0) * 100000.0) / 3600.0) * 3600.0) / 60.0)*60.0))

    if type == "dateAndTime":
        return datetime.datetime.fromtimestamp(unixTimestamp).strftime('%d-%m-%Y %H:%M:%S')
    elif type == "date":
        return datetime.datetime.fromtimestamp(unixTimestamp).strftime("%d-%m-%Y")
    elif type == "time":
        return datetime.datetime.fromtimestamp(unixTimestamp).strftime("%H:%M:%S")
    elif type == "weekday":
        day = datetime.datetime.fromtimestamp(unixTimestamp).weekday()
        if (day == 0):
            return "Monday"
        elif (day == 1):
            return "Tuesday"
        elif (day == 2):
            return "Wednesday"
        elif (day == 3):
            return "Thursday"
        elif (day == 4):
            return "Friday"
        elif (day == 5):
            return "Saturday"
        elif (day == 6):
            return "Sunday"
    elif type == "nameMonth":
        month = datetime.datetime.fromtimestamp(unixTimestamp).month

        if (month == 1):
            return "January"
        elif (month == 2):
            return "February"
        elif (month == 3):
            return "March"
        elif (month == 4):
            return "April"
        elif (month == 5):
            return "May"
        elif (month == 6):
            return "June"
        elif (month == 7):
            return "July"
        elif (month == 8):
            return "August"
        elif (month == 9):
            return "September"
        elif (month == 10):
            return "October"
        elif (month == 11):
            return "November"
        else:
            return "December"

    elif type == "day":
        return datetime.datetime.fromtimestamp(unixTimestamp).day
    elif type == "month":
        return datetime.datetime.fromtimestamp(unixTimestamp).month
    elif type == "year":
        return datetime.datetime.fromtimestamp(unixTimestamp).year
    elif type == "hour":
        return datetime.datetime.fromtimestamp(unixTimestamp).hour
    elif type == "minutes":
        return datetime.datetime.fromtimestamp(unixTimestamp).minute
    else: #seconds
        return datetime.datetime.fromtimestamp(unixTimestamp).second

def stats(data):
    alldata = data
    statistics = {}
    statistics["min"] = np.amin(data)
    statistics["max"] = np.amax(data)
    statistics["mean"]= np.mean(data)
    statistics["median"] = np.median(data)
    statistics["mode"] = st.mode(data)
    statistics["std"] = np.std(data)

    return statistics


""" Connect to the PostgreSQL database server """
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

        cur.execute('SELECT originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, duration_amt FROM public.call_fct WHERE duration_amt > 0')

        fetchedFCT = cur.fetchall()

        """ obtain records in which calls were made/received from towers in subCall_dim """        # lets assume that subCall_dim is call_dim limit 1000

        cur.execute('SELECT cell_id, latitude, longitude FROM public.call_dim LIMIT 1000')
        fetchedDIM = cur.fetchall()

        subCellIds = parseDBColumns(fetchedDIM, 0, int)
        subCall_fct = []
        forbiddenUsers = set([])
        allUsersRegion = set([])
        allowedUsers = []
        callFrequenciesByUser = defaultdict(int)
        differentActiveDaysByUser = defaultdict(list)

        for index, value in enumerate(fetchedFCT):
            alreadyIn = False
            allUsersRegion.add(value[0])
            allUsersRegion.add(value[2])

            #CALLER
            if(value[1] in subCellIds):
                if(value[0] not in forbiddenUsers):
                    alreadyIn = True
                    subCall_fct.append(value)
                    callFrequenciesByUser[value[0]] += 1
                    date = getExactTime(value[4], "date")
                    if (date not in differentActiveDaysByUser[value[0]]):
                        differentActiveDaysByUser[value[0]].append(date)
            else:
                forbiddenUsers.add(value[0])

            # CALLE
            if (value[3] in subCellIds):
                if(value[2] not in forbiddenUsers):
                    callFrequenciesByUser[value[2]] += 1
                    date = getExactTime(value[4], "date")
                    if (date not in differentActiveDaysByUser[value[2]]):
                        differentActiveDaysByUser[value[2]].append(date)
                    if (alreadyIn == False):
                        subCall_fct.append(value)

            else:   #lets track the forbidden users
                forbiddenUsers.add(value[2])

        allowedUsers = [item for item in allUsersRegion if item not in forbiddenUsers] # allUsersRegion - forbiddenUsers

        callFrequenciesByAllowedUsers = defaultdict(int)
        differentActiveDaysByAllowedUsers = defaultdict(int)

        """find the maximum and the respective user"""
        maxCalls = 0
        maxCallsUser = 0
        maxDaysUser = 0
        maxDays = 0

        for i in allowedUsers:
            tempCalls = callFrequenciesByUser[i]
            tempDays = len(differentActiveDaysByUser[i])
            callFrequenciesByAllowedUsers[i] = tempCalls
            differentActiveDaysByAllowedUsers[i] = tempDays
            if(tempDays > maxDays):
                maxDays = tempDays
                maxDaysUser = i
            if (tempCalls > maxCalls):
                maxCalls = tempCalls
                maxCallsUser = i

        print(len(subCall_fct))
        print(len(allowedUsers))
        print(maxCallsUser)
        print(maxCalls)
        print(differentActiveDaysByAllowedUsers[maxCallsUser])
        print(maxDaysUser)
        print(maxDays)
        print(callFrequenciesByAllowedUsers[maxDaysUser])

        """
        #Parsing the collumns
        allOriginatingIDs = parseDBColumns(fetched, 0, int)

        allOriginatingCellIDs = parseDBColumns(fetched, 1, int)
        allTerminatingIDs = parseDBColumns(fetched, 2, int)
        allTerminatingCellIDs = parseDBColumns(fetched, 3, int)
        allDateIDs = parseDBColumns(fetched, 4, int)
        allDurations = parseDBColumns(fetched, 5, int)
        
        

            
            
        #order users by call activity
        allIDS = allOriginatingIDs + allTerminatingIDs
        usersCallActivity = dict(collections.Counter(allIDS))
        usersCallActivity = sorted(usersCallActivity.items(), key=operator.itemgetter(1))
        #order users by the amount of different days in which call activity was registered
    
        """

        elapsed_time = time.time() - start_time
        print(str(elapsed_time/60) + " minutes")

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
