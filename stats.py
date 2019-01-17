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

        cur.execute('SELECT cell_id, latitude, longitude FROM public.call_dim')
        fetched = cur.fetchall()

        cellIDs = parseDBColumns(fetched, 0, int)
        cellIDsLats= parseDBColumns(fetched, 1, float)
        cellIDsLons = parseDBColumns(fetched, 2, float)

        coordsByCellIDs = defaultdict(list)
        for index, val in enumerate(cellIDs):
            coordsByCellIDs[val] = [cellIDsLats[index], cellIDsLons[index]]

        cur.execute('SELECT originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, duration_amt FROM public.call_fct WHERE duration_amt > 0 ORDER BY date_id')

        fetched = cur.fetchall()

        """Parsing the collumns"""
        allOriginatingIDs = parseDBColumns(fetched, 0, int)
        allOriginatingCellIDs = parseDBColumns(fetched, 1, int)
        allTerminatingIDs = parseDBColumns(fetched, 2, int)
        allTerminatingCellIDs = parseDBColumns(fetched, 3, int)
        allDateIDs = parseDBColumns(fetched, 4, int)
        allDurations = parseDBColumns(fetched, 5, int)

        zipped = list(zip(allOriginatingIDs, allDateIDs, allOriginatingCellIDs))
        zipped = sorted(zipped, key=operator.itemgetter(0, 1))

        """Constructing the necessary Dictionaries"""
        numberDifferentPlacesByUser = defaultdict(int)
        numberUsersByWeekday = defaultdict(int)
        numberUsersByHour = defaultdict(int)

        differentPlacesByUser = defaultdict(list)

        originatingCellIDsByUser = defaultdict(list)
        cellIDsPairsByUser = defaultdict(list)
        originatingIDsByMonths = defaultdict(list)
        originatingIDsByDate = defaultdict(list)

        frequenciesWeekdays = defaultdict(int)
        frequenciesHours = defaultdict(int)

        temp = defaultdict(list)
        temp2 = defaultdict(list)

        usersDurationsDict = defaultdict(float)
        monthDurationsDict = defaultdict(float)
        weekdaysDurationsDict = defaultdict(float)
        hoursDurationsDict = defaultdict(float)
        dateDurationsDict  = defaultdict(float)

        monthCallNumber = defaultdict(int)
        weekdayCallNumber = defaultdict(int)
        dateCallNumber = defaultdict(int)
        hoursCallNumber = defaultdict(float)

        weekday2 = ""
        hour2 = ""
        l = 0
        for index, val in enumerate(allOriginatingIDs):
            print("FASE 1: " + str(l))
            month = getExactTime(allDateIDs[index], "nameMonth")
            weekday = getExactTime(allDateIDs[index], "weekday")
            date = getExactTime(allDateIDs[index], "date")
            hour = getExactTime(allDateIDs[index], "hour")
            usersDurationsDict[val] += allDurations[index] / 60.0
            monthDurationsDict[month] += allDurations[index] / 60.0
            weekdaysDurationsDict[weekday] += allDurations[index] / 60.0
            dateDurationsDict[date] += allDurations[index] / 60.0
            hoursDurationsDict[hour] += allDurations[index] / 60.0

            monthCallNumber[month] += 1
            weekdayCallNumber[weekday] += 1
            dateCallNumber[date] += 1
            hoursCallNumber[hour] += 1

            originatingCellIDsByUser[val].append(allOriginatingCellIDs[index])

            if allTerminatingCellIDs[index] in cellIDs and allOriginatingCellIDs[index] in cellIDs:             #this is because there are some originatingCellIDs and terminatingCellIDs that don't happear in cellID
                cellIDsPairsByUser[val].append([allOriginatingCellIDs[index], allTerminatingCellIDs[index]])

            if val not in originatingIDsByMonths[month]: originatingIDsByMonths[month].append(val)

            if val not in originatingIDsByDate[date]: originatingIDsByDate[date].append(val)

            if val not in temp2[hour]:
                temp2[hour].append(val)

            if hour2 != hour:
                frequenciesHours[hour] += 1
                numberUsersByHour[hour2] += len(temp2[hour2])
                temp2[hour2] = []
                hour2 = hour

            if val not in temp[weekday]:
                temp[weekday].append(val)

            if weekday2 != weekday:
                frequenciesWeekdays[weekday] += 1
                numberUsersByWeekday[weekday2] += len(temp[weekday2])
                temp[weekday2] = []
                weekday2 = weekday

            if index == len(allOriginatingIDs)-1: #last one
                numberUsersByWeekday[weekday] += len(temp[weekday])
                temp[weekday] = []
                numberUsersByHour[hour] += len(temp2[hour])
                temp2[hour] = []


            if allOriginatingCellIDs[index] not in differentPlacesByUser[val]:
                numberDifferentPlacesByUser[val] += 1
                differentPlacesByUser[val].append(allOriginatingCellIDs[index])
            l += 1

        numberDifferentPlacesByMonth = defaultdict(int)
        numberDifferentPlacesByWeekday = defaultdict(int)
        numberDifferentPlacesByHour = defaultdict(int)
        differentPlacesByHourByUser = defaultdict(dict)
        differentPlacesByMonthByUser = defaultdict(dict)
        differentPlacesByWeekdayByUser = defaultdict(dict)
        usersByMonth = defaultdict(list)
        usersByWeekday = defaultdict(list)
        usersByHour = defaultdict(list)


        previousUser = ""
        for i in set(allOriginatingIDs):
            differentPlacesByMonthByUser[i] = defaultdict(list)
            differentPlacesByWeekdayByUser[i] = defaultdict(list)
            differentPlacesByHourByUser[i] = defaultdict(list)
        l = 0
        for tuple in zipped:
            print("FASE 2: " + str(l))

            user = tuple[0]
            month = getExactTime(tuple[1], "nameMonth")
            weekday = getExactTime(tuple[1], "weekday")
            hour = getExactTime(tuple[1], "hour")
            place = tuple[2]

            if user not in usersByMonth[month]:
                usersByMonth[month].append(user)

            if user not in usersByWeekday[weekday]:
                usersByWeekday[weekday].append(user)

            if user not in usersByHour[hour]:
                usersByHour[hour].append(user)

            if place not in differentPlacesByMonthByUser[user][month]:
                differentPlacesByMonthByUser[user][month].append(place)
                numberDifferentPlacesByMonth[month] += 1

            if place not in differentPlacesByWeekdayByUser[user][weekday]:
                differentPlacesByWeekdayByUser[user][weekday].append(place)
                numberDifferentPlacesByWeekday[weekday] += 1

            if place not in differentPlacesByHourByUser[user][hour]:
                differentPlacesByHourByUser[user][hour].append(place)
                numberDifferentPlacesByHour[hour] += 1

            previousUser = user
            l += 1

        for i in numberDifferentPlacesByWeekday.keys():
            numberDifferentPlacesByWeekday[i] /= frequenciesWeekdays[i]

        for i in numberDifferentPlacesByHour.keys():
            numberDifferentPlacesByHour[i] /= frequenciesHours[i]

        for i in numberDifferentPlacesByMonth.keys():
            numberDifferentPlacesByMonth[i] /= len(usersByMonth[i])

        for i in numberDifferentPlacesByWeekday.keys():
            numberDifferentPlacesByWeekday[i] /= len(usersByWeekday[i])

        for i in numberDifferentPlacesByHour.keys():
            numberDifferentPlacesByHour[i] /= len(usersByHour[i])


        """ --------------------------------  calls activity (calls) x duration of the calls throughout the year ------------------------------- """

        durationsMinutes = [x / 60 for x in allDurations]
        frequenciesOfCallsByDuration = dict(collections.Counter(durationsMinutes))
        differentDurations, numberOfCalls = zip(*frequenciesOfCallsByDuration.items())

        fig = plt.figure()
        ax = plt.axes()
        plt.title("Call Frequency per Different Durations Throughout the Year")
        plt.xlabel("Duration of the Calls (in minutes)")
        plt.ylabel("Number of Calls")
        ax.plot(differentDurations, numberOfCalls,'rx')
        plt.grid(True)
        plt.show()
        

        print("-------------------- STATISTICS: calls activity (calls) x duration of the calls throughout the year ------------------------")
        print("STATS OF CALL'S DURATIONS (in seconds):")
        statistics = stats(allDurations)
        print(statistics)

        differentDurations = list(differentDurations)
        numberOfCalls = list(numberOfCalls)

        minNumberCalls = np.min(numberOfCalls)
        maxNumberCalls = np.max(numberOfCalls)
        minDuration = statistics["min"]
        maxDuration = statistics["max"]
        indexMinDuration = differentDurations.index(minDuration/60.0)
        indexMaxDuration = differentDurations.index(maxDuration/60.0)
        indexMinCalls = numberOfCalls.index(minNumberCalls)
        indexMaxCalls = numberOfCalls.index(maxNumberCalls)

        print("There are " + str(numberOfCalls[indexMinDuration]) + " calls with the minimum duration (" + str(minDuration/60.0) + " minutes)")
        print("There are " + str(numberOfCalls[indexMaxDuration]) + " calls with the maximum duration (" + str(maxDuration/60.0) + " minutes)")
        print(str(differentDurations[indexMinCalls]) + " minutes was the duration recorded in the less amount of calls (" + str(minNumberCalls) + ")")
        print(str(differentDurations[indexMaxCalls]) + " minutes was the duration recorded in the most amount of calls (" + str(maxNumberCalls) + ")")
        print("-------------------------------------------------------------------------------------------------------------------------")



        """ --------------------------------  number of different active users  (calls) x calls activity throughout the year ------------------------------- """

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
        ax.plot( NumberOfUsersCalls, numberOfCalls,'gx')
        plt.grid(True)
        plt.show()
        
        print("-------------------- STATISTICS: number of different active users  (calls) x calls activity throughout the year ------------------------")
        print("Number of active users during the study: " + str(len(set(allOriginatingIDs))))
        print("Number of Calls: " + str(len(set(allOriginatingIDs))))

        NumberOfUsersCalls = list(NumberOfUsersCalls)
        numberOfCalls = list(numberOfCalls)

        minNumberCalls = np.min(numberOfCalls)
        maxNumberCalls = np.max(numberOfCalls)
        minNumberOfUsers = np.min(NumberOfUsersCalls)
        maxNumberOfUsers = np.max(NumberOfUsersCalls)
        indexMinCalls = numberOfCalls.index(minNumberCalls)
        indexMaxCalls = numberOfCalls.index(maxNumberCalls)
        indexMinUsers = NumberOfUsersCalls.index(minNumberOfUsers)
        indexMaxUsers = NumberOfUsersCalls.index(maxNumberOfUsers)

        print("There are " + str(NumberOfUsersCalls[indexMinCalls]) + " different users that made " + str(minNumberCalls) + " calls troughout the year - the lowest number of calls registered.")
        print("There are " + str(NumberOfUsersCalls[indexMaxCalls]) + " different users that made " + str(maxNumberCalls) + " calls troughout the year - the highest number of calls registered.")

        print("-------------------------------------------------------------------------------------------------------------------------")


        """ -----------------------------------  number of different active users in each month  ------------------------------------------------------------ """

        months = []
        frequencies = []
        for key, value in originatingIDsByMonths.items():
            months.append(key)
            frequencies.append(len(value))

        freq_series = pd.Series(frequencies)

        ax = freq_series.plot(kind='bar', figsize=(30, 20), fontsize=25)
        ax.set_title('Number of Active Users in Each Month', fontsize=40)
        ax.set_xlabel('Months', fontsize=30)
        ax.set_ylabel('Number of Active Users', fontsize=30)
        ax.set_xticklabels(months, rotation=0)
        ax.margins(y=0.2, tight=True)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, frequencies):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, label, ha='center', va='bottom', fontsize=19)

        plt.show()
        print("-------------------- STATISTICS: number of different active users in each month ------------------------")
        print("STATS OF NUMBER OF ACTIVE USERS BY MONTH:")
        statistics = stats(frequencies)
        print(statistics)
        minUsers = statistics["min"]
        maxUsers = statistics["max"]
        indexMinMonth = frequencies.index(minUsers)
        indexMaxMonth = frequencies.index(maxUsers)
        print("The month with less active users (" + str(minUsers) + ") was: " + str(months[indexMinMonth]))
        print("The month with most active users (" + str(maxUsers) + ") was: " + str(months[indexMaxMonth]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """ -------------------------------------  number of different active users in each weekday (on average) -------------------------------------------- """

        for key in frequenciesWeekdays.keys():
            numberUsersByWeekday[key] /= frequenciesWeekdays[key]

        weekdays, frequencies = zip(*numberUsersByWeekday.items())
        weekdays = weekdays[1:]
        frequencies = frequencies[1:]

        freq_series = pd.Series(frequencies)

        ax = freq_series.plot(kind='bar', color='r', figsize=(30, 20), fontsize=25)
        ax.set_title('Number of Active Users in Each Weekday (on Average)', fontsize=40)
        ax.set_xlabel('Weekday', fontsize=30)
        ax.set_ylabel('Number of Active Users', fontsize=30)
        ax.set_xticklabels(weekdays, rotation=0)
        ax.margins(y=0.2, tight=True)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, frequencies):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, int(label), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: number of different active users in each weekday (on average) ------------------------")
        minUsers = np.min(frequencies)
        maxUsers = np.max(frequencies)
        indexMinWeekday = frequencies.index(minUsers)
        indexMaxWeekday = frequencies.index(maxUsers)
        print("The Weekday with less active users on average (" + str(int(minUsers)) + ") was: " + str(weekdays[indexMinWeekday]))
        print("The Weekday with most active users on average (" + str(int(maxUsers)) + ") was: " + str(weekdays[indexMaxWeekday]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """ --------------------------------------------  number of different active users in each day throughout the year ---------------------------------- """

        for key in originatingIDsByDate.keys():
            originatingIDsByDate[key] = len(originatingIDsByDate[key])

        dates, frequencies = zip(*originatingIDsByDate.items())

        freq_series = pd.Series(frequencies)

        ax = freq_series.plot(kind='bar', color='g', figsize=(30, 20), fontsize=25)
        ax.set_title('Number of Active Users in Each Day', fontsize=40)
        ax.set_xlabel('Day', fontsize=30)
        ax.set_ylabel('Number of Active Users', fontsize=30)
        ax.set_xticklabels(dates, rotation=0)
        ax.margins(y=0.2, tight=True)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, frequencies):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, int(label), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: number of different active users in each weekday (on average) ------------------------")
        print("STATS OF NUMBER OF ACTIVE USERS BY DAY:")
        statistics = stats(frequencies)
        print(statistics)
        minUsers = np.min(frequencies)
        maxUsers = np.max(frequencies)
        indexMinDate = frequencies.index(minUsers)
        indexMaxDate = frequencies.index(maxUsers)
        print("The Date with less active users on average (" + str(int(minUsers)) + ") was: " + str(dates[indexMinDate]))
        print("The Date with most active users on average (" + str(int(maxUsers)) + ") was: " + str(dates[indexMaxDate]))
        print("-------------------------------------------------------------------------------------------------------------------------")

        """ -----------------------------------------------  number of different active users in each hour  (on average) ------------------------------------ """

        for key in frequenciesHours.keys():
            numberUsersByHour[key] /= frequenciesHours[key]


        hours, frequencies = zip(*numberUsersByHour.items())
        hours = hours[1:]
        frequencies = frequencies[1:]

        freq_series = pd.Series(frequencies)

        ax = freq_series.plot(kind='bar', color='y', figsize=(30, 20), fontsize=25)
        ax.set_title('Number of Active Users in Each Hour (on Average)', fontsize=40)
        ax.set_xlabel('Hour', fontsize=30)
        ax.set_ylabel('Number of Active Users', fontsize=30)
        ax.margins(y=0.2, tight=True)
        ax.set_xticklabels(hours, rotation=0)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, frequencies):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, int(label), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: number of different active users in each hour (on average) ------------------------")
        print("STATS OF NUMBER OF ACTIVE USERS PER HOUR:")
        statistics = stats(frequencies)
        print(statistics)
        minHours = np.min(frequencies)
        maxHours = np.max(frequencies)
        indexMinHour = frequencies.index(minHours)
        indexMaxHour = frequencies.index(maxHours)
        print("The Hour with less active users on average (" + str(int(minHours)) + ") was: " + str(hours[indexMinHour]))
        print("The Hour with most active users on average (" + str(int(maxHours)) + ") was: " + str(hours[indexMaxHour]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """ ----------------------------------------------- number of different active users (calls) x duration of the calls throughout the year ------------ """

        durationsByNumberOfUsers = collections.Counter(usersDurationsDict.values()).most_common()
        differentDurations, NumberOfUsersDurations = zip(*durationsByNumberOfUsers)

        fig = plt.figure()
        ax = plt.axes()
        plt.title("Duration of the Calls per Different Number of Users Throughout the Year")
        plt.xlabel("Number of Users")
        plt.ylabel("Duration of the Calls (in minutes)")
        ax.plot(NumberOfUsersDurations, differentDurations,'yx')
        plt.grid(True)
        plt.show()

        print("-------------------- STATISTICS: number of different active users (calls) x duration of the calls throughout the year ------------------------")
        
        minNumberUsers = np.min(NumberOfUsersDurations)
        maxNumberUsers = np.max(NumberOfUsersDurations)
        minDuration = np.min(differentDurations)
        maxDuration = np.max(differentDurations)
        indexMinDuration = differentDurations.index(minDuration)
        indexMaxDuration = differentDurations.index(maxDuration)

        print("There are " + str(NumberOfUsersDurations[indexMinDuration]) + " users that spent " + str(minDuration) + " minutes on the phone throughout the year - the lowest number of minutes registered per user")
        print("There are " + str(NumberOfUsersDurations[indexMaxDuration]) + " users that spent " + str(maxDuration) + " minutes on the phone throughout the year - the highest number of minutes registered per user")
        print("-------------------------------------------------------------------------------------------------------------------------")

        """ ---------------------------------------------- duration of the calls in each month  ------------------------------------------------------------- """

        months, durations = zip(*monthDurationsDict.items())

        freq_series = pd.Series(durations)

        ax = freq_series.plot(kind='bar', color='b', figsize=(30, 20), fontsize=25)
        ax.set_title('Duration of the Calls in Each Month', fontsize=40)
        ax.set_xlabel('Months', fontsize=30)
        ax.set_ylabel('Duration of the Calls (in minutes)', fontsize=30)
        ax.margins(y=0.2, tight=True)
        ax.set_xticklabels(months, rotation=0)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, durations):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label, 2), ha='center', va='bottom', fontsize=19)

        plt.show()
        print("-------------------- STATISTICS: duration of the calls in each month ------------------------")
        print("STATS OF DURATIONS BY MONTH:")
        statistics = stats(durations)
        print(statistics)
        minDurations = statistics["min"]
        maxDurations = statistics["max"]
        indexMinMonth = durations.index(minDurations)
        indexMaxMonth = durations.index(maxDurations)
        print("The month with less duration of the calls (" + str(minDurations) + " minutes) was: " + str(months[indexMinMonth]))
        print("The month with more duration of the calls (" + str(maxDurations) + " minutes) was: " + str(months[indexMaxMonth]))
        print("-------------------------------------------------------------------------------------------------------------------------")

        """ --------------------------------------------  duration of the calls  in each weekday (on average) ----------------------------------------------- """

        for key in frequenciesWeekdays.keys():
            weekdaysDurationsDict[key] /= frequenciesWeekdays[key]

        weekdays, durations = zip(*weekdaysDurationsDict.items())

        freq_series = pd.Series(durations)

        ax = freq_series.plot(kind='bar', color='r', figsize=(30, 20), fontsize=25)
        ax.set_title('Duration of the Calls in Each Weekday (on average)', fontsize=40)
        ax.set_xlabel('Weekdays', fontsize=30)
        ax.set_ylabel('Duration of the Calls (in minutes)', fontsize=30)
        ax.margins(y=0.2, tight=True)
        ax.set_xticklabels(weekdays, rotation=0)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, durations):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label, 2), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: duration of the calls  in each weekday (on average) ------------------------")
        minUsers = np.min(durations)
        maxUsers = np.max(durations)
        indexMinWeekday = durations.index(minUsers)
        indexMaxWeekday = durations.index(maxUsers)
        print("The Weekday with less duration of the calls on Average (" + str(int(minUsers)) + " minutes) was: " + str(weekdays[indexMinWeekday]))
        print("The Weekday with more duration of the calls on Average (" + str(int(maxUsers)) + " minutes) was: " + str(weekdays[indexMaxWeekday]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """ --------------------------------------------  duration of the calls in each day throughout the year - --------------------------------- """

        dates, durations = zip(*dateDurationsDict.items())

        freq_series = pd.Series(durations)

        ax = freq_series.plot(kind='bar', color='g', figsize=(30, 20), fontsize=25)
        ax.set_title('Duration of the Calls in Each Day', fontsize=40)
        ax.set_xlabel('Days', fontsize=30)
        ax.set_ylabel('Duration of the Calls (in minutes)', fontsize=30)
        ax.margins(y=0.2, tight=True)
        ax.set_xticklabels(dates, rotation=0)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, durations):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label,2), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: duration of the calls in each day throughout the year ------------------------")
        print("STATS OF Duration of the Calls BY DAY:")
        statistics = stats(durations)
        print(statistics)
        minDurations = np.min(durations)
        maxDurations = np.max(durations)
        indexMinDate = durations.index(minDurations)
        indexMaxDate = durations.index(maxDurations)
        print("The Day with less duration of the calls (" + str(int(minDurations)) + " minutes) was: " + str(dates[indexMinDate]))
        print("The Date with more duration of the calls (" + str(int(maxDurations)) + " minutes) was: " + str(dates[indexMaxDate]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """ --------------------------------------------  duration of the calls in each hour ------------------------------------------------------ """

        for key in frequenciesHours.keys():
            hoursDurationsDict[key] /= frequenciesHours[key]

        hours, durations = zip(*hoursDurationsDict.items())

        freq_series = pd.Series(durations)

        ax = freq_series.plot(kind='bar', color='y', figsize=(30, 20), fontsize=25)
        ax.set_title('Duration of the Calls in Each Hour (average)', fontsize=40)
        ax.set_xlabel('Hour', fontsize=30)
        ax.set_ylabel('Duration of the Calls (in minutes)', fontsize=30)
        ax.margins(y=0.2, tight=True)
        ax.set_xticklabels(hours, rotation=0, )
        plt.grid(True)
        rects = ax.patches
        figure()
        for rect, label in zip(rects, durations):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label,2), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: duration of the calls in each hour (on average) ------------------------")
        print("STATS OF DURATION OF THE CALLS PER HOUR:")
        statistics = stats(durations)
        print(statistics)
        minHours = np.min(durations)
        maxHours = np.max(durations)
        indexMinHour = durations.index(minHours)
        indexMaxHour = durations.index(maxHours)
        print("The Hour with less duration of the calls on Average (" + str(int(minHours)) + " minutes) was: " + str(hours[indexMinHour]))
        print("The Hour with more duration of the calls on Average (" + str(int(maxHours)) + " minutes) was: " + str(hours[indexMaxHour]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """ -----------------------------------  Call Activity (number of calls) in each month  ------------------------------------------------------------ """

        months, frequencies = zip(*monthCallNumber.items())

        freq_series = pd.Series(frequencies)

        ax = freq_series.plot(kind='bar', color='b', figsize=(30, 20), fontsize=25)
        ax.set_title('Number of Calls Made in Each Month', fontsize=40)
        ax.set_xlabel('Months', fontsize=30)
        ax.set_ylabel('Number of Calls', fontsize=30)
        ax.margins(y=0.2, tight=True)
        ax.set_xticklabels(months, rotation=0)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, frequencies):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, int(label), ha='center', va='bottom', fontsize=19)

        plt.show()
        print("-------------------- Call Activity (number of calls) in each month ------------------------")
        print("STATS OF NUMBER OF CALLS BY MONTH:")
        statistics = stats(frequencies)
        print(statistics)
        minCalls = statistics["min"]
        maxCalls = statistics["max"]
        indexMinMonth = frequencies.index(minCalls)
        indexMaxMonth = frequencies.index(maxCalls)
        print("The month with less number of calls (" + str(minCalls) + ") was: " + str(months[indexMinMonth]))
        print("The month with more number of calls (" + str(maxCalls) + ") was: " + str(months[indexMaxMonth]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """ -----------------------------------  Call Activity (number of calls) in each weekday (on average)  ------------------------------------------------------------ """

        for key in frequenciesWeekdays.keys():
            weekdayCallNumber[key] /= frequenciesWeekdays[key]

        weekdays, frequencies = zip(*weekdayCallNumber.items())

        freq_series = pd.Series(frequencies)

        ax = freq_series.plot(kind='bar', color='r', figsize=(30, 20), fontsize=25)
        plt.margins(y=0.2, tight=True)
        ax.set_title('Number of Calls Made in Each Weekday (on average)', fontsize=40)
        ax.set_xlabel('Weekdays', fontsize=30)
        ax.set_ylabel('Number of Calls', fontsize=30)
        ax.set_xticklabels(weekdays, rotation=0)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, frequencies):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, int(label), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: Call Activity (number of calls) in each weekday (on average) ------------------------")
        minCalls = np.min(frequencies)
        maxCalls = np.max(frequencies)
        indexMinWeekday = frequencies.index(minCalls)
        indexMaxWeekday = frequencies.index(maxCalls)
        print("The Weekday with less number of calls on Average (" + str(int(minCalls)) + ") was: " + str(weekdays[indexMinWeekday]))
        print("The Weekday with more number of calls on Average (" + str(int(maxCalls)) + ") was: " + str(weekdays[indexMaxWeekday]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """ -----------------------------------  Call Activity (number of calls) in each day throughout the year  ------------------------------------------------------------ """

        dates, frequencies = zip(*dateCallNumber.items())

        freq_series = pd.Series(frequencies)

        ax = freq_series.plot(kind='bar', color='g', figsize=(30, 20), fontsize=25)
        ax.set_title('Number of Calls in Each Day', fontsize=40)
        ax.set_xlabel('Days', fontsize=30)
        ax.set_ylabel('Number of Calls in Each Day', fontsize=30)
        ax.margins(y=0.2, tight=True)
        ax.set_xticklabels(dates, rotation=0)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, frequencies):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, int(label), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: Call Activity (number of calls) in each day throughout the year ------------------------")
        print("STATS OF Number of Calls BY DAY:")
        statistics = stats(frequencies)
        print(statistics)
        minCalls = np.min(frequencies)
        maxCalls = np.max(frequencies)
        indexMinDate = frequencies.index(minCalls)
        indexMaxDate = frequencies.index(maxCalls)
        print("The Day with less number of calls (" + str(int(minCalls)) + ") was: " + str(dates[indexMinDate]))
        print("The Date with more duration of the calls (" + str(int(maxCalls)) + ") was: " + str(dates[indexMaxDate]))
        print("-------------------------------------------------------------------------------------------------------------------------")

        """ -----------------------------------  Call Activity (number of calls) in each hour  ------------------------------------------------------------ """

        for key in frequenciesHours.keys():
            hoursCallNumber[key] /= frequenciesHours[key]

        hours, frequencies = zip(*hoursCallNumber.items())

        freq_series = pd.Series(frequencies)

        ax = freq_series.plot(kind='bar', color='y', figsize=(30, 20), fontsize=25)
        ax.set_title('Number of Calls in Each Hour ( on average)', fontsize=40)
        ax.set_xlabel('Hour', fontsize=30)
        ax.set_ylabel('Number of Calls in Each Hour', fontsize=30)
        ax.set_xticklabels(hours, rotation=0)
        ax.margins(y=0.2, tight=True)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, frequencies):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, int(label), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: Call Activity (number of calls) in each hour ------------------------")
        print("STATS OF NUMBER OF CALLS PER HOUR:")
        statistics = stats(frequencies)
        print(statistics)
        minHours = np.min(frequencies)
        maxHours = np.max(frequencies)
        indexMinHour = frequencies.index(minHours)
        indexMaxHour = frequencies.index(maxHours)
        print("The Hour with less number of calls (" + str(int(minHours)) + ") was: " + str(hours[indexMinHour]))
        print("The Hour with more number of calls (" + str(int(maxHours)) + ") was: " + str(hours[indexMaxHour]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """------  number of different visited cells (only calls) x number of subjects throughout the year ------ """

        numberDifferentPlacesByNumberOfUsers = collections.Counter(numberDifferentPlacesByUser.values()).most_common()
        numberDifferentPlaces, NumberOfUsersDifferentPlaces = zip(*numberDifferentPlacesByNumberOfUsers)
        
        fig = plt.figure()
        ax = plt.axes()
        plt.title("Different Visited Places Per Different Number of Users Throughout the Year")
        plt.xlabel("Number of Users")
        plt.ylabel("Number of Different Visited Places")
        ax.plot(NumberOfUsersDifferentPlaces, numberDifferentPlaces,'ro')
        plt.grid(True)
        plt.show()

        print("-------------------- STATISTICS: number of different visited cells (only calls) x number of subjects throughout the year ------------------------")
        print("STATS OF DIFFERENT VISITED PLACES BY EACH USER THROUGHOUT THE YEAR:")
        statistics = stats(NumberOfUsersDifferentPlaces)
        print(statistics)
        minNumberUsers = np.min(NumberOfUsersDifferentPlaces)
        maxNumberUsers = np.max(NumberOfUsersDifferentPlaces)
        minNumberPlaces = np.min(numberDifferentPlaces)
        maxNumberPlaces = np.max(numberDifferentPlaces)
        indexMinNumberPlaces = numberDifferentPlaces.index(minNumberPlaces)
        indexMaxNumberPlaces = numberDifferentPlaces.index(maxNumberPlaces)

        print("There are " + str(NumberOfUsersDifferentPlaces[indexMinNumberPlaces]) + " users that travelled through " + str(minNumberPlaces) + " different places throughout the year - the lowest number of different travelled places registered per user")
        print("There are " + str(NumberOfUsersDifferentPlaces[indexMaxNumberPlaces]) + " users that travelled through " + str(maxNumberPlaces) + " different places throughout the year - the highest number of different travelled places registered per user")
        print("-------------------------------------------------------------------------------------------------------------------------")

        """----------------------------------  Different Visited Places By Each User on Average in Each Month ------------------------------------------------------ """

        months, averageDifferentPlaces = zip(*numberDifferentPlacesByMonth.items())

        freq_series = pd.Series(averageDifferentPlaces)


        ax = freq_series.plot(kind='bar', color='b', figsize=(30, 20), fontsize=25)
        ax.set_title('Different Visited Places By Each User on Average in Each Month', fontsize=40)
        ax.set_xlabel('Months', fontsize=30)
        ax.set_ylabel('Different Visited Places By Each User on Average', fontsize=30)
        ax.set_xticklabels(months, rotation=0)

        plt.margins(y=0.2, tight=True)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, averageDifferentPlaces):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label,2), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- Different Visited Places By Each User on Average in Each Month ------------------------")
        print("STATS OF DIFFERENT VISITED PLACES BY EACH USER ON AVERAGE BY MONTH:")
        statistics = stats(averageDifferentPlaces)
        print(statistics)
        minDifferentPlaces = statistics["min"]
        maxDifferentPlaces = statistics["max"]
        indexMinMonth = averageDifferentPlaces.index(minDifferentPlaces)
        indexMaxMonth = averageDifferentPlaces.index(maxDifferentPlaces)
        print("The month with less number of different visited places on average by each user (" + str(minDifferentPlaces) + ") was: " + str(months[indexMinMonth]))
        print("The month with more number of different visited places on average by each user (" + str(maxDifferentPlaces) + ") was: " + str(months[indexMaxMonth]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """------  Different Visited Places By Each User on Average in Each Weekday (on average) ------ """

        weekdays, averageDifferentPlaces = zip(*numberDifferentPlacesByWeekday.items())

        freq_series = pd.Series(averageDifferentPlaces)

        ax = freq_series.plot(kind='bar', color='r', figsize=(30, 20), fontsize=25)
        plt.margins(y=0.2, tight=True)
        ax.set_title('Different Visited Places By Each User on Average in Each Weekday', fontsize=40)
        ax.set_xlabel('Weekdays', fontsize=30)
        ax.set_ylabel('Different Visited Places By Each User on Average', fontsize=30)
        ax.set_xticklabels(weekdays, rotation=0)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, averageDifferentPlaces):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label,2), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: Different Visited Places By Each User on Average in Each Weekday (on average) ------------------------")
        minDifferentPlaces = np.min(averageDifferentPlaces)
        maxDifferentPlaces = np.max(averageDifferentPlaces)
        indexMinWeekday = averageDifferentPlaces.index(minDifferentPlaces)
        indexMaxWeekday = averageDifferentPlaces.index(maxDifferentPlaces)
        print("The Weekday with less number of different visited places on average by each user (" + str(int(minDifferentPlaces)) + ") was: " + str(weekdays[indexMinWeekday]))
        print("The Weekday with more number of different visited places on average by each user (" + str(int(maxDifferentPlaces)) + ") was: " + str(weekdays[indexMaxWeekday]))
        print("-------------------------------------------------------------------------------------------------------------------------")

        """------  Different Visited Places By Each User on Average in Each Hour (on average) ------ """

        differentPlacesByHour = sorted(numberDifferentPlacesByHour.items())

        hours, averageDifferentPlaces = zip(*differentPlacesByHour)

        freq_series = pd.Series(averageDifferentPlaces)

        ax = freq_series.plot(kind='bar', color='y', figsize=(30, 20), fontsize=25)
        ax.set_title('Different Visited Places By Each User on Average in Each Hour', fontsize=40)
        ax.set_xlabel('Hour', fontsize=30)
        ax.set_ylabel('Different Visited Places By Each User on Average', fontsize=30)
        ax.set_xticklabels(hours, rotation=0)
        ax.margins(y=0.2, tight=True)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, averageDifferentPlaces):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label,2), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- STATISTICS: Different Visited Places By Each User on Average in Each Hour (on average) ------------------------")
        print("STATS OF DIFFERENT VISITED PLACES BY EACH USER ON AVERAGE PER HOUR:")
        statistics = stats(averageDifferentPlaces)
        print(statistics)
        minHours = np.min(averageDifferentPlaces)
        maxHours = np.max(averageDifferentPlaces)
        indexMinHour = averageDifferentPlaces.index(minHours)
        indexMaxHour = averageDifferentPlaces.index(maxHours)
        print("The Hour with less number of different visited places on average by each user (" + str(int(minHours)) + ") was: " + str(hours[indexMinHour]))
        print("The Hour with more number of different visited places on average by each user (" + str(int(maxHours)) + ") was: " + str(hours[indexMaxHour]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """ Range of Distances that cover all the different visited places (only people who call) x number of subjects throughout the year """

        distanceTravelledByUser = defaultdict(float)

        plausibleDifferentPlacesByUser = defaultdict(list)

        for user, differentPlaces in differentPlacesByUser.items():
            for index in range(len(differentPlaces)):
                if(len(coordsByCellIDs[differentPlaces[index]]) != 0):
                    plausibleDifferentPlacesByUser[user].append(differentPlacesByUser[user][index])

        for user, differentPlaces in plausibleDifferentPlacesByUser.items():
            for index in range(len(differentPlaces)):
                if index >= 1:
                    distanceTravelledByUser[user] += distanceInKmBetweenEarthCoordinates(coordsByCellIDs[differentPlaces[index-1]][0], coordsByCellIDs[differentPlaces[index-1]][1], coordsByCellIDs[differentPlaces[index]][0], coordsByCellIDs[differentPlaces[index]][1])
                else:
                    distanceTravelledByUser[user] += 0

        classes = [0, 5, 25, 100, 500, 1250, 2500]
        classNames = ["[0 to 5[",  "]5 to 25]", "]25 to 100]", "]100 to 500]", "]500, 1250]", "]1250, 2500]"]
        out = pd.cut(list(distanceTravelledByUser.values()), bins=classes, include_lowest=True)
        ax = out.value_counts().plot.bar(rot=0, color="g",  figsize=(30, 20), fontsize=25)
        ax.set_xticklabels(classNames)
        ax.margins(y=0.2, tight=True)
        plt.grid(True)
        rects = ax.patches

        values = []

        allInfo = str(pd.Categorical.value_counts(out))
        lines = allInfo.split("\n")

        for i in range(len(lines)-1):
            line = lines[i].split("]")
            values.append(int(line[1].strip()))

        for rect, label in zip(rects, values):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label, 2), ha='center', va='bottom', fontsize=19)


        plt.title("Range of Travels Distance", fontsize=40)
        plt.ylabel("Number of Users", fontsize=30)
        plt.xlabel("Range of Travells Distance (in Kms)", fontsize=30)
        plt.grid(True)
        plt.show()

        """----------------------------------- Range of Distances of all the different visited places By the Users on Average in each Month --------------------------------------------------"""

        distanceTravelledByMonth = defaultdict(float)

        plausibleDifferentPlacesByMonthByUser = defaultdict(dict)

        for i in set(allOriginatingIDs):
            plausibleDifferentPlacesByMonthByUser[i] = defaultdict(list)

        for user, dict1 in differentPlacesByMonthByUser.items():
            for month, differentPlaces in dict1.items():
                for place in differentPlaces:
                    if (len(coordsByCellIDs[place]) != 0):
                        plausibleDifferentPlacesByMonthByUser[user][month].append(place)

        for user, dict1 in plausibleDifferentPlacesByMonthByUser.items():
            for month, differentPlaces in dict1.items():
                for index, place in enumerate(differentPlaces):
                    if index >= 1:

                        distanceTravelledByMonth[month] += distanceInKmBetweenEarthCoordinates(coordsByCellIDs[differentPlaces[index-1]][0], coordsByCellIDs[differentPlaces[index-1]][1], coordsByCellIDs[differentPlaces[index]][0], coordsByCellIDs[differentPlaces[index]][1])
                    else:
                        distanceTravelledByMonth[month] += 0

        for i in distanceTravelledByMonth.keys():
            distanceTravelledByMonth[i] /= len(usersByMonth[i])

        months, averageDistanceTravelled = zip(*distanceTravelledByMonth.items())


        freq_series = pd.Series(averageDistanceTravelled)

        ax = freq_series.plot(kind='bar', color='b', figsize=(30, 20), fontsize=25)
        ax.set_title('Range of Distances Travelled By Each user on Average in Each Month' , fontsize=40)
        ax.set_xlabel('Months', fontsize=30)
        ax.set_ylabel('Range of Distances Travelled By Each user on Average (in Kms)', fontsize=30)
        ax.set_xticklabels(months, rotation=0)
        ax.margins(y=0.2, tight=True)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, averageDistanceTravelled):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label,2), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- Range of Distances of all the different visited places By the Users on Average in each Month ------------------------")
        print("STATS OF RANGE OF THE DIFFERENT VISITED PLACES BY EACH USER ON AVERAGE PER MONTH:")
        statistics = stats(averageDistanceTravelled)
        print(statistics)
        minDistance = np.min(averageDistanceTravelled)
        maxDistance = np.max(averageDistanceTravelled)
        indexMinDistance = averageDistanceTravelled.index(minDistance)
        indexMaxDistance = averageDistanceTravelled.index(maxDistance)
        
        print("The Month with a smaller range of travel on average by each user (" + str(minDistance) + " Kms) was: " + str(months[indexMinDistance]))
        print("The Month with a bigger range of travel on average by each user (" + str(maxDistance) + " Kms) was: " + str(months[indexMaxDistance]))
        print("-------------------------------------------------------------------------------------------------------------------------")

        """----------------------------------- Range of Distances of all the different visited places By Each user on Average in each Hour --------------------------------------------------"""

        distanceTravelledByHour = defaultdict(float)

        plausibleDifferentPlacesByHourByUser = defaultdict(dict)

        for i in set(allOriginatingIDs):
            plausibleDifferentPlacesByHourByUser[i] = defaultdict(list)

        for user, dict1 in differentPlacesByHourByUser.items():
            for month, differentPlaces in dict1.items():
                for place in differentPlaces:
                    if (len(coordsByCellIDs[place]) != 0):
                        plausibleDifferentPlacesByHourByUser[user][month].append(place)

        for user, dict1 in plausibleDifferentPlacesByHourByUser.items():
            for month, differentPlaces in dict1.items():
                for index, place in enumerate(differentPlaces):
                    if index >= 1:
                        distanceTravelledByHour[month] += distanceInKmBetweenEarthCoordinates(coordsByCellIDs[differentPlaces[index - 1]][0], coordsByCellIDs[differentPlaces[index - 1]][1], coordsByCellIDs[differentPlaces[index]][0], coordsByCellIDs[differentPlaces[index]][1])
                    else:
                        distanceTravelledByHour[month] += 0


        for i in distanceTravelledByHour.keys():
            distanceTravelledByHour[i] /= frequenciesHours[i]

        for i in distanceTravelledByHour.keys():
            distanceTravelledByHour[i] /= len(usersByHour[i])

        distanceTravelledByHour = sorted(distanceTravelledByHour.items())
        hours, distanceTravelled = zip(*distanceTravelledByHour)

        freq_series = pd.Series(distanceTravelled)

        ax = freq_series.plot(kind='bar', color='y', figsize=(30, 20), fontsize=25)
        ax.set_title('Range of Distances Travelled By Each user on Average in Each Hour', fontsize=40)
        ax.set_xlabel('Hours', fontsize=30)
        ax.set_ylabel('Range of Distances Travelled By Each user on Average (in Kms)', fontsize=30)
        ax.set_xticklabels(hours, rotation=0)
        ax.margins(y=0.2, tight=True)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, distanceTravelled):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label,2), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- Range of Distances of all the different visited places By Each user on Average in each Hour ------------------------")
        print("STATS OF RANGE OF THE DIFFERENT VISITED PLACES BY EACH USER ON AVERAGE PER HOUR:")
        statistics = stats(distanceTravelled)
        print(statistics)
        minDistance = np.min(distanceTravelled)
        maxDistance = np.max(distanceTravelled)
        indexMinHour = distanceTravelled.index(minDistance)
        indexMaxHour = distanceTravelled.index(maxDistance)
        print("The Hour with a smaller range of travel on average by each user (" + str(minDistance) + " Kms) was: " + str(hours[indexMinHour]))
        print("The Hour with a bigger range of travel on average by each user (" + str(maxDistance) + " Kms) was: " + str(hours[indexMaxHour]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """----------------------------------- Range of Distances of all the different visited places By Each user on Average in each Weekday --------------------------------------------------"""

        distanceTravelledByWeekday = defaultdict(float)

        plausibleDifferentPlacesByWeekdayByUser = defaultdict(dict)

        for i in set(allOriginatingIDs):
            plausibleDifferentPlacesByWeekdayByUser[i] = defaultdict(list)

        for user, dict1 in differentPlacesByWeekdayByUser.items():
            for month, differentPlaces in dict1.items():
                for place in differentPlaces:
                    if (len(coordsByCellIDs[place]) != 0):
                        plausibleDifferentPlacesByWeekdayByUser[user][month].append(place)

        for user, dict1 in plausibleDifferentPlacesByWeekdayByUser.items():
            for month, differentPlaces in dict1.items():
                for index, place in enumerate(differentPlaces):
                    if index >= 1:
                        distanceTravelledByWeekday[month] += distanceInKmBetweenEarthCoordinates(coordsByCellIDs[differentPlaces[index - 1]][0], coordsByCellIDs[differentPlaces[index - 1]][1], coordsByCellIDs[differentPlaces[index]][0], coordsByCellIDs[differentPlaces[index]][1])
                    else:
                        distanceTravelledByWeekday[month] += 0

        for i in distanceTravelledByWeekday.keys():
                distanceTravelledByWeekday[i] /= frequenciesWeekdays[i]

        for i in distanceTravelledByWeekday.keys():
            distanceTravelledByWeekday[i] /= len(usersByWeekday[i])

        distanceTravelledByWeekday = sorted(distanceTravelledByWeekday.items())
        weekdays, averageDistanceTravelled = zip(*distanceTravelledByWeekday)

        freq_series = pd.Series(averageDistanceTravelled)

        ax = freq_series.plot(kind='bar', color='r', figsize=(30, 20), fontsize=25)
        plt.margins(y=0.2, tight=True)
        ax.set_title('Range of Distances Travelled By Each user on Average in Each Weekday', fontsize=40)
        ax.set_xlabel('Weekdays', fontsize=30)
        ax.set_ylabel('Range of Distances Travelled By Each user on Average (in Kms)', fontsize=30)
        ax.set_xticklabels(weekdays, rotation=0)
        plt.grid(True)
        rects = ax.patches

        for rect, label in zip(rects, averageDistanceTravelled):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label, 2), ha='center', va='bottom', fontsize=19)

        plt.show()

        print("-------------------- Range of Distances of all the different visited places By Each user on Average in each Weekday ------------------------")
        print("STATS OF RANGE OF THE DIFFERENT VISITED PLACES BY EACH USER ON AVERAGE PER DAY:")
        statistics = stats(averageDistanceTravelled)
        print(statistics)
        minDifferentPlaces = np.min(averageDistanceTravelled)
        maxDifferentPlaces = np.max(averageDistanceTravelled)
        indexMinWeekday = averageDistanceTravelled.index(minDifferentPlaces)
        indexMaxWeekday = averageDistanceTravelled.index(maxDifferentPlaces)
        print("The Weekday with a smaller range of travel on average by each user (" + str(minDifferentPlaces) + " Kms) was: " + str(weekdays[indexMinWeekday]))
        print("The Weekday with a bigger range of travel on average by each user (" + str(maxDifferentPlaces) + " Kms) was: " + str(weekdays[indexMaxWeekday]))
        print("-------------------------------------------------------------------------------------------------------------------------")


        """----------------------------------- Average Distance between receivers and callers throughout the year -----------------------------------------"""

        averageDistancesByUser = defaultdict(float)

        for user, cellIDPairs in cellIDsPairsByUser.items():
            for index, cellIDPair in enumerate(cellIDPairs):

                originatingCoords = coordsByCellIDs[cellIDPair[0]]
                terminatingCoords = coordsByCellIDs[cellIDPair[1]]

                averageDistancesByUser[user] += distanceInKmBetweenEarthCoordinates(originatingCoords[0], originatingCoords[1], terminatingCoords[0], terminatingCoords[1])

            averageDistancesByUser[user] /= len(cellIDPairs)

        averageDistancesByUser = list(averageDistancesByUser.values())
        classes = [0, 5, 25, 100, 500, 1250, 2500]
        classNames = ["[0 to 5[", "]5 to 25]", "]25 to 100]", "]100 to 500]", "]500, 1250]", "]1250, 2500]"]
        out = pd.cut(averageDistancesByUser, bins=classes, include_lowest=True)
        ax = out.value_counts().plot.bar(rot=0, color="g",  figsize=(30, 20), fontsize=25)
        ax.set_xticklabels(classNames)

        rects = ax.patches
        values = []

        allInfo = str(pd.Categorical.value_counts(out))
        lines = allInfo.split("\n")

        for i in range(len(lines)-1):
            line = lines[i].split("]")
            values.append(int(line[1].strip()))

        for rect, label in zip(rects, values):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, round(label, 2), ha='center', va='bottom', fontsize=19)

        rects = ax.patches
        plt.title("Average Distance between the Callers and Receivers", fontsize=40)
        plt.xlabel("Average Distance (in Kms)", fontsize=30)
        plt.ylabel("Number of Users", fontsize=30)
        plt.grid(True)
        plt.show()
        
        elapsed_time = time.time() - start_time
        print(str(elapsed_time/60) + "minutes")
        

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
