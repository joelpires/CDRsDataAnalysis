"""
Exploratory Analysis Tools for CDR Dataset
January 2019
Joel Pires
"""

__author__ = 'Joel Pires'
__date__ = 'January 2019'

import psycopg2
import configparser
from collections import defaultdict
import datetime
import operator
import collections
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy import stats

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
        else:
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

        cur.execute('SELECT originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, duration_amt FROM public.call_fct WHERE duration_amt != -1 ORDER BY date_id LIMIT 5000')

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
        for index, val in enumerate(allOriginatingIDs):

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

        for tuple in zipped:

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


            if previousUser != user:
                for i in numberDifferentPlacesByWeekday.keys():
                    numberDifferentPlacesByWeekday[i] /= frequenciesWeekdays[i]
                for i in numberDifferentPlacesByHour.keys():
                    numberDifferentPlacesByHour[i] /= frequenciesHours[i]

            previousUser = user


        for i in numberDifferentPlacesByMonth.keys():
            numberDifferentPlacesByMonth[i] /= len(usersByMonth[i])

        for i in numberDifferentPlacesByWeekday.keys():
            numberDifferentPlacesByWeekday[i] /= len(usersByWeekday[i])

        for i in numberDifferentPlacesByHour.keys():
            numberDifferentPlacesByHour[i] /= len(usersByHour[i])


        """ --------------------------------  calls activity (calls) x duration of the calls throughout the year ------------------------------- """
        """
        durationsMinutes = [round(x / 60) for x in allDurations]
        frequenciesOfCallsByUser = dict(collections.Counter(durationsMinutes))
        differentDurations, NumberOfUsers = zip(*frequenciesOfCallsByUser.items())

        fig = plt.figure()
        ax = plt.axes()
        plt.title("Call Frequency per Different Durations Throughout the Year")
        plt.xlabel("Duration of the Calls (in minutes)")
        plt.ylabel("Number of Calls")
        ax.plot(differentDurations, NumberOfUsers,'rx')
        plt.grid(True)
        plt.show()
        """

        """ --------------------------------  number of different active users  (calls) x calls activity throughout the year ------------------------------- """
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

        """ -----------------------------------  number of different active users in each month  ------------------------------------------------------------ """
        """
        months = []
        frequencies = []
        for key, value in originatingIDsByMonths.items():
            months.append(key)
            frequencies.append(len(value))

        y_pos = np.arange(len(months))
        plt.bar(y_pos, frequencies, align='center', alpha=0.5)
        plt.xticks(y_pos, months)
        plt.title("Number of Active Users in Each Month")
        plt.ylabel("Number of Users")
        plt.xlabel("Months")
        plt.grid(True)
        plt.show()

        """
        """ -------------------------------------  number of different active users in each weekday (on average) -------------------------------------------- """
        """
        for key in frequenciesWeekdays.keys():
            numberUsersByWeekday[key] /= frequenciesWeekdays[key]

        weekdays, frequencies = zip(*numberUsersByWeekday.items())
        weekdays = weekdays[1:]
        frequencies = frequencies[1:]

        y_pos = np.arange(len(weekdays))
        plt.bar(y_pos, frequencies, align='center', alpha=0.5)
        plt.xticks(y_pos, weekdays)
        plt.title("Number of Active Users in Each Weekday")
        plt.ylabel("Number of Users")
        plt.xlabel("Weekday")
        plt.grid(True)
        plt.show()
        """

        """ --------------------------------------------  number of different active users in each day throughout the year ---------------------------------- """
        """
        for key in originatingIDsByDate.keys():
            originatingIDsByDate[key] = len(originatingIDsByDate[key])

        dates, frequencies = zip(*originatingIDsByDate.items())
        y_pos = np.arange(len(list(dates)))
        plt.bar(y_pos, frequencies, align='center', alpha=0.5)

        plt.xticks(y_pos, dates)
        plt.title("Number of Active Users in Each Day throughout the year")
        plt.ylabel("Number of Users")
        plt.xlabel("Days")
        plt.grid(True)
        plt.show()
        """

        """ -----------------------------------------------  number of different active users in each hour  (on average) ------------------------------------ """
        """
        for key in frequenciesHours.keys():
            numberUsersByHour[key] /= frequenciesHours[key]


        hours, frequencies = zip(*numberUsersByHour.items())
        hours = hours[1:]
        frequencies = frequencies[1:]

        y_pos = np.arange(len(hours))
        plt.bar(y_pos, frequencies, align='center', alpha=0.5)
        plt.xticks(y_pos, hours)
        plt.title("Number of Active Users in Each Hour of the Day (on average)")
        plt.ylabel("Number of Users")
        plt.xlabel("Hour")
        plt.grid(True)
        plt.show()
        """

        """ ----------------------------------------------- number of different active users (calls) x duration of the calls throughout the year ------------ """
        """
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

        """ ---------------------------------------------- duration of the calls in each month  ------------------------------------------------------------- """
        """
        months, durations = zip(*monthDurationsDict.items())

        y_pos = np.arange(len(list(months)))
        plt.bar(y_pos, durations, align='center', alpha=0.5)

        plt.xticks(y_pos, months)
        plt.title("Duration of the Calls in Each Month")
        plt.ylabel("Duration of the Calls (in minutes)")
        plt.xlabel("months")
        plt.grid(True)
        plt.show()
        """

        """ --------------------------------------------  duration of the calls  in each weekday (on average) ----------------------------------------------- """
        """
        for key in frequenciesWeekdays.keys():
            weekdaysDurationsDict[key] /= frequenciesWeekdays[key]

        weekdays, durations = zip(*weekdaysDurationsDict.items())
        
        y_pos = np.arange(len(list(weekdays)))
        plt.bar(y_pos, durations, align='center', alpha=0.5)

        plt.xticks(y_pos, weekdays)
        plt.title("Duration of the Calls in Each Weekday (on average)")
        plt.ylabel("Duration of the Calls (in minutes)")
        plt.xlabel("Weekdays")
        plt.grid(True)
        plt.show()
        """

        """ --------------------------------------------  duration of the calls in each day throughout the year - --------------------------------- """
        """
        dates, durations = zip(*dateDurationsDict.items())

        y_pos = np.arange(len(list(dates)))
        plt.bar(y_pos, durations, align='center', alpha=0.5)

        plt.xticks(y_pos, dates)
        plt.title("Duration of the Calls in Each Day")
        plt.ylabel("Duration of the Calls (in minutes)")
        plt.xlabel("Days")
        plt.grid(True)
        plt.show()
        """
        """ --------------------------------------------  duration of the calls in each hour ------------------------------------------------------ """
        """
        for key in frequenciesHours.keys():
            hoursDurationsDict[key] /= frequenciesHours[key]

        hours, durations = zip(*hoursDurationsDict.items())

        y_pos = np.arange(len(list(hours)))
        plt.bar(y_pos, durations, align='center', alpha=0.5)

        plt.xticks(y_pos, hours)
        plt.title("Duration of the Calls in Each Hour (average)")
        plt.ylabel("Duration of the Calls (in minutes)")
        plt.xlabel("Hours")
        plt.grid(True)
        plt.show()
        """

        """ -----------------------------------  Call Activity (number of calls) in each month  ------------------------------------------------------------ """
        """
        months, frequencies = zip(*monthCallNumber.items())

        y_pos = np.arange(len(list(months)))
        plt.bar(y_pos, frequencies, align='center', alpha=0.5)

        plt.xticks(y_pos, months)
        plt.title("Number of Calls Made in Each Month")
        plt.ylabel("Number of Calls")
        plt.xlabel("Months")
        plt.grid(True)
        plt.show()
        """

        """ -----------------------------------  Call Activity (number of calls) in each weekday (on average)  ------------------------------------------------------------ """
        """
        for key in frequenciesWeekdays.keys():
            weekdayCallNumber[key] /= frequenciesWeekdays[key]


        weekday, frequencies = zip(*weekdayCallNumber.items())

        y_pos = np.arange(len(list(weekday)))
        plt.bar(y_pos, frequencies, align='center', alpha=0.5)

        plt.xticks(y_pos, weekday)
        plt.title("Number of Calls Made in Each Weekday (on average)")
        plt.ylabel("Number of Calls")
        plt.xlabel("Weekday")
        plt.grid(True)
        plt.show()
        """

        """ -----------------------------------  Call Activity (number of calls) in each day throughout the year  ------------------------------------------------------------ """
        """
        dates, frequencies = zip(*dateCallNumber.items())

        y_pos = np.arange(len(list(dates)))
        plt.bar(y_pos, frequencies, align='center', alpha=0.5)

        plt.xticks(y_pos, dates)
        plt.title("Number of Calls in Each Day")
        plt.ylabel("Number of Calls in Each Day")
        plt.xlabel("Days")
        plt.grid(True)
        plt.show()
        """

        """ -----------------------------------  Call Activity (number of calls) in each hour  ------------------------------------------------------------ """
        """
        for key in frequenciesHours.keys():
            hoursCallNumber[key] /= frequenciesHours[key]

        hours, frequencies = zip(*hoursCallNumber.items())

        y_pos = np.arange(len(list(hours)))
        plt.bar(y_pos, frequencies, align='center', alpha=0.5)

        plt.xticks(y_pos, hours)
        plt.title("Number of Calls in Each Hour ( on average)")
        plt.ylabel("Number of Calls in Each Hour")
        plt.xlabel("Hours")
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

        """----------------------------------  Different Visited Places By Each User on Average in Each Month ------------------------------------------------------ """
        """
        months, averageDifferentPlaces = zip(*numberDifferentPlacesByMonth.items())

        y_pos = np.arange(len(list(months)))
        plt.bar(y_pos, averageDifferentPlaces, align='center', alpha=0.5)

        plt.xticks(y_pos, months)
        plt.title("Different Visited Places By Each User on Average in Each Month")
        plt.ylabel("Different Visited Places By Each User on Average")
        plt.xlabel("Months")
        plt.grid(True)
        plt.show()
        """

        """------  Different Visited Places By Each User on Average in Each Weekday (on average) ------ """
        """
        weekdays, averageDifferentPlaces = zip(*numberDifferentPlacesByWeekday.items())

        y_pos = np.arange(len(list(weekdays)))
        plt.bar(y_pos, averageDifferentPlaces, align='center', alpha=0.5)

        plt.xticks(y_pos, weekdays)
        plt.title("Different Visited Places By Each User on Average in Each Weekday")
        plt.ylabel("Different Visited Places By Each User on Average")
        plt.xlabel("Weekday")
        plt.grid(True)
        plt.show()
        """

        """------  Different Visited Places By Each User on Average in Each Hour (on average) ------ """
        """
        differentPlacesByHour = sorted(numberDifferentPlacesByHour.items())

        hours, averageDifferentPlaces = zip(*differentPlacesByHour)

        y_pos = np.arange(len(list(hours)))
        plt.bar(y_pos, averageDifferentPlaces, align='center', alpha=0.5)

        plt.xticks(y_pos, hours)
        plt.title("Different Visited Places By Each User on Average in Each Hour")
        plt.ylabel("Different Visited Places By Each User on Average")
        plt.xlabel("Hour")
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

        """----------------------------------- Range of Distances of all the different visited places By the Users on Average in each Month --------------------------------------------------"""
        """
        distanceTravelledByMonth = defaultdict(float)

        for user, val in differentPlacesByMonthByUser.items():
            for month, val2 in differentPlacesByMonthByUser[user].items():
                for index, val3 in enumerate(differentPlacesByMonthByUser[user][month]):
                    differentPlaces = differentPlacesByMonthByUser[user][month]
                    if index >= 1:
                        distanceTravelledByMonth[month] += distanceInKmBetweenEarthCoordinates(coordsByCellIDs[differentPlaces[index-1]][0], coordsByCellIDs[differentPlaces[index-1]][1], coordsByCellIDs[differentPlaces[index]][0], coordsByCellIDs[differentPlaces[index]][1])
                    else:
                        distanceTravelledByMonth[month] += 0

        for i in distanceTravelledByMonth.keys():
            distanceTravelledByMonth[i] /= len(usersByMonth[i])

        months, averageDistanceTravelled = zip(*distanceTravelledByMonth.items())

        y_pos = np.arange(len(list(months)))
        plt.bar(y_pos, averageDistanceTravelled, align='center', alpha=0.5)

        plt.xticks(y_pos, months)
        plt.title("Range of Distances Travelled By Each user on Average in Each Month")
        plt.ylabel("Range of Distances Travelled By Each user on Average (in Kms)")
        plt.xlabel("Months")
        plt.grid(True)
        plt.show()
        """

        """----------------------------------- Range of Distances of all the different visited places By Each user on Average in each Hour --------------------------------------------------"""
        """
        distanceTravelledByHour = defaultdict(float)

        for user, val in differentPlacesByHourByUser.items():
            for hour, val2 in differentPlacesByHourByUser[user].items():
                for index, val3 in enumerate(differentPlacesByHourByUser[user][hour]):
                    differentPlaces = differentPlacesByHourByUser[user][hour]
                    if index >= 1:
                        distanceTravelledByHour[hour] += distanceInKmBetweenEarthCoordinates(coordsByCellIDs[differentPlaces[index-1]][0], coordsByCellIDs[differentPlaces[index-1]][1], coordsByCellIDs[differentPlaces[index]][0], coordsByCellIDs[differentPlaces[index]][1])
                    else:
                        distanceTravelledByHour[hour] += 0

            for i in distanceTravelledByHour.keys():
                distanceTravelledByHour[i] /= frequenciesHours[i]

        for i in distanceTravelledByHour.keys():
            distanceTravelledByHour[i] /= len(usersByHour[i])

        distanceTravelledByHour = sorted(distanceTravelledByHour.items())
        hours, averageDistanceTravelled = zip(*distanceTravelledByHour)

        y_pos = np.arange(len(list(hours)))
        plt.bar(y_pos, averageDistanceTravelled, align='center', alpha=0.5)

        plt.xticks(y_pos, hours)
        plt.title("Range of Distances Travelled By Each user on Average in Each Hour")
        plt.ylabel("Range of Distances Travelled By Each user on Average (in Kms)")
        plt.xlabel("Hours")
        plt.grid(True)
        plt.show()
        """

        """----------------------------------- Range of Distances of all the different visited places By Each user on Average in each Weekday --------------------------------------------------"""
        """
        distanceTravelledByWeekday = defaultdict(float)

        for user, val in differentPlacesByWeekdayByUser.items():
            for weekday, val2 in differentPlacesByWeekdayByUser[user].items():
                for index, val3 in enumerate(differentPlacesByWeekdayByUser[user][weekday]):
                    differentPlaces = differentPlacesByWeekdayByUser[user][weekday]
                    if index >= 1:
                        distanceTravelledByWeekday[weekday] += distanceInKmBetweenEarthCoordinates(coordsByCellIDs[differentPlaces[index-1]][0], coordsByCellIDs[differentPlaces[index-1]][1], coordsByCellIDs[differentPlaces[index]][0], coordsByCellIDs[differentPlaces[index]][1])
                    else:
                        distanceTravelledByWeekday[weekday] += 0

            for i in distanceTravelledByWeekday.keys():
                distanceTravelledByWeekday[i] /= frequenciesWeekdays[i]

        for i in distanceTravelledByWeekday.keys():
            distanceTravelledByWeekday[i] /= len(usersByWeekday[i])

        distanceTravelledByWeekday = sorted(distanceTravelledByWeekday.items())
        weekdays, averageDistanceTravelled = zip(*distanceTravelledByWeekday)

        y_pos = np.arange(len(list(weekdays)))
        plt.bar(y_pos, averageDistanceTravelled, align='center', alpha=0.5)

        plt.xticks(y_pos, weekdays)
        plt.title("Range of Distances Travelled By Each user on Average in Each Hour")
        plt.ylabel("Range of Distances Travelled By Each user on Average (in Kms)")
        plt.xlabel("Weekdays")
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
