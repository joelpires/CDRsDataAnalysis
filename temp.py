"""
Exploratory Analysis Tools for CDR Dataset
January 2019
Joel Pires
"""
import time
import os
import urllib.request, json

__author__ = 'Joel Pires'
__date__ = 'January 2019'

import psycopg2
import configparser
import datetime
import numpy as np
import scipy.stats as st
import polyline
from collections import defaultdict

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


def analyzeLegs(mode, route, mobilityByUsers, userID, multimode):
    differentModes = []
    routePoints = []
    for leg in route['legs']:

        mobilityByUsers[userID]['duration'] = leg['duration']['value']

        previousMode = route['legs'][0]['steps'][0]['travel_mode']

        if mode == "TRANSIT" and previousMode == "TRANSIT":
            previousMode = route['legs'][0]['steps'][0]['transit_details']['line']['vehicles']['type']

        for step in leg['steps']:

            if mode == "TRANSIT" and step['travel_mode'] == "TRANSIT":
                atualMode = step['transit_details']['line']['vehicles']['type']
            else:
                atualMode = step['travel_mode']


            if (previousMode != atualMode or step['travel_mode'] != mode) and multimode is False:
                return []
            else:
                for j in polyline.decode(step['polyline']['points']):
                    routePoints.append(j)

            differentModes.append(previousMode)


            if mode == "TRANSIT" and step['travel_mode'] == "TRANSIT":
                previousMode = step['transit_details']['line']['vehicles']['type']
            else:
                previousMode = step['travel_mode']

        if (multimode is True and differentModes[1:] != differentModes[:-1]):
            mobilityByUsers[userID]['transport_modes'].append(set(differentModes))
            return routePoints
        elif (multimode is False and differentModes[1:] == differentModes[:-1]):
            mobilityByUsers[userID]['transport_modes'].append(previousMode)
            return routePoints
        else:
            return []




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

        cur.execute('SELECT * FROM public.ODPorto_users_characterization LIMIT 1')

        fetchedODPorto_users = cur.fetchall()

        key1 = os.environ.get('MAPSAPIKEYJO')
        key2 = os.environ.get('MAPSAPIMA')
        chosenkey = key1

        roadsAPIendpoint = 'https://roads.googleapis.com/v1/snapToRoads?'
        directionsAPIendpoint = 'https://maps.googleapis.com/maps/api/directions/json?'

        mobilityByUsers = defaultdict(dict)


        countRequests = 0
        #DIRECTIONS API
        for i in range(len(fetchedODPorto_users)):

            userID = str(fetchedODPorto_users[i][0])
            home_location = "" + str(fetchedODPorto_users[i][12]) + "," + str(fetchedODPorto_users[i][13])
            work_location = "" + str(fetchedODPorto_users[i][15]) + "," + str(fetchedODPorto_users[i][16])
            min_traveltime_h_w = "" + str(fetchedODPorto_users[i][19])
            min_traveltime_w_h = "" + str(fetchedODPorto_users[i][25])
            mobilityByUsers[userID]['duration'] = 0
            mobilityByUsers[userID]['transport_modes'] = list()
            mobilityByUsers[userID]['routes'] = list()


            travel_modes = ["MULTIMODE"] #, "BICYCLING", "WALKING", "TRANSIT", "MULTIMODE
            multimode = False

            for mode in travel_modes:
                if(mode == "MULTIMODE"):
                    request = directionsAPIendpoint + 'origin={}&destination={}&alternatives=true&key={}'.format(home_location, work_location, chosenkey)
                    multimode = True
                else:
                    request = directionsAPIendpoint + 'origin={}&destination={}&mode={}&alternatives=true&key={}'.format(home_location, work_location, mode, chosenkey)
                request = "https://maps.googleapis.com/maps/api/directions/json?&mode=transit&origin=frontera+el+hierro&destination=la+restinga+el+hierro&alternatives=true&key=AIzaSyD1uL38USx9YBdzVKxw5GuCeOqY-2Xhj3Q"
                response = json.loads(urllib.request.urlopen(request).read())

                #pode nao ter resposta
                if response['status'] == 'OK':
                    for route in response['routes']:
                        calculated_route = analyzeLegs(mode, route, mobilityByUsers, userID, multimode)

                        if calculated_route:
                            mobilityByUsers[userID]['routes'].append(calculated_route)


            countRequests += 4





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
