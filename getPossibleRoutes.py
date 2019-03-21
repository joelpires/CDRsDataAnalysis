"""
Exploratory Analysis Tools for CDR Dataset
March 2019
Joel Pires
"""
__author__ = 'Joel Pires'
__date__ = 'March 2019'

import time
import os
import urllib, json
from glob import glob
import csv



import psycopg2
import configparser
import datetime
import numpy as np
import scipy.stats as st
import polyline
from collections import defaultdict
import arcpy


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


def calculate_routes(origin, destination):





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

        cur.execute('SELECT * FROM public.ODPorto_users_characterization')

        fetchedODPorto_users = cur.fetchall()

        key1 = os.environ.get('MAPSAPIKEYJO')
        key2 = os.environ.get('MAPSAPIMA')
        chosenkey = key1

        roadsAPIendpoint = 'https://roads.googleapis.com/v1/nearestRoads?'
        directionsAPIendpoint = 'https://maps.googleapis.com/maps/api/directions/json?'

        mobilityByUsers = defaultdict(dict)

        countRequestsDirections = 0

        #DIRECTIONS API
        for i in range(len(fetchedODPorto_users)):
            routeNumber = 0
            if (countRequestsDirections >= 59000):
                return

            userID = str(fetchedODPorto_users[i][0])
            home_location = str(fetchedODPorto_users[i][12]) + "," + str(fetchedODPorto_users[i][13])
            work_location = str(fetchedODPorto_users[i][15]) + "," + str(fetchedODPorto_users[i][16])
            min_traveltime_h_w = str(fetchedODPorto_users[i][19])
            min_traveltime_w_h = str(fetchedODPorto_users[i][25])

            if(min_traveltime_h_w == "None"):
                H_W = 0
            if (min_traveltime_w_h == "None"):
                W_H = 0

            mobilityByUsers[userID]['duration'] = 0
            mobilityByUsers[userID]['transport_modes'] = list()
            mobilityByUsers[userID]['routes'] = list()




            if (H_W == 1):
                calculate_routes(home_location, work_location)

            if (W_H == 1):
                calculate_routes(work_location, home_location)

            """
            travel_modes = ["DRIVING"]#,"BICYCLING", "WALKING", "TRANSIT", "MULTIMODE"]
            multimode = False
            for mode in travel_modes:
                if(mode == "MULTIMODE"):
                    request = directionsAPIendpoint + 'origin={}&destination={}&alternatives=true&key={}'.format(home_location, work_location, chosenkey)
                    multimode = True
                else:
                    request = directionsAPIendpoint + 'origin={}&destination={}&mode={}&alternatives=true&key={}'.format(home_location, work_location, mode, chosenkey)


                response = json.loads(urllib.urlopen(request).read())
                countRequestsDirections += 1

                #pode nao ter resposta
                if response['status'] == 'OK':
                    for route in response['routes']:
                        calculated_route = analyzeLegs(mode, route, mobilityByUsers, userID, multimode)
                        if calculated_route:
                            

            """

            calculated_route = [(41.15641, -8.64247), (41.15635, -8.64219), (41.15626, -8.64171), (41.15626, -8.64171),
                (41.15411, -8.64243), (41.15411, -8.64243), (41.15452, -8.6438), (41.15454, -8.6439),
                (41.15454, -8.64402), (41.15434, -8.64586), (41.15436, -8.64593), (41.15436, -8.64599),
                (41.15431, -8.64661), (41.1543, -8.64688), (41.1543, -8.64721), (41.15429, -8.64752),
                (41.15429, -8.6478), (41.15427, -8.64828), (41.15427, -8.64828), (41.15442, -8.64818),
                (41.15472, -8.64795), (41.15472, -8.64795), (41.15495, -8.648), (41.15535, -8.6482)]
            mobilityByUsers[userID]['transport_modes'].append("DRIVING")

            filename1 = str(userID) + "_" + str(mobilityByUsers[userID]['transport_modes'][routeNumber]) + "_non_interpolated_route_points_" + str(routeNumber)
            # convert the points to .csv files
            path_csvs = "C:\Users\Joel\Documents\ArcGIS\\non_interpolated_route_points_csvs\\"
            with open(path_csvs + filename1 + ".csv", mode='w') as fp:
                fp.write("latitude, longitude")
                fp.write("\n")
                for point in calculated_route:
                    line = str(point[0]) + "," + str(point[1])
                    fp.write(line)
                    fp.write("\n")

            # convert the shapefile to layer
            arcpy.MakeXYEventLayer_management(path_csvs + filename1 + ".csv", "longitude", "latitude", filename1 + "_Layer", arcpy.SpatialReference("WGS 1984"))

            #convert the points to shapefile
            path_shapefile1 = "C:/Users/Joel/Documents/ArcGIS/non_interpolated_route_points_shapefiles/"
            arcpy.FeatureClassToFeatureClass_conversion(filename1 + "_Layer", path_shapefile1, filename1)


            # Execute PointsToLine
            filename2 = str(userID) + "_" + str(mobilityByUsers[userID]['transport_modes'][routeNumber]) + "_route_line_" + str(routeNumber)
            path_shapefile2 = "C:/Users/Joel/Documents/ArcGIS/route_lines_shapefiles/"
            arcpy.PointsToLine_management(path_shapefile1 + filename1 + ".shp", path_shapefile2 + filename2)


            # interpolate the points
            filename3 = str(userID) + "_" + str(mobilityByUsers[userID]['transport_modes'][routeNumber]) + "_interpolated_route_points_" + str(routeNumber)
            path_shapefile3 = "C:/Users/Joel/Documents/ArcGIS/interpolated_route_points_shapefiles/"
            arcpy.GeneratePointsAlongLines_management(path_shapefile2 + filename2 + ".shp", path_shapefile3 + filename3 + ".shp", 'PERCENTAGE', Percentage=2, Include_End_Points='END_POINTS')

            # convert the shapefile to layer
            layer = arcpy.MakeFeatureLayer_management(path_shapefile3 + filename3 + ".shp", filename3)

            #convert layer to points
            fld_list = arcpy.ListFields(layer)
            fld_names = [fld.name for fld in fld_list]
            cursor = arcpy.da.SearchCursor(layer, fld_names)

            interpolated_route = []
            for row in cursor:
                interpolated_route.append((row[1][1], row[1][0]))

            #guardar route no mobilityByUsers[userID]
            mobilityByUsers[userID]['routes'].append(interpolated_route)



            #CREATE TABLE mobilityByUsers

            for user in interpolated_route:
                query = "INSERT INTO " + city + " (userID, H_W, routeNumber, duration, transportMode, latitude, longitude) VALUES (" userID, H_W, ")"
                cur.execute(query)


            routeNumber += 1




        print("NUMBER OF REQUESTS TO DIRECTIONS API: " + str(countRequestsDirections))

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