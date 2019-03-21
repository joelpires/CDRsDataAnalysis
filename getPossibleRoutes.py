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


countRequests = 0
cur = None


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


def calculate_routes(origin, destination, city, userID):
    global countRequests
    global cur

    routeNumber = 0

    key1 = os.environ.get('MAPSAPIKEYJO')
    key2 = os.environ.get('MAPSAPIMA')
    chosenkey = key1

    directionsAPIendpoint = 'https://maps.googleapis.com/maps/api/directions/json?'

    travel_modes = ["DRIVING","BICYCLING", "WALKING", "TRANSIT", "MULTIMODE"]
    multimode = False
    for mode in travel_modes:
        if (countRequests >= 59000):  #budget limit for each google account
            return

        if (mode == "MULTIMODE"):
            request = directionsAPIendpoint + 'origin={}&destination={}&alternatives=true&key={}'.format(origin,destination,chosenkey)
            multimode = True
        else:
            request = directionsAPIendpoint + 'origin={}&destination={}&mode={}&alternatives=true&key={}'.format(origin, destination, mode, chosenkey)

        response = json.loads(urllib.urlopen(request).read())
        countRequests += 1

        # pode nao ter resposta
        if response['status'] == 'OK':
            for route in response['routes']:
                mobilityUser = analyzeLegs(mode, route, multimode)
                if mobilityUser['route']:
                    interpolated_route = interpolate(mobilityUser['route'], city, userID)

                    for point in interpolated_route:
                        query = "INSERT INTO " + city + "_possible_routes (userID, H_W, routeNumber, duration, transportMode, latitude, longitude) VALUES ("userID, H_W, ")"
                        cur.execute(query)

                    routeNumber += 1


def analyzeLegs(mode, route, multimode):
    differentModes = []
    routePoints = []
    mobilityUser = {}

    for leg in route['legs']:

        mobilityUser['duration'] = leg['duration']['value']

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
            mobilityUser['transport_modes'] = set(differentModes)
            mobilityUser['route'] = routePoints
            return mobilityUser

        elif (multimode is False and differentModes[1:] == differentModes[:-1]):
            mobilityUser['transport_modes'] = previousMode
            mobilityUser['route'] = routePoints
            return mobilityUser

        else:
            return []



def interpolate(route, city, userID):
    # convert the points to .csv files
    filename1 = str(userID) + "_" + str(mobilityByUsers[userID]['transport_modes'][routeNumber]) + "_non_interpolated_route_points_" + str(routeNumber)
    path_csvs = "C:\Users\Joel\Documents\ArcGIS\\non_interpolated_route_points_csvs\\"
    with open(path_csvs + filename1 + ".csv", mode='w') as fp:
        fp.write("latitude, longitude")
        fp.write("\n")
        for point in route:
            line = str(point[0]) + "," + str(point[1])
            fp.write(line)
            fp.write("\n")

    # convert the shapefile to layer
    arcpy.MakeXYEventLayer_management(path_csvs + filename1 + ".csv",
                                      "longitude",
                                      "latitude",
                                      filename1 + "_Layer",
                                      arcpy.SpatialReference("WGS 1984"))

    # convert the points to shapefile
    path_shapefile1 = "C:/Users/Joel/Documents/ArcGIS/non_interpolated_route_points_shapefiles/"
    arcpy.FeatureClassToFeatureClass_conversion(filename1 + "_Layer",
                                                path_shapefile1,
                                                filename1)

    # Execute PointsToLine
    filename2 = str(userID) + "_" + str(mobilityByUsers[userID]['transport_modes'][routeNumber]) + "_route_line_" + str(routeNumber)
    path_shapefile2 = "C:/Users/Joel/Documents/ArcGIS/route_lines_shapefiles/"
    arcpy.PointsToLine_management(path_shapefile1 + filename1 + ".shp",
                                  path_shapefile2 + filename2)

    # interpolate the points
    filename3 = str(userID) + "_" + str(mobilityByUsers[userID]['transport_modes'][routeNumber]) + "_interpolated_route_points_" + str(routeNumber)
    path_shapefile3 = "C:/Users/Joel/Documents/ArcGIS/interpolated_route_points_shapefiles/"
    arcpy.GeneratePointsAlongLines_management(path_shapefile2 + filename2 + ".shp",
                                              path_shapefile3 + filename3 + ".shp",
                                              'PERCENTAGE',
                                              Percentage=2,
                                              Include_End_Points='END_POINTS')

    # convert the shapefile to layer
    layer = arcpy.MakeFeatureLayer_management(path_shapefile3 + filename3 + ".shp",
                                              filename3)

    # convert layer to points
    fld_list = arcpy.ListFields(layer)
    fld_names = [fld.name for fld in fld_list]
    cursor = arcpy.da.SearchCursor(layer, fld_names)

    interpolated_route = []
    for row in cursor:
        interpolated_route.append((row[1][1], row[1][0]))

    return interpolated_route


""" Connect to the PostgreSQL database server """
def connect():
    global countRequests
    global cur

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


        cities = ["porto"]#, "lisbon", "coimbra"]

        for city in cities:
            query = "SELECT * FROM public.OD" + city + "_users_characterization LIMIT 1"
            cur.execute(query)

            fetched_users = cur.fetchall()

            #DIRECTIONS API
            for i in range(len(fetched_users)):
                H_W = W_H = 1

                userID = str(fetched_users[i][0])
                home_location = str(fetched_users[i][12]) + "," + str(fetched_users[i][13])
                work_location = str(fetched_users[i][15]) + "," + str(fetched_users[i][16])
                min_traveltime_h_w = str(fetched_users[i][19])
                min_traveltime_w_h = str(fetched_users[i][25])

                if(min_traveltime_h_w == "None"):
                    H_W = 0
                if (min_traveltime_w_h == "None"):
                    W_H = 0

                if (H_W == 1):
                    calculate_routes(home_location, work_location, city, userID)

                if (W_H == 1):
                    calculate_routes(work_location, home_location, city, userID)


        print("NUMBER OF REQUESTS TO DIRECTIONS API: " + str(countRequests))

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
