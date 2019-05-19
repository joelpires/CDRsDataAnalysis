# -*- coding: utf-8 -*-
# encoding: utf-8
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

exceptions = 0
usersCounter = 0
routesCounter = 0
cur = None
conn = None
countRequests = 0
geralAPILIMIT = 58500
apiKeys = [os.environ.get('MAPSAPIKEYJO'),
           os.environ.get('MAPSAPIMA'),
           os.environ.get('MAPSAPILA')]
"""
           os.environ.get('MAPSAPIOC'),
           os.environ.get('MAPSAPIBS'),

           os.environ.get('MAPSAPIAP'),
           os.environ.get('MAPSAPIMC')
           #telefone da mae
           ]  # aproximately a total of 351000 requests can be made
"""

atualAPILimit = 58500 #decide the limit of request of the initial api
keyNumber = initialNumber = 0 #decide which api key the program should start use
logfile = open('log.txt', 'w')


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


def calculate_routes(origin, destination, city, userID, commutingtype):

    global countRequests
    global cur
    global atualAPILimit
    global keyNumber
    global apiKeys
    global geralAPILIMIT
    global routesCounter
    global logfile

    chosenkey = apiKeys[initialNumber]

    directionsAPIendpoint = 'https://maps.googleapis.com/maps/api/directions/json?'

    travel_modes = [ "DRIVING", "MULTIMODE", "WALKING", "BICYCLING", "TRANSIT"]

    for mode in travel_modes:
        if (countRequests >= atualAPILimit or countRequests >= geralAPILIMIT):  #budget limit for each google account
            keyNumber += 1
            chosenkey = apiKeys[keyNumber]


        if (mode == "MULTIMODE"):
            request = directionsAPIendpoint + 'origin={}&destination={}&alternatives=true&key={}'.format(str(origin)[1:-1].replace(" ", ""), str(destination)[1:-1].replace(" ", ""),chosenkey)
            multimode = True
        else:
            request = directionsAPIendpoint + 'origin={}&destination={}&mode={}&alternatives=true&key={}'.format(str(origin)[1:-1].replace(" ", ""), str(destination)[1:-1].replace(" ", ""), mode, chosenkey)
            multimode = False

        response = json.loads(urllib.urlopen(request).read())
        countRequests += 1


        logfile.write("\n=== Analyzing routes using the " + mode + " travel mode ===\n")

        # pode nao ter resposta

        if response['status'] == 'OK':
            for route in response['routes']:
                mobilityUser = {}
                mobilityUser['routeNumber'] = 0
                mobilityUser = analyzeLegs(mode, route, mobilityUser, multimode, origin, destination)
                if 'route' in mobilityUser.keys():
                    mobilityUser['route'] = interpolate(mobilityUser, city, userID, commutingtype)
                    sequenceNumber = 0
                    for point in mobilityUser['route']:
                        query = "INSERT INTO public." + city + "_possible_routes (userID, commutingtype, routeNumber, duration, transportModes, latitude, longitude, sequenceNumber) VALUES (" + str(userID) + ", \'" + commutingtype + "\'," + str(mobilityUser['routeNumber']) + "," + str(mobilityUser['duration']) + ", ROW" + str(mobilityUser['transport_modes']) + "," + str(point[0]) + "," + str(point[1]) + "," + str(sequenceNumber) + ")"
                        cur.execute(query)
                        conn.commit()
                        sequenceNumber += 1

                    logfile.write("\n=== Route number " + str(mobilityUser['routeNumber']) + " of user " + str(userID) + " in commuting " + commutingtype + " was processed ===\n")
                    mobilityUser['routeNumber'] += 1
                    routesCounter += 1



def analyzeLegs(mode, route, mobilityUser, multimode, origin, destination):
    global exceptions
    global logfile

    differentModes = ()
    routePoints = []
    mobilityUser['route'] = []

    for leg in route['legs']:

        mobilityUser['duration'] = leg['duration']['value']

        previousMode = str(route['legs'][0]['steps'][0]['travel_mode']).encode("UTF-8")


        if previousMode == "TRANSIT":
            previousMode = str(route['legs'][0]['steps'][0]['transit_details']['line']['vehicle']['type']).encode("UTF-8")

        routePoints.append(origin)
        for step in leg['steps']:

            atualMode = str(step['travel_mode']).encode("UTF-8")
            if atualMode == "TRANSIT":
                atualMode = str(step['transit_details']['line']['vehicle']['type']).encode("UTF-8")


            if (previousMode != atualMode or step['travel_mode'] != mode) and multimode is False:
                return {}
            else:

                for j in polyline.decode(step['polyline']['points']):
                    routePoints.append(tuple(j))
                if('steps' in step.keys()):
                    for substep in step['steps']:
                        for j in polyline.decode(substep['polyline']['points']):
                            routePoints.append(tuple(j))

                        modeTravel = str(substep['travel_mode']).encode("UTF-8")
                        if modeTravel != atualMode:
                            logfile.write("MODE TRAVEL: " + modeTravel)
                            logfile.write("ATUAL TRAVEL: " + atualMode)
                            exceptions += 1
                            differentModes = differentModes + (modeTravel,)

            differentModes = differentModes + (previousMode,)

            previousMode = str(step['travel_mode']).encode("UTF-8")
            if previousMode == "TRANSIT":
                previousMode = str(step['transit_details']['line']['vehicle']['type']).encode("UTF-8")

        routePoints.append(destination)

        if (multimode is True and differentModes[1:] != differentModes[:-1]):
            mobilityUser['transport_modes'] = tuple(set(differentModes))
            while(len(mobilityUser['transport_modes']) != 4):
                mobilityUser['transport_modes'] = mobilityUser['transport_modes'] + ("",)
            mobilityUser['route'] = routePoints #list(OrderedSet(routePoints))
            return mobilityUser

        elif (multimode is False and differentModes[1:] == differentModes[:-1]):
            mobilityUser['transport_modes'] = (previousMode,)
            while (len(mobilityUser['transport_modes']) != 4):
                mobilityUser['transport_modes'] = mobilityUser['transport_modes'] + ("",)
            mobilityUser['route'] = routePoints #list(OrderedSet(routePoints))
            return mobilityUser

        else:
            return {}



def interpolate(mobilityUser, city, userID, commutingtype):
    global logfile

    # convert the points to .csv files
    #logfile.write("Saving the Google Maps API points to CSV file...")
    transport_modes = ""
    for word in mobilityUser['transport_modes']:
        transport_modes = transport_modes + word

    filename1 = city + "_" + commutingtype + "_" + str(userID) + "_" + transport_modes + "_non_interpolated_route_points_" + str(mobilityUser['routeNumber'])
    path_csvs = "C:\Users\Joel\Documents\ArcGIS\\" + city + "\\" + commutingtype + "\\non_interpolated_route_points_csvs\\"
    with open(path_csvs + filename1 + ".csv", mode='w') as fp:
        fp.write("latitude, longitude, sequence")
        fp.write("\n")
        sequence = 0
        for point in mobilityUser['route']:
            line = str(point[0]) + "," + str(point[1]) + "," + str(sequence)
            fp.write(line)
            fp.write("\n")
            sequence += 1
    fp.close()

    # creating GIS Layer
    #logfile.write("Creating a GIS Layer from the CSV file...")
    arcpy.MakeXYEventLayer_management(path_csvs + filename1 + ".csv",
                                      "longitude",
                                      "latitude",
                                      filename1 + "_Layer",
                                      arcpy.SpatialReference("WGS 1984"),
                                      "sequence")

    # convert the points to shapefile
    #logfile.write("Creating a shapefile of the route points...")
    path_shapefile1 = "C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/" + commutingtype + "/non_interpolated_route_points_shapefiles/"
    arcpy.FeatureClassToFeatureClass_conversion(filename1 + "_Layer",
                                                path_shapefile1,
                                                filename1)

    # Execute PointsToLine
    #logfile.write("Rendering the route line...")
    filename2 = city + "_" + commutingtype + "_" + str(userID) + "_" + transport_modes + "_route_line_" + str(mobilityUser['routeNumber'])
    path_shapefile2 = "C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/" + commutingtype + "/route_lines_shapefiles/"
    arcpy.PointsToLine_management(path_shapefile1 + filename1 + ".shp",
                                  path_shapefile2 + filename2,
                                  "",
                                  "sequence")

    # interpolate the points
    #logfile.write("Creating a shapefile with the interpolated route points...")
    filename3 = city + "_" + commutingtype + "_" + str(userID) + "_" + transport_modes + "_interpolated_route_points_" + str(mobilityUser['routeNumber'])
    path_shapefile3 = "C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/" + commutingtype + "/interpolated_route_points_shapefiles/"
    arcpy.GeneratePointsAlongLines_management(path_shapefile2 + filename2 + ".shp",
                                              path_shapefile3 + filename3 + ".shp",
                                              'DISTANCE',
                                              Distance='20 meters',
                                              Include_End_Points='END_POINTS')

    # convert the shapefile to layer
    print("Converting the shapefile to a layer...")
    logfile.write("Converting the shapefile to a layer...")
    layer = arcpy.MakeFeatureLayer_management(path_shapefile3 + filename3 + ".shp",
                                              filename3)

    # convert layer to points
    print("Obtaining the interpolated points from the layer...\n")
    logfile.write("Obtaining the interpolated points from the layer...\n")
    fld_list = arcpy.ListFields(layer)
    fld_names = [fld.name for fld in fld_list]
    cursor = arcpy.da.SearchCursor(layer, fld_names)

    interpolated_route = []
    for row in cursor:
        interpolated_route.append((row[1][1], row[1][0]))

    return interpolated_route


def calculatingExactRoutes(city):

    query1 = "ALTER TABLE public." + city + "_possible_routes ADD COLUMN geom_point_orig GEOMETRY(Point, 4326)"
    query2 = "UPDATE public." + city + "_possible_routes SET geom_point_orig=st_SetSrid(st_MakePoint(longitude, latitude), 4326)"

    cur.execute(query1)
    conn.commit()

    cur.execute(query2)
    conn.commit()

    query3 = "CREATE TABLE public.frequencies_intermediateTowers_H_W_" + city + " AS ( " \
             "SELECT intermediatetowers_h_wid, tower, latitude, longitude, count(*) AS frequencia " \
             "FROM intermediateTowers_H_W_u " \
             "INNER JOIN (SELECT user_id FROM public.OD" + city + "_users_characterization) y " \
             "ON intermediatetowers_h_wid = user_id " \
             "GROUP BY intermediatetowers_h_wid, tower, latitude, longitude)"
    cur.execute(query3)
    conn.commit()


    query4 = "CREATE TABLE public.frequencies_intermediateTowers_W_H_" + city + " AS ( " \
             "SELECT intermediatetowers_w_hid, tower, latitude, longitude, count(*) AS frequencia " \
             "FROM intermediateTowers_W_H_u " \
             "INNER JOIN (SELECT user_id FROM public.OD" + city + "_users_characterization) y " \
             "ON intermediatetowers_w_hid = user_id " \
             "GROUP BY intermediatetowers_w_hid, tower, latitude, longitude)"
    cur.execute(query4)
    conn.commit()


    query5 = "ALTER TABLE public.frequencies_intermediateTowers_H_W_" + city + " ADD COLUMN geom_point_dest GEOMETRY(Point, 4326)"
    cur.execute(query5)
    conn.commit()
    query5 = "UPDATE public.frequencies_intermediateTowers_H_W_" + city + " SET geom_point_dest=st_SetSrid(st_MakePoint(longitude, latitude), 4326)"
    cur.execute(query5)
    conn.commit()


    query6 = "ALTER TABLE public.frequencies_intermediateTowers_W_H_" + city + " ADD COLUMN geom_point_dest GEOMETRY(Point, 4326)"
    cur.execute(query6)
    conn.commit()
    query6 = "UPDATE public.frequencies_intermediateTowers_W_H_" + city + " SET geom_point_dest=st_SetSrid(st_MakePoint(longitude, latitude), 4326)"
    cur.execute(query6)
    conn.commit()

    query7 = "CREATE TABLE public.distancesWeighted_" + city + " AS (" \
             "SELECT f.*, cellID, frequencia, st_distance(ST_Transform(geom_point_orig, 3857),ST_Transform(geom_point_dest, 3857)) * CAST(1 AS FLOAT)/frequencia AS distanceWeighted " \
             "FROM public." + city + "_possible_routes f " \
             "INNER JOIN (SELECT intermediatetowers_h_wid, tower AS cellID, frequencia, geom_point_dest FROM public.frequencies_intermediateTowers_H_W_" + city + ") g " \
             "ON g.intermediatetowers_h_wid = userid " \
             "AND commutingtype = 'H_W' " \
             "" \
             "UNION ALL " \
             "" \
             "SELECT f.*, cellID, frequencia, st_distance(ST_Transform(geom_point_orig, 3857),ST_Transform(geom_point_dest, 3857)) * CAST(1 AS FLOAT)/frequencia AS distanceWeighted " \
             "FROM public." + city + "_possible_routes f " \
             "INNER JOIN (SELECT intermediatetowers_w_hid, tower AS cellID, frequencia, geom_point_dest FROM public.frequencies_intermediateTowers_W_H_" + city + ") g " \
             "ON g.intermediatetowers_w_hid = userid " \
             "AND commutingtype = 'W_H')"

    cur.execute(query7)
    conn.commit()

    query8 = "CREATE TABLE public.distanceScores_" + city + " AS( " \
             "SELECT userid, commutingtype, routenumber, avg(averageToIntermediateTowers) AS distanceScore, transportmodes, duration " \
             "FROM( " \
                 "SELECT userid, commutingtype, routenumber, duration, transportmodes, latitude, longitude,avg(distanceWeighted) AS averageToIntermediateTowers " \
                 "FROM public.distancesWeighted_" + city + " " \
                 "GROUP BY userid, commutingtype, routenumber, duration, transportmodes, latitude, longitude " \
             ") h " \
             "GROUP BY userID, commutingType, routenumber, transportmodes, duration " \
             ")"

    cur.execute(query8)
    conn.commit()

    query9 = "CREATE TABLE public.traveltimes_and_durations_" + city + " AS ( " \
             "SELECT userID, commutingType, routenumber, transportmodes, duration, travelTime " \
             "FROM public." + city + "_possible_routes f " \
             "INNER JOIN (SELECT hwid, minTravelTime_H_W AS travelTime FROM new_traveltimes_h_w_u) g " \
             "ON hwid = userid " \
             "AND commutingtype = 'H_W' " \
             "GROUP BY userID, commutingType, routenumber, transportmodes, duration,travelTime " \
             "" \
             "UNION ALL " \
             "SELECT userID, commutingType, routenumber, transportmodes, duration,travelTime " \
             "FROM public." + city + "_possible_routes f " \
             "INNER JOIN (SELECT whid, minTravelTime_W_H AS travelTime FROM new_traveltimes_w_h_u) g " \
             "ON whid = userid " \
             "AND commutingtype = 'W_H' " \
             "GROUP BY userID, commutingType, routenumber, transportmodes, duration, travelTime)"

    cur.execute(query9)
    conn.commit()


    query10 = "CREATE TABLE public.durationsScores_" + city + " AS ( " \
              "SELECT *, " \
              "CASE " \
              "WHEN (traveltime-duration) < 0 THEN traveltime-duration " \
              "ELSE 0 " \
              "END AS durationscore " \
              "FROM public.traveltimes_and_durations_" + city + ")"

    cur.execute(query10)
    conn.commit()

    query11 = "CREATE TABLE public.finalScores_" + city + " AS ( " \
              "SELECT j.userid, j.commutingtype, j.routenumber, j.transportmodes, j.duration, (CAST(0.7 AS FLOAT)*distanceScore + CAST(0.3 AS FLOAT)*durationscore) AS finalscore " \
              "FROM public.distanceScores_" + city + " j " \
              "INNER JOIN public.durationsScores_" + city + " l " \
              "ON     j.userID = l.userID " \
              "AND    j.commutingType = l.commutingType " \
              "AND    j.routenumber = l.routenumber " \
              "AND    j.transportmodes = l.transportmodes " \
              "AND    j.duration = l.duration) "

    cur.execute(query11)
    conn.commit()

    query12 = "CREATE TABLE public.exactRoutes_" + city + " AS ( " \
              "SELECT userid, commutingtype, routenumber, transportmodes, duration, finalscore AS score " \
              "FROM public.finalScores_" + city + " " \
              "WHERE (userid, commutingType, finalscore) IN ( " \
                  "SELECT userid, commutingType, max(finalscore) " \
                  "FROM public.finalScores_" + city + " " \
                  "GROUP BY userID, commutingType))"

    cur.execute(query12)
    conn.commit()


    query13 = "CREATE TABLE public.finalRoutes_" + city + " AS ( " \
              "SELECT g.* " \
              "FROM public." + city + "_possible_routes g, public.exactRoutes_" + city + " f " \
              "WHERE f.userid = g.userid " \
              "AND f.commutingtype = g.commutingtype " \
              "AND f.routenumber = g.routenumber)"

    cur.execute(query13)
    conn.commit()


    query11 = "DROP TABLE IF EXISTS public.frequencies_intermediateTowers_H_W_" + city
    cur.execute(query11)
    conn.commit()


    query11 = "DROP TABLE IF EXISTS public.frequencies_intermediateTowers_W_H_" + city
    cur.execute(query11)
    conn.commit()

    query11 = "DROP TABLE IF EXISTS public.distancesWeighted_" + city
    cur.execute(query11)
    conn.commit()

    query11 = "DROP TABLE IF EXISTS public.distanceScores_" + city
    cur.execute(query11)
    conn.commit()

    query11 = "DROP TABLE IF EXISTS public.traveltimes_and_durations_" + city
    cur.execute(query11)
    conn.commit()

    query11 = "DROP TABLE IF EXISTS public.durationsScores_" + city
    cur.execute(query11)
    conn.commit()

    query11 = "DROP TABLE IF EXISTS public.exactRoutes_" + city
    cur.execute(query11)
    conn.commit()

    query11 = "DROP TABLE IF EXISTS public.finalScores_" + city
    cur.execute(query11)
    conn.commit()

    print("ganda lol")
    print(city)


def renderFinalRoutes(city):

    #logfile.write("Saving the final route points into csvs...")
    query1 = "SELECT * FROM public." + city + "_possible_routes SET geom_point_orig=st_SetSrid(st_MakePoint(longitude, latitude), 4326)"

    cur.execute(query1)
    conn.commit()


    filename1 = city + "_" + commutingtype + "_" + str(userID) + "_" + transport_modes + "_non_interpolated_route_points_" + str(mobilityUser['routeNumber'])
    path_csvs = "C:\Users\Joel\Documents\ArcGIS\\" + city + "\\" + commutingtype + "\\non_interpolated_route_points_csvs\\"
    with open(path_csvs + filename1 + ".csv", mode='w') as fp:
        fp.write("latitude, longitude, sequence")
        fp.write("\n")
        sequence = 0
        for point in mobilityUser['route']:
            line = str(point[0]) + "," + str(point[1]) + "," + str(sequence)
            fp.write(line)
            fp.write("\n")
            sequence += 1
    fp.close()

    # creating GIS Layer
    #logfile.write("Creating a GIS Layer of the final route from the CSV file...")
    arcpy.MakeXYEventLayer_management(path_csvs + filename1 + ".csv",
                                      "longitude",
                                      "latitude",
                                      filename1 + "_Layer",
                                      arcpy.SpatialReference("WGS 1984"),
                                      "sequence")

    # convert the points to shapefile
    #logfile.write("Creating a shapefile of the final route...")
    path_shapefile1 = "C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/" + commutingtype + "/non_interpolated_route_points_shapefiles/"
    arcpy.FeatureClassToFeatureClass_conversion(filename1 + "_Layer",
                                                path_shapefile1,
                                                filename1)












""" Connect to the PostgreSQL database server """
def connect():
    global countRequests
    global cur
    global keyNumber
    global initialNumber
    global conn
    global usersCounter
    global logfile

    start_time = time.time()

    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('CONNECTION TO THE POSTGRESQL DATABASE...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        query = "SELECT * FROM public.eligibleUsers_byMunicipal"
        cur.execute(query)

        fetched = cur.fetchall()
        municipals = parseDBColumns(fetched, 0, str)
        population = parseDBColumns(fetched, 1, float)
        datasetUsers = parseDBColumns(fetched, 2, float)
        number = parseDBColumns(fetched, 3, float)
        density = parseDBColumns(fetched, 4, float)

        municipals = [unidecode.unidecode(line.decode('utf-8').strip()) for line in municipals]
        """
        for index, elem in enumerate(municipals):
            municipals[index] = elem.replace(" ", "\n")
    
        array = np.arange(0, len(municipals), 1)
        array0 = [x - 0.4 for x in array]
        array2 = [x - 0.2 for x in array]
        array3 = [x + 0.2 for x in array]

        fig = plt.figure(figsize=(16, 12))
        ax = plt.axes()
        ax.set_xlim(-1, 12)
        ax.set_ylim(1, 1000000)
        #plt.yticks(np.arange(0, 560000, 50000), fontsize=14)
        plt.yscale("log")
        rects1 = ax.bar(array[:12], number[:12], width=0.2, color='b', align='center')
        rects2 = ax.bar(array3[:12], density[:12], width=0.2, color='g', align='center')
        rects3 = ax.bar(array0[:12], population[:12], width=0.2, color='r', align='center')
        rects4 = ax.bar(array2[:12], datasetUsers[:12], width=0.2, color='k', align='center')

        ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), ("Number of Users to Infer Commuting Patterns", "Tower Density - Number of Towers per 20 Km2", "Number of Inhabitants", "Number of Users in the Dataset"))
        plt.xticks(array[:12], municipals[:12])

        rects = ax.patches
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 1, 1.01 * height,
                    '%d' % int(height),
                    ha='center', va='bottom')
        


        plt.xlabel("Municipal", fontsize=20)
        plt.grid(True)
        plt.show()

        """

        if os.path.exists('C:/Users/Joel/Documents/ArcGIS/ODPaths') and os.path.isdir('C:/Users/Joel/Documents/ArcGIS/ODPaths'):
            shutil.rmtree('C:/Users/Joel/Documents/ArcGIS/ODPaths')

        os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths", 0777)

        countCity = 0
        for city in municipals:

            countUsers = 0
            city = city.replace(" ", "_")
            city = city.replace("-", "_")

            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city, 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/interpolated_route_points_shapefiles", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/non_interpolated_route_points_csvs", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/non_interpolated_route_points_shapefiles", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/route_lines_shapefiles", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/final_routes_csvs", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/final_routes_shapefiles", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/interpolated_route_points_shapefiles", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/non_interpolated_route_points_csvs", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/non_interpolated_route_points_shapefiles", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/route_lines_shapefiles", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/final_routes_csvs", 0777)
            os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/final_routes_shapefiles", 0777)

            query11 = "DROP TABLE IF EXISTS public." + city + "_possible_routes"
            cur.execute(query11)
            conn.commit()

            #Type "MODES" needs to be previously created

            query13 = "CREATE TABLE public." + city + "_possible_routes (userID INTEGER, commutingType TEXT, routeNumber INTEGER, duration INTEGER, transportModes MODES, latitude NUMERIC, longitude NUMERIC, sequenceNumber INTEGER)"
            cur.execute(query13)
            conn.commit()


            query1 = "DROP TABLE IF EXISTS OD" + city + "_users_characterization"
            cur.execute(query1)
            conn.commit()


            query2 = "CREATE TABLE public.OD" + city + "_users_characterization AS (SELECT * FROM users_characterization_final WHERE user_id IN (SELECT id FROM eligibleUsers WHERE municipal = \'" + city + "\'))"
            cur.execute(query2)
            conn.commit()


            query3 = "SELECT * FROM public.OD" + city + "_users_characterization"
            cur.execute(query3)
            fetched_users = cur.fetchall()

            """
            #DIRECTIONS API
            for i in range(len(fetched_users)):

                userID = str(fetched_users[i][0])
                home_location = (float(fetched_users[i][12]), float(fetched_users[i][13]))
                work_location = (float(fetched_users[i][15]), float(fetched_users[i][16]))
                min_traveltime_h_w = str(fetched_users[i][19])
                min_traveltime_w_h = str(fetched_users[i][25])

             
                if (min_traveltime_h_w != "None"):
                    calculate_routes(home_location, work_location, city, userID, "H_W")

                if (min_traveltime_w_h != "None"):
                    calculate_routes(work_location, home_location, city, userID, "W_H")

                countUsers += 1
                usersCounter += 1
            
                logfile.write("\n==================== User number " + str(countUsers) + " of " + city + " was processed =====================\n")
            """
            countCity += 1

            #logfile.write("Calculating the exact pendular routes...")
            calculatingExactRoutes(city)

            query1 = "DROP TABLE IF EXISTS public.OD" + city + "_users_characterization"
            cur.execute(query1)
            conn.commit()

            renderFinalRoutes(city)

            logfile.write("\n==================== The city  of " + city + " was processed =====================\n")

        logfile.write("A TOTAL OF " + str(countCity) + " cities were processed.")
        logfile.write("A TOTAL OF " + str(routesCounter) + " routes were processed.")
        logfile.write("A TOTAL OF " + str(usersCounter) + " users were processed.")
        logfile.write("A TOTAL OF " + str(exceptions) + " exceptions were encountered")
        logfile.write("A TOTAL OF " + str(countRequests) + " REQUESTS WERE MADE TO DIRECTIONS API, USING " + str(keyNumber-initialNumber+1) + " DIFFERENT API KEYS")

        elapsed_time = time.time() - start_time
        print("EXECUTION TIME: " + str(elapsed_time/60) + " MINUTES")


        # close the communication with the PostgreSQL
        cur.close()
        logfile.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logfile.close()
    finally:
        if conn is not None:
            conn.close()
            print('DATABASE CONNECTION CLOSED.')
            logfile.close()



def parseDBColumns(listToParse, collumn, _constructor):
    constructor = _constructor
    collumnList = []
    for i in range(0, len(listToParse)):
        collumnList.append(constructor(listToParse[i][collumn]))

    return collumnList


def main():

    connect()


if __name__ == '__main__':
    main()
