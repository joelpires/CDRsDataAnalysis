# -*- coding: utf-8 -*-
# encoding: utf-8
"""
Exploratory Analysis Tools for CDR Dataset
September 2019
Joel Pires
"""
__author__ = 'Joel Pires'
__date__ = 'September 2019'

import time
import urllib, json
import psycopg2
import configparser
import polyline
import arcpy
import matplotlib.pyplot as plt
import unidecode
import numpy as np
import os

# VARIABLES JUST FOR DEBUG
exceptions = 0
usersCounter = 436
routesCounter = 0
countRequests = 0

# API VARIABLES
geralAPILIMIT = 58500
apiKeys = [os.environ.get('MAPSAPIKEYJO'),  #All the Google API keys available in the system
           os.environ.get('MAPSAPIMA'),
           os.environ.get('MAPSAPILA')]

atualAPILimit = 58500           #decide the limit of request of the initial api
keyNumber = initialNumber = 1   #decide which api key the program should start use

#VARIABLES OF THE CONNECTION WITH THE DATABASE
cur = None
conn = None
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

"""
    Method that is responsible for calculating possible routes given by the Google Directions API and storing the points of each route in postgres table
    Inputs: origin - the geocoordinates of the home/work location of the user
           destination - the geocoordinates of the work/home location of the user
           city - string naming the city of the user
           userID - the integer that identifies the user
           commutingtype - 'H_W' if it is home to work or 'W_H' otherwise
"""
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

    travel_modes = [ "transit", "driving", "walking", "bicycling"]
    mobilityUser = {}
    mobilityUser['routeNumber'] = 0

    for mode in travel_modes:
        if (countRequests >= atualAPILimit or countRequests >= geralAPILIMIT):  #budget limit for each google account
            keyNumber += 1
            chosenkey = apiKeys[keyNumber]

        if (mode == "transit"): #"transit" corresponds to all public travel modes - tram, train, bus, subway
            multimode = True    #multimodal trips only make sense if it involves public travel modes
        else:
            multimode = False

        request = directionsAPIendpoint + 'origin={}&destination={}&mode={}&alternatives=true&key={}'.format(str(origin)[1:-1].replace(" ", ""), str(destination)[1:-1].replace(" ", ""), mode, chosenkey)
        response = json.loads(urllib.urlopen(request).read())
        countRequests += 1

        print("REQUEST: " + str(request))

        logfile.write("\n=== Analyzing routes " + commutingtype + " using the " + mode + " travel mode ===\n")

        if response['status'] == 'OK':

            for route in response['routes']:
                mobilityUser = analyzeLegs(mode, route, mobilityUser, multimode, origin, destination)
                if 'route' in mobilityUser.keys():

                    mobilityUser['route'] = interpolate(mobilityUser, city, userID, commutingtype)
                    sequenceNumber = 0
                    for point in mobilityUser['route']:

                        query = "INSERT INTO public." + city + "_possible_routes (userID, commutingtype, routeNumber, duration, transportModes, latitude, longitude, sequenceNumber, geom_point_orig) VALUES (" + str(userID) + ", \'" + commutingtype + "\'," + str(mobilityUser['routeNumber']) + "," + str(mobilityUser['duration']) + ", ROW" + str(mobilityUser['transport_modes']) + "," + str(point[0]) + "," + str(point[1]) + "," + str(sequenceNumber) + ", st_SetSrid(st_MakePoint(" + str(point[0]) + ", " + str(point[1 ]) + "), 4326))"
                        cur.execute(query)
                        conn.commit()
                        sequenceNumber += 1

                    mobilityUser['routeNumber'] += 1
                    routesCounter += 1

""" 
   Method that analyze deeply the information obtained from the API and extract all the route points of a possible route of the google API
   Inputs: mode - travel mode under analysis
           route - possible route under analysis
           mobilityUser - structure that contains the number of possible routes, the different route points and the respective travel modes
           multimode - if it is a multimodal or unimodal kind of trip
           origin - the geocoordinates of the home/work location of the user
           destination - the geocoordinates of the work/home location of the user
"""
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


            if (previousMode != atualMode or (step['travel_mode']).lower() != mode) and multimode is False:
                return {}
            else:
                if('steps' in step.keys()):
                    for substep in step['steps']:
                        for j in polyline.decode(substep['polyline']['points']):
                            routePoints.append(tuple(j))

                        modeTravel = str(substep['travel_mode']).encode("UTF-8")
                        if modeTravel == "TRANSIT":
                            modeTravel = str(substep['transit_details']['line']['vehicle']['type']).encode("UTF-8")

                        if modeTravel != atualMode:
                            logfile.write("MODE TRAVEL: " + modeTravel + "\n")
                            logfile.write("ATUAL TRAVEL: " + atualMode + "\n")
                            exceptions += 1
                            differentModes = differentModes + (modeTravel,)
                else:
                    for j in polyline.decode(step['polyline']['points']):
                        routePoints.append(tuple(j))

            differentModes = differentModes + (previousMode,)

            previousMode = str(step['travel_mode']).encode("UTF-8")
            if previousMode == "TRANSIT":
                previousMode = str(step['transit_details']['line']['vehicle']['type']).encode("UTF-8")

        routePoints.append(destination)

        if (multimode is True):
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

"""Method responsible for interpolating the route points of each possible possible route given by the Google API. The points become equally
   spaced by 20 meters. Shapefiles before the interpolation and after the interpolation are created.  
"""
def interpolate(mobilityUser, city, userID, commutingtype):
    global logfile

    # convert the points to .csv files
    #logfile.write("Saving the Google Maps API points to CSV file...\n")
    transport_modes = ""
    for word in mobilityUser['transport_modes']:
        transport_modes = transport_modes + "_" + word

    # Creating the needed csv
    filename1 = city + "_" + commutingtype + "_" + str(userID) + "_" + transport_modes + "_non_interpolated_route_points_" + str(mobilityUser['routeNumber'])
    path_csvs = "C:\Users\Joel\Documents\ArcGIS\\ODPaths\\" + city + "\\" + commutingtype + "\\non_interpolated_route_points_csvs\\"
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

    # Creating a GIS Layer from the CSV file
    arcpy.MakeXYEventLayer_management(path_csvs + filename1 + ".csv",
                                      "longitude",
                                      "latitude",
                                      filename1 + "_Layer",
                                      arcpy.SpatialReference("WGS 1984"),
                                      "sequence")

    # Creating a shapefile of the route points
    path_shapefile1 = "C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/" + commutingtype + "/non_interpolated_route_points_shapefiles/"
    arcpy.FeatureClassToFeatureClass_conversion(filename1 + "_Layer",
                                                path_shapefile1,
                                                filename1)

    # Execute PointsToLine
    # Rendering the route line
    filename2 = city + "_" + commutingtype + "_" + str(userID) + "_" + transport_modes + "_route_line_" + str(mobilityUser['routeNumber'])
    path_shapefile2 = "C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/" + commutingtype + "/route_lines_shapefiles/"
    arcpy.PointsToLine_management(path_shapefile1 + filename1 + ".shp",
                                  path_shapefile2 + filename2,
                                  "",
                                  "sequence")

    # Creating a shapefile with the interpolated route points
    filename3 = city + "_" + commutingtype + "_" + str(userID) + "_" + transport_modes + "_interpolated_route_points_" + str(mobilityUser['routeNumber'])
    path_shapefile3 = "C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/" + commutingtype + "/interpolated_route_points_shapefiles/"
    arcpy.GeneratePointsAlongLines_management(path_shapefile2 + filename2 + ".shp",
                                              path_shapefile3 + filename3 + ".shp",
                                              'DISTANCE',
                                              Distance='20 meters',
                                              Include_End_Points='END_POINTS')

    # Converting the shapefile to a layer
    layer = arcpy.MakeFeatureLayer_management(path_shapefile3 + filename3 + ".shp",
                                              filename3)

    # Obtaining the interpolated points from the layer
    fld_list = arcpy.ListFields(layer)
    fld_names = [fld.name for fld in fld_list]
    cursor = arcpy.da.SearchCursor(layer, fld_names)

    interpolated_route = []
    for row in cursor:
        interpolated_route.append((row[1][1], row[1][0]))

    return interpolated_route


""" This procedure is used to calculate the final route once the possible routes were already stored in PostgresSQL. It makes use of
    PostgreSQL to access the needed table and do the heavy calculations. The final route points are stored in a SQL table, no shapefiles generated.
    Inputs: city - string of the city
            userID - user from which we want to infer the exact route points
"""
def calculatingExactRoutes(city, userID):

    query = "INSERT INTO public.finalscores_" + city + " (userID, commutingType, routenumber, transportmodes, duration, finalscore) " \
            "(SELECT j.userid, j.commutingtype, j.routenumber, j.transportmodes, j.duration, (distanceScore*durationscore) " \
            "FROM (     SELECT userid, commutingtype, routenumber, avg(averageToIntermediateTowers) AS distanceScore, transportmodes, duration " \
                        "FROM ( " \
                              "SELECT userid, commutingtype, routenumber, duration, transportmodes, latitude, longitude, avg(distanceWeighted) AS averageToIntermediateTowers " \
                              "FROM (  SELECT userID, commutingType, routeNumber, duration, transportModes, latitude, longitude, sequenceNumber, cellID, frequencia, st_distance(ST_Transform(geom_point_orig, 3857),ST_Transform(geom_point_dest, 3857)) * CAST(1 AS FLOAT)/frequencia AS distanceWeighted " \
                              "FROM (SELECT * FROM public." + city + "_possible_routes f WHERE userID = " + str(userID) + ") f " \
                              "INNER JOIN (SELECT intermediatetowers_h_wid, tower AS cellID, frequencia, geom_point_dest FROM public.frequencies_intermediateTowers_H_W) g " \
                                      "ON g.intermediatetowers_h_wid = userid AND commutingtype = 'H_W' " \
                              "" \
                              "UNION ALL " \
                              "" \
                              "SELECT userID, commutingType, routeNumber, duration, transportModes, latitude, longitude, sequenceNumber, cellID, frequencia, st_distance(ST_Transform(geom_point_orig, 3857),ST_Transform(geom_point_dest, 3857)) * CAST(1 AS FLOAT)/frequencia AS distanceWeighted " \
                              "FROM (SELECT * FROM public." + city + "_possible_routes f WHERE userID = " + str(userID) + ") f " \
                              "INNER JOIN (SELECT intermediatetowers_w_hid, tower AS cellID, frequencia, geom_point_dest FROM public.frequencies_intermediateTowers_W_H) g " \
                              "ON g.intermediatetowers_w_hid = userid AND commutingtype = 'W_H' " \
                        ") i " \
                        "GROUP BY userid, commutingtype, routenumber, duration, transportmodes, latitude, longitude ) h " \
            "GROUP BY userID, commutingType, routenumber, transportmodes, duration " \
            ") j " \
            "INNER JOIN (SELECT *, CASE WHEN (traveltime-duration) < 0 THEN abs(traveltime-duration) ELSE 1 END AS durationscore " \
                        "FROM ( " \
                                "SELECT userID, commutingType, routenumber, transportmodes, duration, travelTime " \
                                "FROM (SELECT * FROM public." + city + "_possible_routes f WHERE userID = " + str(userID) + ") f " \
                                "INNER JOIN (SELECT hwid, minTravelTime_H_W AS travelTime FROM new_traveltimes_h_w_u) g " \
                                        "ON hwid = userid AND commutingtype = 'H_W' " \
                                "GROUP BY userID, commutingType, routenumber, transportmodes, duration,travelTime " \
                                "" \
                                "UNION ALL " \
                                "" \
                                "SELECT userID, commutingType, routenumber, transportmodes, duration,travelTime " \
                                "FROM (SELECT * FROM public." + city + "_possible_routes f WHERE userID = " + str(userID) + ") f " \
                                "INNER JOIN (SELECT whid, minTravelTime_W_H AS travelTime FROM new_traveltimes_w_h_u) g " \
                                        "ON whid = userid AND commutingtype = 'W_H' " \
                                "GROUP BY userID, commutingType, routenumber, transportmodes, duration, travelTime " \
                        ") t) l " \
             "ON     j.userID = l.userID " \
             "AND    j.commutingType = l.commutingType " \
             "AND    j.routenumber = l.routenumber " \
             "AND    j.transportmodes = l.transportmodes " \
             "AND    j.duration = l.duration " \
             ")"

    cur.execute(query)
    conn.commit()


    query = "INSERT INTO public.finalroutes_" + city + " (userID, commutingType, routeNumber, duration, transportModes, latitude, longitude, sequenceNumber, geom_point_orig) " \
            "(SELECT g.* FROM (SELECT * FROM public." + city + "_possible_routes f WHERE userID = " + str(userID) + ") g, (   SELECT userid, commutingtype, routenumber, transportmodes, duration, finalscore " \
                                                                                                           "FROM (SELECT * FROM public.finalscores_" + city + " WHERE userID = " + str(userID) + ") f " \
                                                                                                           "WHERE (userid, commutingType, finalscore) IN ( SELECT userid, commutingType, min(finalscore) " \
                                                                                                                                                          "FROM (SELECT DISTINCT ON (finalscore) * FROM public.finalscores_" + city + " " \
                                                                                                                                                          "WHERE userID = " + str(userID) + ") f " \
                                                                                                           "GROUP BY userID, commutingType)) f " \
             "WHERE f.userid = g.userid " \
               "AND f.commutingtype = g.commutingtype " \
               "AND f.routenumber = g.routenumber)"

    cur.execute(query)
    conn.commit()

""" Once the final routes were calculated and store in postgreSQL, it is needed to render the shapefiles of the final routes.
    Inputs: city - string of the city
            userID - user from which we want to infer the exact route points
"""
def renderFinalRoutes(city, userID):


    query1 = "SELECT DISTINCT ON(userID, commutingType) * FROM public.finalRoutes_" + city + " WHERE userID = " + str(userID)

    cur.execute(query1)
    differentRoutes = cur.fetchall()


    for route, value in enumerate(differentRoutes):
        query2 = "SELECT * FROM public.finalRoutes_" + city + " WHERE userID = " + str(userID) + " AND commutingType = \'" + str(differentRoutes[route][1]) + "\' ORDER BY sequencenumber ASC"

        cur.execute(query2)
        fetched = cur.fetchall()

        transport_modes = (differentRoutes[route][4]).replace(",\"", "")
        transport_modes = (differentRoutes[route][4]).replace("\"", "")
        transport_modes = transport_modes.replace("(", "")
        transport_modes = transport_modes.replace(")", "")
        transport_modes = transport_modes.replace(",", "_")

        filename1 = city + "_" + str(differentRoutes[route][1]) + "_" + str(differentRoutes[route][0]) + "_" + transport_modes + "_final_routes_points_" + str(differentRoutes[route][2])
        path_csvs = "C:\Users\Joel\Documents\ArcGIS\\ODPaths\\" + city + "\\" + str(differentRoutes[route][1]) + "\\final_routes_csvs\\"

        with open(path_csvs + filename1 + ".csv", mode='w') as fp:
            fp.write("latitude, longitude, sequence")
            fp.write("\n")
            for record in fetched:
                line = str(record[5]) + "," + str(record[6]) + "," + str(record[7])
                fp.write(line)
                fp.write("\n")
        fp.close()

        #Creating a GIS Layer of the final route from the CSV file..
        arcpy.MakeXYEventLayer_management(path_csvs + filename1 + ".csv",
                                          "longitude",
                                          "latitude",
                                          filename1 + "_Layer",
                                          arcpy.SpatialReference("WGS 1984"),
                                          "sequence")

        # Creating a shapefile of the final route...
        path_shapefile1 = "C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/" + differentRoutes[route][1] + "/final_routes_points/"
        arcpy.FeatureClassToFeatureClass_conversion(filename1 + "_Layer",
                                                    path_shapefile1,
                                                    filename1)

        # Rendering the route line...
        filename2 = city + "_" + differentRoutes[route][1] + "_" + str(userID) + "_" + transport_modes + "_final_route_line_" + str(differentRoutes[route][2])
        path_shapefile2 = "C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/" + differentRoutes[route][1] + "/final_routes_lines/"
        arcpy.PointsToLine_management(path_shapefile1 + filename1 + ".shp",
                                      path_shapefile2 + filename2,
                                      "",
                                      "sequence")

""" This method serves to create the necessary folders to store all the shapefiles created relative to a certain city"""
def archivesCity(city):
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city, 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/interpolated_route_points_shapefiles", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/non_interpolated_route_points_csvs", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/non_interpolated_route_points_shapefiles", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/route_lines_shapefiles", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/final_routes_csvs", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/final_routes_points", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/H_W/final_routes_lines", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/interpolated_route_points_shapefiles", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/non_interpolated_route_points_csvs", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/non_interpolated_route_points_shapefiles", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/route_lines_shapefiles", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/final_routes_csvs", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/final_routes_points", 0777)
    os.mkdir("C:/Users/Joel/Documents/ArcGIS/ODPaths/" + city + "/W_H/final_routes_lines", 0777)

    #print("[DIRECTORIES CREATED]")

    # Type "MODES" needs to be previously created

    query13 = "CREATE TABLE IF NOT EXISTS public." + city + "_possible_routes (userID INTEGER, commutingType TEXT, routeNumber INTEGER, duration INTEGER, transportModes MODES, latitude NUMERIC, longitude NUMERIC, sequenceNumber INTEGER, geom_point_orig GEOMETRY(Point, 4326))"
    cur.execute(query13)
    conn.commit()

    query2 = "CREATE TABLE IF NOT EXISTS public.OD" + city + "_users_characterization AS (SELECT * FROM users_characterization_final WHERE user_id IN (SELECT id FROM final_eligibleUsers WHERE municipal = \'" + city + "\'))"
    cur.execute(query2)
    conn.commit()

    query11 = "CREATE TABLE IF NOT EXISTS public.finalScores_" + city + " (userID INTEGER, commutingType TEXT, routeNumber INTEGER, transportmodes MODES, duration INTEGER, finalscore NUMERIC)"
    cur.execute(query11)
    conn.commit()


    query13 = "CREATE TABLE IF NOT EXISTS public.finalRoutes_" + city + " (userID INTEGER, commutingType TEXT, routeNumber INTEGER, duration INTEGER, transportModes MODES, latitude NUMERIC, longitude NUMERIC, sequenceNumber INTEGER, geom_point_orig GEOMETRY(Point, 4326))"
    cur.execute(query13)
    conn.commit()

""" This method serves just to make a graph of the distribution of the users in each municipal"""
def charts():
    query = "SELECT * FROM public.eligibleUsers_byMunicipal"
    cur.execute(query)

    fetched = cur.fetchall()
    municipals = parseDBColumns(fetched, 0, str)
    population = parseDBColumns(fetched, 1, float)
    datasetUsers = parseDBColumns(fetched, 2, float)
    number = parseDBColumns(fetched, 3, float)
    density = parseDBColumns(fetched, 4, float)

    municipals = [unidecode.unidecode(line.decode('utf-8').strip()) for line in municipals]

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


""" Helper function just to obtain a certain collumn of a DB table in a form of a list
    Inputs: listToParse - list of lists, in which each list is a collumn of a DB table
            collumn: specify the index of the collumn that we want to extract
            _constructor: specifiy the type of the data contained in the specidifed collumn
    Outputs: a list with the values of the collumn
"""
def parseDBColumns(listToParse, collumn, _constructor):
    constructor = _constructor
    collumnList = []
    for i in range(0, len(listToParse)):
        collumnList.append(constructor(listToParse[i][collumn]))

    return collumnList


""" Main Function """
def main():
    global countRequests
    global cur
    global keyNumber
    global initialNumber
    global conn
    global usersCounter
    global logfile

    start_time = time.time()    #Count time

    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('CONNECTION TO THE POSTGRESQL DATABASE...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        query = "SELECT * FROM public.final_eligibleUsers_byMunicipal"  #query needed just to retrieve all the municipals under evaluation
        cur.execute(query)

        fetched = cur.fetchall()
        municipals = parseDBColumns(fetched, 0, str)

        #optional - create charts about the municipals and the quantity of the users in there
        #charts()

        #These 4 lines are necessary in order to the algorithm run smoothly in cities with special characters like "ç", "à", etc
        temp = [unidecode.unidecode(line.decode('utf-8').strip()) for line in municipals]
        new_municipals = []
        for index, elem in enumerate(temp):
            new_municipals.append((elem.replace(" ", "_")).replace("-", "_"))

        #Main loop that will create routes for the users of each municipal
        countCity = 0
        for index, city in enumerate(new_municipals):

            archivesCity(city)

            countUsers = 0

            #Retrieving the users of each city. Information about their home and work location is also retrieved
            query2 = "SELECT * FROM public.OD" + city + "_users_characterization"
            cur.execute(query2)
            fetched_users = cur.fetchall()

            #LOOP across all the users
            for i in range(len(fetched_users)):
                userID = str(fetched_users[i][0])
                home_location = (float(fetched_users[i][11]), float(fetched_users[i][12]))
                work_location = (float(fetched_users[i][14]), float(fetched_users[i][15]))
                min_traveltime_h_w = str(fetched_users[i][18])
                min_traveltime_w_h = str(fetched_users[i][24])

                if (min_traveltime_h_w != "None"):
                    #CALCULATING ALL POSSIBLE ROUTES HOME TO WORKPLACE OF USER
                    calculate_routes(home_location, work_location, city, userID, "H_W")

                if (min_traveltime_w_h != "None"):
                    #CALCULATING ALL POSSIBLE ROUTES WORKPLACE TO HOME OF USER
                    calculate_routes(work_location, home_location, city, userID, "W_H")

                #Calculating the exact/final commuting routes
                calculatingExactRoutes(city, userID)

                #Creating the final shapefiles to put on ARCGIS
                renderFinalRoutes(city, userID)

                countUsers += 1
                usersCounter += 1

                logfile.write("\n==================== User " + str(usersCounter) + " (id: " + str(userID) + ") of " + city + " was processed =====================\n")

            countCity += 1

            logfile.write("\n==================== The city " + str(countCity) + "/274 (name:" + city + ") was processed =====================\n\n\n")

            query1 = "DROP TABLE IF EXISTS public.OD" + city + "_users_characterization"    #We don't need this temporary table anymore
            cur.execute(query1)
            conn.commit()

        logfile.write("A TOTAL OF " + str(countCity) + " cities were processed.\n")
        logfile.write("A TOTAL OF " + str(routesCounter) + " routes were processed.\n")
        logfile.write("A TOTAL OF " + str(usersCounter) + " users were processed.\n")
        logfile.write("A TOTAL OF " + str(exceptions) + " exceptions were encountered.\n")
        logfile.write("A TOTAL OF " + str(countRequests) + " REQUESTS WERE MADE TO DIRECTIONS API, USING " + str(keyNumber-initialNumber+1) + " DIFFERENT API KEYS\n")

        elapsed_time = time.time() - start_time
        logfile.write("===== FINAL EXECUTION TIME: " + str(elapsed_time/60) + " MINUTES =====")


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


if __name__ == '__main__':
    main()
