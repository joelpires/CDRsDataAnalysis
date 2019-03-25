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

    mobilityUser = {}
    mobilityUser['routeNumber'] = 0

    chosenkey = apiKeys[initialNumber]

    directionsAPIendpoint = 'https://maps.googleapis.com/maps/api/directions/json?'

    travel_modes = [ "MULTIMODE", "DRIVING", "WALKING", "BICYCLING", "TRANSIT"]
    multimode = False
    for mode in travel_modes:
        if (countRequests >= atualAPILimit or countRequests >= geralAPILIMIT):  #budget limit for each google account
            keyNumber += 1
            chosenkey = apiKeys[keyNumber]

        if (mode == "MULTIMODE"):
            request = directionsAPIendpoint + 'origin={}&destination={}&alternatives=true&key={}'.format(origin,destination,chosenkey)
            multimode = True
        else:
            request = directionsAPIendpoint + 'origin={}&destination={}&mode={}&alternatives=true&key={}'.format(origin, destination, mode, chosenkey)

        response = json.loads(urllib.urlopen(request).read())
        countRequests += 1

        print("\n=== Analyzing routes using the " + mode + " travel mode ===\n")
        # pode nao ter resposta
        if response['status'] == 'OK':
            for route in response['routes']:
                mobilityUser = analyzeLegs(mode, route, mobilityUser, multimode)
                if 'route' in mobilityUser.keys():
                    mobilityUser['route'] = interpolate(mobilityUser, city, userID, commutingtype)
                    sequenceNumber = 0
                    for point in mobilityUser['route']:
                        query = "INSERT INTO public." + city + "_possible_routes (userID, commutingtype, routeNumber, duration, transportModes, latitude, longitude, sequenceNumber) VALUES (" + str(userID) + ", \'" + commutingtype + "\'," + str(mobilityUser['routeNumber']) + "," + str(mobilityUser['duration']) + ", ROW" + str(mobilityUser['transport_modes']) + "," + str(point[0]) + "," + str(point[1]) + "," + str(sequenceNumber) + ")"
                        print(query)
                        cur.execute(query)
                        conn.commit()
                        sequenceNumber += 1
                    print("\n=== Route number " + str(mobilityUser['routeNumber']) + " of user " + str(userID) + " was processed ===\n")
                    mobilityUser['routeNumber'] += 1
                    routesCounter += 1



def analyzeLegs(mode, route, mobilityUser, multimode):
    global exceptions

    differentModes = ()
    routePoints = []
    mobilityUser['route'] = []

    for leg in route['legs']:

        mobilityUser['duration'] = leg['duration']['value']

        previousMode = str(route['legs'][0]['steps'][0]['travel_mode']).encode("UTF-8")


        if previousMode == "TRANSIT":
            previousMode = str(route['legs'][0]['steps'][0]['transit_details']['line']['vehicle']['type']).encode("UTF-8")

        for step in leg['steps']:

            atualMode = str(step['travel_mode']).encode("UTF-8")
            if atualMode == "TRANSIT":
                atualMode = str(step['transit_details']['line']['vehicle']['type']).encode("UTF-8")


            if (previousMode != atualMode or step['travel_mode'] != mode) and multimode is False:
                return {}
            else:
                for j in polyline.decode(step['polyline']['points']):
                    routePoints.append(j)
                if('steps' in step.keys()):
                    for substep in step['steps']:
                        for j in polyline.decode(substep['polyline']['points']):
                            routePoints.append(j)

                        modeTravel = str(substep['travel_mode']).encode("UTF-8")
                        if modeTravel != atualMode:
                            print(modeTravel)
                            print(atualMode)
                            exceptions += 1
                            differentModes = differentModes + (modeTravel,)

            differentModes = differentModes + (previousMode,)

            previousMode = str(step['travel_mode']).encode("UTF-8")
            if previousMode == "TRANSIT":
                previousMode = str(step['transit_details']['line']['vehicle']['type']).encode("UTF-8")


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
    # convert the points to .csv files
    print("Saving the Google Maps API points to CSV file...")
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

    # creating GIS Layer
    print("Creating a GIS Layer from the CSV file...")
    arcpy.MakeXYEventLayer_management(path_csvs + filename1 + ".csv",
                                      "longitude",
                                      "latitude",
                                      filename1 + "_Layer",
                                      arcpy.SpatialReference("WGS 1984"),
                                      "sequence")

    # convert the points to shapefile
    print("Creating a shapefile of the route points...")
    path_shapefile1 = "C:/Users/Joel/Documents/ArcGIS/" + city + "/" + commutingtype + "/non_interpolated_route_points_shapefiles/"
    arcpy.FeatureClassToFeatureClass_conversion(filename1 + "_Layer",
                                                path_shapefile1,
                                                filename1)

    # Execute PointsToLine
    print("Rendering the route line...")
    filename2 = city + "_" + commutingtype + "_" + str(userID) + "_" + transport_modes + "_route_line_" + str(mobilityUser['routeNumber'])
    path_shapefile2 = "C:/Users/Joel/Documents/ArcGIS/" + city + "/" + commutingtype + "/route_lines_shapefiles/"
    arcpy.PointsToLine_management(path_shapefile1 + filename1 + ".shp",
                                  path_shapefile2 + filename2,
                                  "",
                                  "sequence")

    # interpolate the points
    print("Creating a shapefile with the interpolated route points...")
    filename3 = city + "_" + commutingtype + "_" + str(userID) + "_" + transport_modes + "_interpolated_route_points_" + str(mobilityUser['routeNumber'])
    path_shapefile3 = "C:/Users/Joel/Documents/ArcGIS/" + city + "/" + commutingtype + "/interpolated_route_points_shapefiles/"
    arcpy.GeneratePointsAlongLines_management(path_shapefile2 + filename2 + ".shp",
                                              path_shapefile3 + filename3 + ".shp",
                                              'DISTANCE',
                                              Distance='10 meters',
                                              Include_End_Points='END_POINTS')

    # convert the shapefile to layer
    print("Converting the shapefile to a layer...")
    layer = arcpy.MakeFeatureLayer_management(path_shapefile3 + filename3 + ".shp",
                                              filename3)

    # convert layer to points
    print("Obtaining the interpolated points from the layer...")
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
    global keyNumber
    global initialNumber
    global conn
    global usersCounter

    start_time = time.time()

    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('CONNECTION TO THE POSTGRESQL DATABASE...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        cities = ["porto", "lisbon", "coimbra"]
        countCity = 0
        for city in cities:
            countUsers = 0
            query = "SELECT * FROM public.OD" + city + "_users_characterization LIMIT 1"
            cur.execute(query)

            fetched_users = cur.fetchall()

            #DIRECTIONS API
            for i in range(len(fetched_users)):

                userID = str(fetched_users[i][0])
                home_location = str(fetched_users[i][12]) + "," + str(fetched_users[i][13])
                work_location = str(fetched_users[i][15]) + "," + str(fetched_users[i][16])
                min_traveltime_h_w = str(fetched_users[i][19])
                min_traveltime_w_h = str(fetched_users[i][25])

                if (min_traveltime_h_w != "None"):
                    calculate_routes(home_location, work_location, city, userID, "H_W")

                if (min_traveltime_w_h != "None"):
                    calculate_routes(work_location, home_location, city, userID, "W_H")

                countUsers += 1
                usersCounter += 1
                print("\n==================== User number " + str(countUsers + 1) + " of " + city + " was processed =====================\n")


            countCity += 1
            print("\n==================== The city  of " + city + " was processed =====================\n")


        print("A TOTAL OF " + str(countCity) + " cities were processed.")
        print("A TOTAL OF " + str(routesCounter) + " routes were processed.")
        print("A TOTAL OF " + str(usersCounter) + " users were processed.")
        print("A TOTAL OF " + str(exceptions) + " exceptions were encountered")
        print("A TOTAL OF " + str(countRequests) + " REQUESTS WERE MADE TO DIRECTIONS API, USING " + str(keyNumber-initialNumber+1) + " DIFFERENT API KEYS")

        elapsed_time = time.time() - start_time
        print("EXECUTION TIME: " + str(elapsed_time/60) + " MINUTES")

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('DATABASE CONNECTION CLOSED.')


def main():
    connect()


if __name__ == '__main__':
    main()
