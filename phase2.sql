
DROP TABLE IF EXISTS subset_users;
CREATE TEMPORARY TABLE subset_users AS (

  SELECT *
  FROM users_characterization_final
  -- quality parameters
  WHERE "Call Every x Days (on Average)" <= 16.8
    AND "Average Calls Per Day" <= 7.5
    AND densityhome <= 370
    -- AND "Nº Active Days" is linear so, the greater, the better

);

DROP TABLE IF EXISTS aux_eligibleUsers;
CREATE TEMPORARY TABLE aux_eligibleUsers AS (

  SELECT *
  FROM subset_users
  -- certifying that home and workplace are distinct and in the same municipal
  WHERE home_id != workplace_id
    AND municipalworkplace = municipalhome
    AND (number_intermediatetowers_h_w IS NOT NULL
    OR number_intermediatetowers_w_h IS NOT NULL)
);


DROP TABLE IF EXISTS eligibleUsers;
CREATE TABLE eligibleUsers AS (
    SELECT user_id as id, municipalhome AS Municipal, "Tower Density (Km2 per Cell)" AS TowerDensity, m."Population" AS Population
    FROM (
         SELECT distinct id
         FROM experiment4_2_3_universe
         WHERE (intermediatehome_h_w = 0 AND intermediateworkplace_h_W = 0)
            OR (intermediatehome_w_h = 0 AND intermediateworkplace_W_h = 0)
    ) p

    INNER JOIN aux_eligibleUsers y
    ON id = user_id
    INNER JOIN statsmunicipals m
    ON municipalhome = m.name_2
);
SELECT * FROM statsmunicipals;
DROP TABLE tempaux;
CREATE TEMPORARY TABLE tempaux AS (
  SELECT municipal, count(distinct user_id) AS datasetusers
  FROM (
    SELECT municipalhome AS municipal, user_id
    FROM subset_users t
    WHERE municipalhome IS NOT NULL AND municipalworkplace = municipalhome

    UNION

    SELECT municipalworkplace AS municipal, user_id
    FROM subset_users t
    WHERE municipalworkplace IS NOT NULL AND municipalworkplace = municipalhome

  ) p
  GROUP BY municipal
);

DROP TABLE eligibleUsers_byMunicipal;
CREATE TABLE eligibleUsers_byMunicipal AS (
  SELECT t.municipal, Population, datasetusers,count(distinct ii.id) AS userscommuting, 10/CAST(TowerDensity AS FLOAT) AS "Towers per 10 Km2"
  FROM eligibleUsers ii
  INNER JOIN tempaux t
      ON ii.Municipal = t.municipal
  GROUP BY t.municipal, TowerDensity, Population, datasetusers
  ORDER BY  userscommuting DESC
);

SELECT * FROM eligibleUsers;


SELECT * FROM eligibleUsers_byMunicipal;
CREATE TYPE MODES AS (
  mode1 TEXT,
  mode2 TEXT,
  mode3 TEXT,
  mode4 TEXT
);










SELECT * FROM odgondomar_users_characterization


SELECT * FROM intermediateTowers_H_W_u;

CREATE TABLE frequencies_intermediateTowers_H_W AS (
  SELECT id, cell_id, latitude, longitude, count(date_id) AS frequencia
  FROM intermediateTowers_H_W_u

  INNER JOIN (SELECT user_id FROM odgondomar_users_characterization) y
  ON id = user_id

  GROUP BY id, cell_id, latitude, longitude
);

CREATE TABLE frequencies_intermediateTowers_W_H AS (
  SELECT id, cell_id, latitude, longitude, count(date_id) AS frequencia
  FROM intermediateTowers_W_H

  INNER JOIN (SELECT user_id FROM ODPorto_users_characterization) y
  ON id = user_id

  GROUP BY id, cell_id, latitude, longitude
);






-- number of users that I ended up with
SELECT count (DISTINCT userID) FROM porto_possible_routes;

-- number of exact routes that I need to end up with in distanceScore
SELECT count(*)
FROM (SELECT DISTINCT (userID, commutingType, routeNumber) FROM porto_possible_routes) f;

-- number of exact routes that I need to end up in the final
SELECT count(*)
FROM (SELECT DISTINCT (userID, commutingType, routeNumber) FROM porto_possible_routes) f;

CREATE TABLE possible_routes_byUser AS (
  SELECT userid, sum(qtd) AS routes
  FROM (
      SELECT userid, commutingType, count(DISTINCT routeNumber) AS qtd
      FROM porto_possible_routes
      GROUP BY userid, commutingType
  ) g
  GROUP BY userid
);

-- number of possible routes that need to be filtered
SELECT sum(routes)
FROM (
  SELECT userid, sum(qtd) AS routes
  FROM (
      SELECT userid, commutingType, count(DISTINCT routeNumber) AS qtd
      FROM porto_possible_routes
      GROUP BY userid, commutingType
  ) g
  GROUP BY userid
) i;

-- number of different transport modes detected in general



-- DISTANCES SCORES HOME <-> WORK
/*
ALTER TABLE frequencies_intermediateTowers_H_W ADD COLUMN geom_point_dest GEOMETRY(Point, 4326);
UPDATE frequencies_intermediateTowers_H_W SET geom_point_dest=st_SetSrid(st_MakePoint(longitude, latitude), 4326);

ALTER TABLE frequencies_intermediateTowers_W_H ADD COLUMN geom_point_dest GEOMETRY(Point, 4326);
UPDATE frequencies_intermediateTowers_W_H SET geom_point_dest=st_SetSrid(st_MakePoint(longitude, latitude), 4326);
*/

ALTER TABLE porto_possible_routes ADD COLUMN geom_point_orig GEOMETRY(Point, 4326);
UPDATE porto_possible_routes SET geom_point_orig=st_SetSrid(st_MakePoint(longitude, latitude), 4326);


CREATE TEMPORARY TABLE distancesWeighted AS (

    SELECT f.*, cellID,
           frequencia,
           st_distance(ST_Transform(geom_point_orig, 3857),ST_Transform(geom_point_dest, 3857)) * CAST(1 AS FLOAT)/frequencia AS distanceWeighted
    FROM porto_possible_routes f

    INNER JOIN (SELECT id, cell_id AS cellID, frequencia, geom_point_dest FROM frequencies_intermediateTowers_H_W) g
        ON g.id = userid
        AND commutingtype = 'H_W'

    UNION ALL

    SELECT f.*, cellID,
           frequencia,
           st_distance(ST_Transform(geom_point_orig, 3857),ST_Transform(geom_point_dest, 3857)) * CAST(1 AS FLOAT)/frequencia AS distanceWeighted
    FROM porto_possible_routes f

    INNER JOIN (SELECT id, cell_id AS cellID, frequencia, geom_point_dest FROM frequencies_intermediateTowers_W_H) g
        ON g.id = userid
        AND commutingtype = 'W_H'

);


CREATE TEMPORARY TABLE distanceScores AS(
  SELECT userid,
         commutingtype,
         routenumber,
         avg(averageToIntermediateTowers) AS distanceScore,
         transportmodes, duration
  FROM(
    SELECT userid,
           commutingtype,
           routenumber,
           duration,
           transportmodes,
           latitude,
           longitude,
           avg(distanceWeighted) AS averageToIntermediateTowers
    FROM distancesWeighted
    GROUP BY userid, commutingtype, routenumber, duration, transportmodes, latitude, longitude
  ) h
  GROUP BY userID, commutingType, routenumber, transportmodes, duration
);


-- DURATIONS SCORES HOME <-> WORK

CREATE TEMPORARY TABLE traveltimes_and_durations AS (

    SELECT userID, commutingType, routenumber, transportmodes, duration, travelTime
    FROM porto_possible_routes f

    INNER JOIN (SELECT hwid, minTravelTime_H_W AS travelTime FROM travelTimes_H_W) g
        ON hwid = userid
        AND commutingtype = 'H_W'
    GROUP BY userID, commutingType, routenumber, transportmodes, duration,travelTime

    UNION ALL

    SELECT userID, commutingType, routenumber, transportmodes, duration,travelTime
    FROM porto_possible_routes f

    INNER JOIN (SELECT whid, minTravelTime_W_H AS travelTime FROM travelTimes_W_H) g
        ON whid = userid
        AND commutingtype = 'W_H'

    GROUP BY userID, commutingType, routenumber, transportmodes, duration, travelTime

);

CREATE TEMPORARY TABLE durationsScores AS (
  SELECT *,
          CASE
                WHEN (traveltime-duration) < 0 THEN traveltime-duration
                ELSE 0
          END AS durationscore
  FROM traveltimes_and_durations
);


-- FINAL EXACT ROUTES --
CREATE TEMPORARY TABLE finalScores AS (
    SELECT j.userid, j.commutingtype, j.routenumber, j.transportmodes, j.duration, (CAST(0.7 AS FLOAT)*distanceScore + CAST(0.3 AS FLOAT)*durationscore) AS finalscore
    FROM distanceScores j
    INNER JOIN durationsScores l
    ON     j.userID = l.userID
    AND    j.commutingType = l.commutingType
    AND    j.routenumber = l.routenumber
    AND    j.transportmodes = l.transportmodes
    AND    j.duration = l.duration
);

CREATE TABLE exactRoutes AS (
  SELECT userid, commutingtype, routenumber, transportmodes, duration, finalscore AS minimumScore
  FROM finalScores
  WHERE (userid, commutingType, finalscore) IN (
      SELECT userid, commutingType, min(finalscore)
      FROM finalScores
      GROUP BY userID, commutingType
  )
);

SELECT * FROM porto;


-- number of different transport modes detected (including multimode configurations)

-- how many use driving, walking, bicycling in general

-- how many use driving, walking, bicycling (including multimode configurations)

-- how many use driving, walking, bicycling in general on their way H->W

-- how many use driving, walking, bicycling (including multimode configurations) on their way H->W

-- how many use driving, walking, bicycling in general on their way W->H

-- how many use driving, walking, bicycling (including multimode configurations) on their way W->H