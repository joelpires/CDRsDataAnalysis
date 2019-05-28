------------------------------------- DETERMINING THE RIGHT SUBSET -------------------------------------------------
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


---------------------------- DETERMINING THE ELIGIBLE USERS FROM THE SUBSET ------------------------------------

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

DROP TABLE IF EXISTS eligibleUsers_full;
CREATE TABLE eligibleUsers_full AS (
    SELECT user_id as id, municipalhome AS Municipal, "Tower Density (Km2 per Cell)" AS TowerDensity, m."Population" AS Population, y.number_intermediatetowers_h_w + y.number_intermediatetowers_w_h AS number_intermediatetowers
    FROM (
         SELECT distinct id
         FROM experiment4_2_3_universe
         WHERE (intermediatehome_h_w = 0 AND intermediateworkplace_h_W = 0)
            AND (intermediatehome_w_h = 0 AND intermediateworkplace_W_h = 0)
    ) p

    INNER JOIN aux_eligibleUsers y
    ON id = user_id
    INNER JOIN statsmunicipals m
    ON municipalhome = m.name_2
);


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

DROP TABLE eligibleUsers_full_byMunicipal;
CREATE TABLE eligibleUsers_full_byMunicipal AS (
  SELECT t.municipal, Population, datasetusers,count(distinct ii.id) AS userscommuting, 10/CAST(TowerDensity AS FLOAT) AS "Towers per 10 Km2"
  FROM eligibleUsers_full ii
  INNER JOIN tempaux t
      ON ii.Municipal = t.municipal
  GROUP BY t.municipal, TowerDensity, Population, datasetusers
  ORDER BY  userscommuting DESC
);

CREATE TYPE MODES AS (
  mode1 TEXT,
  mode2 TEXT,
  mode3 TEXT,
  mode4 TEXT
);


---------------------------- DETERMINING THE FINAL SCORE AND EXACT ROUTES -------------------------------------------------
CREATE TEMPORARY TABLE temp AS (
  SELECT *
  FROM users_characterization_final
  WHERE user_id IN (SELECT id FROM eligibleUsers)
);


CREATE TABLE frequencies_intermediateTowers_H_W AS (
  SELECT intermediatetowers_h_wid, tower, latitude, longitude, count(*) AS frequencia
  FROM intermediateTowers_H_W_u

  INNER JOIN temp
  ON intermediatetowers_h_wid = user_id

  GROUP BY intermediatetowers_h_wid, tower, latitude, longitude
);

CREATE TABLE frequencies_intermediateTowers_W_H AS (
  SELECT intermediatetowers_w_hid, tower, latitude, longitude, count(*) AS frequencia
  FROM intermediateTowers_W_H_u

  INNER JOIN temp
  ON intermediatetowers_w_hid = user_id

  GROUP BY intermediatetowers_w_hid, tower, latitude, longitude
);

ALTER TABLE frequencies_intermediateTowers_H_W ADD COLUMN geom_point_dest GEOMETRY(Point, 4326);
UPDATE frequencies_intermediateTowers_H_W SET geom_point_dest=st_SetSrid(st_MakePoint(longitude, latitude), 4326);

ALTER TABLE frequencies_intermediateTowers_W_H ADD COLUMN geom_point_dest GEOMETRY(Point, 4326);
UPDATE frequencies_intermediateTowers_W_H SET geom_point_dest=st_SetSrid(st_MakePoint(longitude, latitude), 4326);

/*

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

    INNER JOIN (SELECT hwid, minTravelTime_H_W AS travelTime FROM new_traveltimes_h_w_u) g
        ON hwid = userid
        AND commutingtype = 'H_W'
    GROUP BY userID, commutingType, routenumber, transportmodes, duration,travelTime

    UNION ALL

    SELECT userID, commutingType, routenumber, transportmodes, duration,travelTime
    FROM porto_possible_routes f

    INNER JOIN (SELECT whid, minTravelTime_W_H AS travelTime FROM new_traveltimes_w_h_u) g
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
  SELECT userid, commutingtype, routenumber, transportmodes, duration, finalscore AS score
  FROM finalScores
  WHERE (userid, commutingType, finalscore) IN (
      SELECT userid, commutingType, min(finalscore)
      FROM finalScores
      GROUP BY userID, commutingType
  )
);

CREATE TABLE finalRoutes_porto AS (
  SELECT g.*
  FROM porto_de_mos_possible_routes g, exactRoutes_porto f
  WHERE f.userid = g.userid
    AND f.commutingtype = g.commutingtype
    AND f.routenumber = g.routenumber
);


*/


INSERT INTO public.finalscores_Lisboa (userID, commutingType, routenumber, transportmodes, duration, finalscore)
    (SELECT j.userid, j.commutingtype, j.routenumber, j.transportmodes, j.duration, (distanceScore*durationscore)
     FROM (     SELECT userid, commutingtype, routenumber, avg(averageToIntermediateTowers) AS distanceScore, transportmodes, duration
                FROM (
                      SELECT userid, commutingtype, routenumber, duration, transportmodes, latitude, longitude, avg(distanceWeighted) AS averageToIntermediateTowers
                      FROM (  SELECT userID, commutingType, routeNumber, duration, transportModes, latitude, longitude, sequenceNumber, cellID, frequencia, st_distance(ST_Transform(geom_point_orig, 3857),ST_Transform(geom_point_dest, 3857)) * CAST(1 AS FLOAT)/frequencia AS distanceWeighted
                      FROM (SELECT * FROM public.Lisboa_possible_routes f WHERE userID = 23646673) f
                      INNER JOIN (SELECT intermediatetowers_h_wid, tower AS cellID, frequencia, geom_point_dest FROM public.frequencies_intermediateTowers_H_W) g
                              ON g.intermediatetowers_h_wid = userid AND commutingtype = 'H_W'

                      UNION ALL

                      SELECT userID, commutingType, routeNumber, duration, transportModes, latitude, longitude, sequenceNumber, cellID, frequencia, st_distance(ST_Transform(geom_point_orig, 3857),ST_Transform(geom_point_dest, 3857)) * CAST(1 AS FLOAT)/frequencia AS distanceWeighted
                      FROM (SELECT * FROM public.Lisboa_possible_routes f WHERE userID = 23646673) f
                      INNER JOIN (SELECT intermediatetowers_w_hid, tower AS cellID, frequencia, geom_point_dest FROM public.frequencies_intermediateTowers_W_H) g
                      ON g.intermediatetowers_w_hid = userid AND commutingtype = 'W_H'
                ) i
                GROUP BY userid, commutingtype, routenumber, duration, transportmodes, latitude, longitude ) h
     GROUP BY userID, commutingType, routenumber, transportmodes, duration
     ) j
     INNER JOIN (SELECT *, CASE WHEN (traveltime-duration) < 0 THEN abs(traveltime-duration) ELSE 1 END AS durationscore
                 FROM (
                        SELECT userID, commutingType, routenumber, transportmodes, duration, travelTime
                        FROM (SELECT * FROM public.Lisboa_possible_routes f WHERE userID = 23646673) f
                        INNER JOIN (SELECT hwid, minTravelTime_H_W AS travelTime FROM new_traveltimes_h_w_u) g
                                ON hwid = userid AND commutingtype = 'H_W'
                        GROUP BY userID, commutingType, routenumber, transportmodes, duration,travelTime

                        UNION ALL

                        SELECT userID, commutingType, routenumber, transportmodes, duration,travelTime
                        FROM (SELECT * FROM public.Lisboa_possible_routes f WHERE userID = 23646673) f
                        INNER JOIN (SELECT whid, minTravelTime_W_H AS travelTime FROM new_traveltimes_w_h_u) g
                                ON whid = userid AND commutingtype = 'W_H'
                        GROUP BY userID, commutingType, routenumber, transportmodes, duration, travelTime


                 ) t) l
     ON     j.userID = l.userID
     AND    j.commutingType = l.commutingType
     AND    j.routenumber = l.routenumber
     AND    j.transportmodes = l.transportmodes
     AND    j.duration = l.duration
)




DELETE FROM finalroutes_Lisboa;
INSERT INTO public.finalroutes_Lisboa (userID, commutingType, routeNumber, duration, transportModes, latitude, longitude, sequenceNumber, geom_point_orig)
(SELECT g.*
 FROM (SELECT * FROM public.Lisboa_possible_routes WHERE userID = 23646673) g, (SELECT userid, commutingtype, routenumber, transportmodes, duration, finalscore
                                                                                FROM (SELECT * FROM public.finalscores_Lisboa WHERE userID = 23646673) l
                                                                                WHERE (userid, commutingType, finalscore) IN ( SELECT userid, commutingType, min(finalscore)
                                                                                                                                FROM (SELECT DISTINCT ON (finalscore) * FROM public.finalscores_Lisboa WHERE userID = 23646673) o
                                                                                                                                GROUP BY userID, commutingType)
                                                                               ) f
 WHERE f.userid = g.userid
   AND f.commutingtype = g.commutingtype
   AND f.routenumber = g.routenumber
);


/*
SELECT * FROM public.eligibleUsers_full_byMunicipal WHERE municipal = 'Viana do Castelo' OR municipal = 'Braga' OR municipal = 'Porto' OR municipal = 'Vila Real'
OR municipal = 'Bragança'
OR municipal = 'Aveiro'
OR municipal = 'Viseu'
OR municipal = 'Guarda'
OR municipal = 'Coimbra'
OR municipal = 'Castelo Branco'
OR municipal = 'Leiria'
OR municipal = 'Santarém'
OR municipal = 'Lisboa'
OR municipal = 'Portalegre'
OR municipal = 'Évora'
OR municipal = 'Setúbal'
OR municipal = 'Beja'
OR municipal = 'Faro'
ORDER BY "Towers per 10 Km2" DESC, userscommuting DESC;

CREATE TEMPORARY TABLE final AS (
  SELECT *
  FROM public.eligibleUsers_full_byMunicipal
  ORDER BY "Towers per 10 Km2" DESC, userscommuting DESC
);
*/


-- 25 municipios (18 capitais de distrito + 7 municipios à escolha - 3 do porto: Matosinhos, Maia e Vila Nova de Gaia; e 3 de lisboa: Amadora, Oeiras, Odivelas)
-- criterios de escolha: cobrir portugal inteiro | ordenar municípios por densidade de torres, e depois por numero de userscommuting
-- foram inferenciados pessoas que tinham full commuting patterns
-- as pessoas escolhidas tinham maior numero de somatório de torres intermedias ativas h_w e w_h
-- LISBOA, PORTO, COIMBRA, BRAGA são os primeiros a ser analisados (500 pessoas de cada) -- 2000
-- AVEIRO, SETUBAL, BRAGANçA, VILA REAL, LEIRIA, VIANA DO CASTELO E VISEU -> 300 -- 2400
-- CASTELO BRANCO, SANTAREM, FARO, EVORA -> 150  -- 450
-- GUARDA, BEJA -> 100  -- 200
-- PORTOALEGRE -> 97  -- 97
-- no resto dos municípios é põr um numero redondo de pessoas (no minimo ~100 pessoas como no caso de Portalegre) - 5247 pessoas no total
-- previsao de 21 dias para acabar

DROP TABLE IF EXISTS final_eligibleUsers;
CREATE TABLE final_eligibleUsers AS (
  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Lisboa' ORDER BY number_intermediatetowers DESC LIMIT 500) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Porto' ORDER BY number_intermediatetowers DESC LIMIT 500) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Coimbra' ORDER BY number_intermediatetowers DESC LIMIT 500) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Braga' ORDER BY number_intermediatetowers DESC LIMIT 500) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Aveiro' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Setúbal' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Bragança' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Vila Real' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Leiria' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Viana do Castelo' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Viseu' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Castelo Branco' ORDER BY number_intermediatetowers DESC LIMIT 150) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Santarém' ORDER BY number_intermediatetowers DESC LIMIT 150) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Faro' ORDER BY number_intermediatetowers DESC LIMIT 150) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Évora' ORDER BY number_intermediatetowers DESC LIMIT 150) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Beja' ORDER BY number_intermediatetowers DESC LIMIT 100) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Guarda' ORDER BY number_intermediatetowers DESC LIMIT 100) u

  UNION ALL

  SELECT * FROM (SELECT * FROM eligibleUsers_full  WHERE municipal = 'Portalegre' ORDER BY number_intermediatetowers DESC LIMIT 97) u

);

DROP TABLE final_eligibleUsers_byMunicipal;
CREATE TABLE final_eligibleUsers_byMunicipal AS (
  SELECT t.municipal, Population, datasetusers,count(distinct ii.id) AS userscommuting, 10/CAST(TowerDensity AS FLOAT) AS "Towers per 10 Km2"
  FROM final_eligibleUsers ii
  INNER JOIN tempaux t
      ON ii.Municipal = t.municipal
  GROUP BY t.municipal, TowerDensity, Population, datasetusers
  ORDER BY  userscommuting DESC, t.municipal DESC
);





SELECT * FROM finalscores_lisboa;
SELECT * FROM finalroutes_lisboa;




---------------------- --- STATISTICS AND VISUALIZATIONS ------------ ------------ ------------ ------------ ------------

-- know the number of different final routes written
SELECT count(*) FROM finalroutes_lisboa WHERE sequencenumber = 0;

-- EM LISBOA HOUVE UMA DECISAO QUE SE TEVE DE ESCOLHER TRAVEL MODES ALEATORIAMENTE (ISTO È, HOUVE MAIS QUE UMA ROTA COM MODOS DE TRANSPORTE DIFERENTES MAS COM FINALSCORE)
SELECT count(*) FROM (SELECT DISTINCT ON (userid, commutingtype, (transportmodes).mode1, (transportmodes).mode2, (transportmodes).mode3, (transportmodes).mode4) * FROM finalroutes_lisboa) t ;

-- EM PORTO HOUVE quatro DECISões QUE SE TEVE DE ESCOLHER TRAVEL MODES ALEATORIAMENTE (ISTO È, HOUVE MAIS QUE UMA ROTA COM MODOS DE TRANSPORTE DIFERENTES MAS COM FINALSCORE)
SELECT count(*) FROM (SELECT DISTINCT ON (userid, commutingtype, (transportmodes).mode1, (transportmodes).mode2, (transportmodes).mode3, (transportmodes).mode4) * FROM finalroutes_porto) t ;

-- EM Coimbra NAO HOUVE UMA DECISAO QUE SE TEVE DE ESCOLHER TRAVEL MODES ALEATORIAMENTE (ISTO È, HOUVE MAIS QUE UMA ROTA COM MODOS DE TRANSPORTE DIFERENTES MAS COM FINALSCORE)
SELECT count(*) FROM (SELECT DISTINCT ON (userid, commutingtype, (transportmodes).mode1, (transportmodes).mode2, (transportmodes).mode3, (transportmodes).mode4) * FROM finalroutes_coimbra) t ;

DROP TABLE new_finalRoutes_porto;
CREATE TEMPORARY TABLE new_finalRoutes_porto AS (
  SELECT *
  FROM porto_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM finalroutes_porto)

);

DROP TABLE new_finalRoutes_lisboa;
CREATE TEMPORARY TABLE new_finalRoutes_lisboa AS (
  SELECT *
  FROM lisboa_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM finalroutes_lisboa)

);

DROP TABLE new_finalRoutes_coimbra;
CREATE TEMPORARY TABLE new_finalRoutes_coimbra AS (
  SELECT *
  FROM coimbra_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM finalroutes_coimbra)

);



-- different transport modes/combinations in the final routes
SELECT count(distinct transportmodes) FROM new_finalRoutes_lisboa;
SELECT DISTINCT ON (transportmodes) * FROM new_finalRoutes_lisboa;

SELECT count(distinct transportmodes) FROM new_finalroutes_porto;
SELECT DISTINCT ON (transportmodes) * FROM new_finalroutes_porto;

SELECT count(distinct transportmodes) FROM new_finalroutes_coimbra;
SELECT DISTINCT ON (transportmodes) * FROM new_finalroutes_coimbra;


-- different transport modes/combinations in the possible routes
SELECT count(distinct transportmodes) FROM lisboa_possible_routes;
SELECT DISTINCT ON (transportmodes) * FROM lisboa_possible_routes; -- tem bus sozinho

SELECT count(distinct transportmodes) FROM porto_possible_routes;
SELECT DISTINCT ON (transportmodes) * FROM porto_possible_routes; -- nao tem bus sozinho

SELECT count(distinct transportmodes) FROM coimbra_possible_routes;
SELECT DISTINCT ON (transportmodes) * FROM coimbra_possible_routes; -- nao tem bus


-- how many final routes are in lisbon using unimodal driving, walking, bus, subway and train
SELECT count(*) FROM new_finalRoutes_lisboa WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'DRIVING' AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';

SELECT count(*) FROM new_finalRoutes_lisboa WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
SELECT count(*) FROM new_finalRoutes_lisboa WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 = 'SUBWAY' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
SELECT count(*) FROM new_finalRoutes_lisboa WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 = 'COMMUTER_TRAIN' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';

-- how many final routes are in lisbon using multimodal
SELECT count(*) FROM new_finalRoutes_lisboa WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'BUS' AND (transportmodes).mode2 = 'WALKING' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
SELECT count(*) FROM new_finalRoutes_lisboa WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 = 'COMMUTER_TRAIN' AND (transportmodes).mode3 = 'SUBWAY' AND (transportmodes).mode4 = '';
SELECT count(*) FROM new_finalRoutes_lisboa WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'BUS' AND (transportmodes).mode2 = 'WALKING' AND (transportmodes).mode3 = 'COMMUTER_TRAIN' AND (transportmodes).mode4 = '';
SELECT count(*) FROM new_finalRoutes_lisboa WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'BUS' AND (transportmodes).mode2 = 'WALKING' AND (transportmodes).mode3 = 'COMMUTER_TRAIN' AND (transportmodes).mode4 = 'SUBWAY';
SELECT count(*) FROM new_finalRoutes_lisboa WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'BUS' AND (transportmodes).mode2 = 'WALKING' AND (transportmodes).mode3 = 'SUBWAY' AND (transportmodes).mode4 = '';


-- how many possible routes are in lisbon using unimodal driving, walking, bus, subway and train
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'DRIVING' AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'BUS' AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 = 'SUBWAY' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 = 'COMMUTER_TRAIN' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';

-- how many possible routes are in lisbon using multimodal
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'BUS' AND (transportmodes).mode2 = 'WALKING' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 = 'COMMUTER_TRAIN' AND (transportmodes).mode3 = 'SUBWAY' AND (transportmodes).mode4 = '';
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'BUS' AND (transportmodes).mode2 = 'WALKING' AND (transportmodes).mode3 = 'COMMUTER_TRAIN' AND (transportmodes).mode4 = '';
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'BUS' AND (transportmodes).mode2 = 'WALKING' AND (transportmodes).mode3 = 'COMMUTER_TRAIN' AND (transportmodes).mode4 = 'SUBWAY';
SELECT count(*) FROM lisboa_possible_routes WHERE sequencenumber = 0 AND (transportmodes).mode1 = 'BUS' AND (transportmodes).mode2 = 'WALKING' AND (transportmodes).mode3 = 'SUBWAY' AND (transportmodes).mode4 = '';


"(BUS,"""","""","""")"
"(BUS,WALKING,"""","""")"
"(BUS,WALKING,COMMUTER_TRAIN,"""")"
"(BUS,WALKING,COMMUTER_TRAIN,SUBWAY)"
"(BUS,WALKING,HEAVY_RAIL,"""")"
"(BUS,WALKING,SUBWAY,"""")"
"(DRIVING,"""","""","""")"
"(HEAVY_RAIL,WALKING,SUBWAY,"""")"
"(WALKING,"""","""","""")"
"(WALKING,COMMUTER_TRAIN,"""","""")"
"(WALKING,COMMUTER_TRAIN,HEAVY_RAIL,"""")"
"(WALKING,COMMUTER_TRAIN,SUBWAY,"""")"
"(WALKING,SUBWAY,"""","""")"

(TRAM,WALKING,"","")
(WALKING,HEAVY_RAIL,"","")

CREATE TEMPORARY TABLE new_porto_possible_routes AS(
  SELECT *
  FROM porto_possible_routes
  WHERE sequencenumber = 0
);

CREATE TEMPORARY TABLE new_lisboa_possible_routes AS(
  SELECT *
  FROM lisboa_possible_routes
  WHERE sequencenumber = 0
);

CREATE TEMPORARY TABLE new_coimbra_possible_routes AS(
  SELECT *
  FROM coimbra_possible_routes
  WHERE sequencenumber = 0
);

ALTER TABLE new_porto_possible_routes ADD COLUMN city TEXT;
UPDATE new_porto_possible_routes SET city='Porto';

ALTER TABLE new_lisboa_possible_routes ADD COLUMN city TEXT;
UPDATE new_lisboa_possible_routes SET city='Lisboa';

ALTER TABLE new_coimbra_possible_routes ADD COLUMN city TEXT;
UPDATE new_coimbra_possible_routes SET city='Coimbra';

DROP TABLE IF EXISTS all_possible_routes;
CREATE TEMPORARY TABLE all_possible_routes AS (
  SELECT *
  FROM new_porto_possible_routes

  UNION ALL

  SELECT *
  FROM new_lisboa_possible_routes

  UNION ALL

  SELECT *
  FROM new_coimbra_possible_routes

);

CREATE TEMPORARY TABLE aux_finalRoutes_lisboa AS(
  SELECT *
  FROM new_finalRoutes_lisboa
  WHERE sequencenumber = 0
);

CREATE TEMPORARY TABLE aux_finalRoutes_porto AS(
  SELECT *
  FROM new_finalRoutes_porto
  WHERE sequencenumber = 0
);

CREATE TEMPORARY TABLE aux_finalRoutes_coimbra AS(
  SELECT *
  FROM new_finalRoutes_coimbra
  WHERE sequencenumber = 0
);

ALTER TABLE aux_finalRoutes_porto ADD COLUMN city TEXT;
UPDATE aux_finalRoutes_porto SET city='Porto';

ALTER TABLE aux_finalRoutes_lisboa ADD COLUMN city TEXT;
UPDATE aux_finalRoutes_lisboa SET city='Lisboa';

ALTER TABLE aux_finalRoutes_coimbra ADD COLUMN city TEXT;
UPDATE aux_finalRoutes_coimbra SET city='Coimbra';

DROP TABLE IF EXISTS all_finalroutes;
CREATE TEMPORARY TABLE all_finalroutes AS (
  SELECT *
  FROM aux_finalRoutes_lisboa

  UNION ALL

  SELECT *
  FROM aux_finalRoutes_porto

  UNION ALL

  SELECT *
  FROM aux_finalRoutes_coimbra

);


-- === POSSIBLE AND FINAL ROUTES ==
  --  === HOME TO WORK; WORK TO HOME TRAJECTORIES; OVERALL === ----

  -- how many mobility combinations are possible: 15
  SELECT count(distinct transportmodes) FROM all_possible_routes;
  SELECT DISTINCT ON (transportmodes) * FROM all_possible_routes;


  -- how many unimodal possible routes: - 3: bus, driving, walking
  SELECT count(distinct transportmodes) FROM all_possible_routes WHERE (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
  SELECT DISTINCT ON (transportmodes) * FROM all_possible_routes WHERE (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';

  -- how many multimodal possible routes: - 12: ...
  SELECT count(distinct transportmodes) FROM all_possible_routes WHERE (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 != '';
  SELECT DISTINCT ON (transportmodes) * FROM all_possible_routes WHERE (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 != '';


  -- how many private transport modes: driving

  -- how many public transport modes: bus, commuter_train, heavy_rail, tram, subway

  -- how many non-poluent transport modes: bicycle, walking

  -- how many poluent transport modes: driving, bus, commuter_train, heavy_rail, tram, subway

  -- percentages and rations comparison

-- percentages and rations comparison



-- preparing all possible routes: we have to eliminate walking as a multimodal travel mode combination
UPDATE all_possible_routes
SET transportmodes.mode1 = (transportmodes).mode2, transportmodes.mode2 = (transportmodes).mode3, transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 != '';

UPDATE all_possible_routes
SET transportmodes.mode2 = (transportmodes).mode3, transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode2 = 'WALKING';

UPDATE all_possible_routes
SET transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode3 = 'WALKING';

UPDATE all_possible_routes
SET transportmodes.mode4 = ''
WHERE (transportmodes).mode4 = 'WALKING';


DROP TABLE IF EXISTS prep_all_possible_routes;

CREATE TEMPORARY TABLE prep_all_possible_routes AS (
  SELECT *,   CASE
              WHEN (transportmodes).mode1 = 'DRIVING' AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode3 = '' THEN 1 -- and bicycling
              ELSE 0
            END AS private,     CASE
                                WHEN (transportmodes).mode1 = 'BUS' OR (transportmodes).mode2 = 'BUS' OR (transportmodes).mode3 = 'BUS' OR (transportmodes).mode4 = 'BUS'
                                    OR (transportmodes).mode1 = 'COMMUTER_TRAIN' OR (transportmodes).mode2 = 'COMMUTER_TRAIN' OR (transportmodes).mode3 = 'COMMUTER_TRAIN' OR (transportmodes).mode4 = 'COMMUTER_TRAIN'
                                    OR (transportmodes).mode1 = 'TRAM' OR (transportmodes).mode2 = 'TRAM' OR (transportmodes).mode3 = 'TRAM' OR (transportmodes).mode4 = 'TRAM'
                                    OR (transportmodes).mode1 = 'HEAVY_RAIL' OR (transportmodes).mode2 = 'HEAVY_RAIL' OR (transportmodes).mode3 = 'HEAVY_RAIL' OR (transportmodes).mode4 = 'HEAVY_RAIL'
                                    OR (transportmodes).mode1 = 'SUBWAY' OR (transportmodes).mode2 = 'SUBWAY' OR (transportmodes).mode3 = 'SUBWAY' OR (transportmodes).mode4 = 'SUBWAY'
                                THEN 1
                                ELSE 0
                              END AS public,  CASE
                                              WHEN (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '' THEN 1
                                              ELSE 0
                                            END AS unimodal
  FROM all_possible_routes
);


-- preparing all final routes: we have to eliminate walking as a multimodal travel mode combination
UPDATE all_finalroutes
SET transportmodes.mode1 = (transportmodes).mode2, transportmodes.mode2 = (transportmodes).mode3, transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 != '';

UPDATE all_finalroutes
SET transportmodes.mode2 = (transportmodes).mode3, transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode2 = 'WALKING';

UPDATE all_finalroutes
SET transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode3 = 'WALKING';

UPDATE all_finalroutes
SET transportmodes.mode4 = ''
WHERE (transportmodes).mode4 = 'WALKING';


DROP TABLE IF EXISTS prep_all_final_routes;
CREATE TEMPORARY TABLE prep_all_final_routes AS (
  SELECT *,   CASE
              WHEN (transportmodes).mode1 = 'DRIVING' AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode3 = '' THEN 1 -- and bicycling
              ELSE 0
            END AS private,     CASE
                                WHEN (transportmodes).mode1 = 'BUS' OR (transportmodes).mode2 = 'BUS' OR (transportmodes).mode3 = 'BUS' OR (transportmodes).mode4 = 'BUS'
                                    OR (transportmodes).mode1 = 'COMMUTER_TRAIN' OR (transportmodes).mode2 = 'COMMUTER_TRAIN' OR (transportmodes).mode3 = 'COMMUTER_TRAIN' OR (transportmodes).mode4 = 'COMMUTER_TRAIN'
                                    OR (transportmodes).mode1 = 'TRAM' OR (transportmodes).mode2 = 'TRAM' OR (transportmodes).mode3 = 'TRAM' OR (transportmodes).mode4 = 'TRAM'
                                    OR (transportmodes).mode1 = 'HEAVY_RAIL' OR (transportmodes).mode2 = 'HEAVY_RAIL' OR (transportmodes).mode3 = 'HEAVY_RAIL' OR (transportmodes).mode4 = 'HEAVY_RAIL'
                                    OR (transportmodes).mode1 = 'SUBWAY' OR (transportmodes).mode2 = 'SUBWAY' OR (transportmodes).mode3 = 'SUBWAY' OR (transportmodes).mode4 = 'SUBWAY'
                                THEN 1
                                ELSE 0
                              END AS public,  CASE
                                              WHEN (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '' THEN 1
                                              ELSE 0
                                            END AS unimodal
  FROM all_finalroutes
);


DROP TABLE IF EXISTS possibleroutes_stats_typemode_commutingtype;
CREATE TEMPORARY TABLE aux_possibleroutes_stats_typemode_commutingtype AS (
   SELECT city, commutingtype, sum(unimodal) AS freqUnimodal, count(*)-sum(unimodal) AS freqmultimodal, sum(public) AS freqPublic, sum(private) AS freqprivate, count(*) AS TOTAL
   FROM prep_all_possible_routes
   GROUP BY city, commutingtype
);

DROP TABLE IF EXISTS possibleroutes_stats_typemode_commutingtype;
CREATE TEMPORARY TABLE possibleroutes_stats_typemode_commutingtype AS (
  SELECT city, commutingtype, CAST(sum(unimodal) AS FLOAT)*100/count(*) AS percentUnimodal, CAST(count(*)-sum(unimodal) AS FLOAT)*100/count(*) AS percentmultimodal, CAST(sum(public) AS FLOAT)*100/count(*) AS percentPublic, CAST(sum(private) AS FLOAT)*100/count(*) AS percentPrivate
  FROM prep_all_possible_routes
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, 'H_W_H' AS commutingtype, CAST(sum(freqUnimodal) AS FLOAT)*100/sum(TOTAL) AS percentUnimodal, CAST(sum(freqmultimodal) AS FLOAT)*100/sum(TOTAL) AS percentMultimodal, CAST(sum(freqPublic) AS FLOAT)*100/sum(TOTAL) AS percentPublic, CAST(sum(freqprivate) AS FLOAT)*100/sum(TOTAL) AS percentPrivate
  FROM aux_possibleroutes_stats_typemode_commutingtype t
  GROUP BY city
);


DROP TABLE IF EXISTS aux_possibleroutes_stats_travel_modes_commutingtype;
CREATE TEMPORARY TABLE aux_possibleroutes_stats_travel_modes_commutingtype AS (

  SELECT city, commutingtype, CAST(transportmodes AS TEXT), count(*) AS freq
  FROM prep_all_possible_routes
  GROUP BY city, commutingtype, transportmodes

  UNION ALL

  SELECT city, commutingtype, 'BUS IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'BUS' OR (transportmodes).mode2 = 'BUS' OR (transportmodes).mode3 = 'BUS' OR (transportmodes).mode4 = 'BUS'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'COMMUTER_TRAIN IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'COMMUTER_TRAIN' OR (transportmodes).mode2 = 'COMMUTER_TRAIN' OR (transportmodes).mode3 = 'COMMUTER_TRAIN' OR (transportmodes).mode4 = 'COMMUTER_TRAIN'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'HEAVY_RAIL IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'HEAVY_RAIL' OR (transportmodes).mode2 = 'HEAVY_RAIL' OR (transportmodes).mode3 = 'HEAVY_RAIL' OR (transportmodes).mode4 = 'HEAVY_RAIL'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'TRAM IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'TRAM' OR (transportmodes).mode2 = 'TRAM' OR (transportmodes).mode3 = 'TRAM' OR (transportmodes).mode4 = 'TRAM'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'SUBWAY IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'SUBWAY' OR (transportmodes).mode2 = 'SUBWAY' OR (transportmodes).mode3 = 'SUBWAY' OR (transportmodes).mode4 = 'SUBWAY'
  GROUP BY city, commutingtype

);



CREATE TEMPORARY TABLE possibleroutes_stats_travel_modes_city AS (

  SELECT u.city, u.commutingtype, u.transportmodes, CAST(freq AS FLOAT)*100/total AS percentage
  FROM aux_possibleroutes_stats_travel_modes_commutingtype u
  INNER JOIN (SELECT city,commutingtype , sum(freq) AS total
              FROM aux_possibleroutes_stats_travel_modes_commutingtype
              GROUP BY city, commutingtype) p
  ON u.city = p.city
  AND u.commutingtype = p.commutingtype
  GROUP BY u.city, u.commutingtype,u.transportmodes, u.freq, total

  UNION ALL

  SELECT u.city, 'H_W_H' AS commutingtype, u.transportmodes, CAST(frequi AS FLOAT)*100/total AS percentage
  FROM aux_possibleroutes_stats_travel_modes_commutingtype u
  INNER JOIN (SELECT city, sum(freq) AS total
              FROM aux_possibleroutes_stats_travel_modes_commutingtype
              GROUP BY city) y
  ON u.city = y.city
  INNER JOIN (SELECT city, transportmodes, sum(freq) AS frequi
              FROM aux_possibleroutes_stats_travel_modes_commutingtype
              GROUP BY city, transportmodes) p
  ON u.city = p.city
  AND u.transportmodes = p.transportmodes
  GROUP BY u.city, u.transportmodes,frequi, total

);


--- final routes ---

CREATE TEMPORARY TABLE aux_finalroutes_stats_typemode_commutingtype AS (
   SELECT city, commutingtype, sum(unimodal) AS freqUnimodal, count(*)-sum(unimodal) AS freqmultimodal, sum(public) AS freqPublic, sum(private) AS freqprivate, count(*) AS TOTAL
   FROM prep_all_possible_routes
   GROUP BY city, commutingtype
);

DROP TABLE IF EXISTS aux_finalroutes_stats_travel_modes_commutingtype;
CREATE TEMPORARY TABLE aux_finalroutes_stats_travel_modes_commutingtype AS (

  SELECT city, commutingtype, CAST(transportmodes AS TEXT), count(*) AS freq
  FROM prep_all_final_routes
  GROUP BY city, commutingtype, transportmodes

  UNION ALL

  SELECT city, commutingtype, 'BUS IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_final_routes
  WHERE (transportmodes).mode1 = 'BUS' OR (transportmodes).mode2 = 'BUS' OR (transportmodes).mode3 = 'BUS' OR (transportmodes).mode4 = 'BUS'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'COMMUTER_TRAIN IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_final_routes
  WHERE (transportmodes).mode1 = 'COMMUTER_TRAIN' OR (transportmodes).mode2 = 'COMMUTER_TRAIN' OR (transportmodes).mode3 = 'COMMUTER_TRAIN' OR (transportmodes).mode4 = 'COMMUTER_TRAIN'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'HEAVY_RAIL IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_final_routes
  WHERE (transportmodes).mode1 = 'HEAVY_RAIL' OR (transportmodes).mode2 = 'HEAVY_RAIL' OR (transportmodes).mode3 = 'HEAVY_RAIL' OR (transportmodes).mode4 = 'HEAVY_RAIL'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'TRAM IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_final_routes
  WHERE (transportmodes).mode1 = 'TRAM' OR (transportmodes).mode2 = 'TRAM' OR (transportmodes).mode3 = 'TRAM' OR (transportmodes).mode4 = 'TRAM'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'SUBWAY IN GENERAL' AS transportmodes, count(*) AS freq
  FROM prep_all_final_routes
  WHERE (transportmodes).mode1 = 'SUBWAY' OR (transportmodes).mode2 = 'SUBWAY' OR (transportmodes).mode3 = 'SUBWAY' OR (transportmodes).mode4 = 'SUBWAY'
  GROUP BY city, commutingtype

);



CREATE TEMPORARY TABLE finalroutes_stats_travel_modes_city AS (

  SELECT u.city, u.commutingtype, u.transportmodes, CAST(freq AS FLOAT)*100/total AS percentage
  FROM aux_finalroutes_stats_travel_modes_commutingtype u
  INNER JOIN (SELECT city,commutingtype , sum(freq) AS total
              FROM aux_finalroutes_stats_travel_modes_commutingtype
              GROUP BY city, commutingtype) p
  ON u.city = p.city
  AND u.commutingtype = p.commutingtype
  GROUP BY u.city, u.commutingtype,u.transportmodes, u.freq, total

  UNION ALL

  SELECT u.city, 'H_W_H' AS commutingtype, u.transportmodes, CAST(frequi AS FLOAT)*100/total AS percentage
  FROM aux_finalroutes_stats_travel_modes_commutingtype u
  INNER JOIN (SELECT city, sum(freq) AS total
              FROM aux_finalroutes_stats_travel_modes_commutingtype
              GROUP BY city) y
  ON u.city = y.city
  INNER JOIN (SELECT city, transportmodes, sum(freq) AS frequi
              FROM aux_finalroutes_stats_travel_modes_commutingtype
              GROUP BY city, transportmodes) p
  ON u.city = p.city
  AND u.transportmodes = p.transportmodes
  GROUP BY u.city, u.transportmodes,frequi, total

);


CREATE TABLE finalroutes_stats_typemode AS (
  SELECT city, commutingtype, CAST(sum(unimodal) AS FLOAT)*100/count(*) AS percentUnimodal, CAST(count(*)-sum(unimodal) AS FLOAT)*100/count(*) AS percentmultimodal, CAST(sum(public) AS FLOAT)*100/count(*) AS percentPublic, CAST(sum(private) AS FLOAT)*100/count(*) AS percentPrivate
  FROM prep_all_final_routes
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, 'H_W_H' AS commutingtype, CAST(sum(freqUnimodal) AS FLOAT)*100/sum(TOTAL) AS percentUnimodal, CAST(sum(freqmultimodal) AS FLOAT)*100/sum(TOTAL) AS percentMultimodal, CAST(sum(freqPublic) AS FLOAT)*100/sum(TOTAL) AS percentPublic, CAST(sum(freqprivate) AS FLOAT)*100/sum(TOTAL) AS percentPrivate
  FROM aux_finalroutes_stats_typemode_commutingtype t
  GROUP BY city
);


CREATE TABLE finalroutes_stats_travel_modes AS (
  SELECT v.city, v.commutingtype, v.transportmodes, coalesce(percentage,0)
  FROM (SELECT * FROM finalroutes_stats_travel_modes_city) y
  RIGHT JOIN (SELECT city, commutingtype, transportmodes
              FROM (SELECT DISTINCT ON (city, commutingtype) city, commutingtype FROM finalroutes_stats_travel_modes_city) y
              CROSS JOIN (SELECT DISTINCT ON (transportmodes) transportmodes FROM aux_finalroutes_stats_travel_modes_commutingtype) h) v
  ON y.city = v.city
  AND y.commutingtype = v.commutingtype
  AND y.transportmodes = v.transportmodes
);
