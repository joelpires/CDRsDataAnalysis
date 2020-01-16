------------------------------------- DETERMINING THE RIGHT SUBSET -------------------------------------------------
DROP TABLE IF EXISTS altice_subset_users;
CREATE TEMPORARY TABLE altice_subset_users AS (

  SELECT *
  FROM altice_users_characterization_final
  -- quality parameters
  WHERE "Call Every x Days (on Average)" <= 16.8        -- WARNING: THE QUALITY PARAMETERS CAN BE CHANGED ACCORDINGLY TO WHAT CAN BE CONCLUDED FROM altice_stats_paper1.py
    --AND "Average Calls Per Day" <= 7.5
    --AND densityhome <= 370
    -- AND "Nº Active Days" is linear so, the greater, the better

);

---------------------------- DETERMINING THE ELIGIBLE USERS FROM altice_THE SUBSET ------------------------------------

DROP TABLE IF EXISTS altice_aux_eligibleUsers;
CREATE TEMPORARY TABLE altice_aux_eligibleUsers AS (

  SELECT *
  FROM altice_subset_users
  -- certifying that home and workplace are distinct and in the same municipal
  WHERE home_id != workplace_id
    AND municipalworkplace = municipalhome
    AND (number_intermediatetowers_h_w IS NOT NULL
    OR number_intermediatetowers_w_h IS NOT NULL)
);


DROP TABLE IF EXISTS altice_eligibleUsers;
CREATE TABLE altice_eligibleUsers AS (
    SELECT user_id as id, municipalhome AS Municipal, "Tower Density (Km2 per Cell)" AS TowerDensity, m."Population" AS Population
    FROM (
         SELECT distinct id
         FROM altice_experiment4_2_3_universe
         WHERE (intermediatehome_h_w = 0 AND intermediateworkplace_h_W = 0)
            OR (intermediatehome_w_h = 0 AND intermediateworkplace_W_h = 0)
    ) p

    INNER JOIN altice_aux_eligibleUsers y
    ON id = user_id
    INNER JOIN altice_statsmunicipals m
    ON municipalhome = m.name_2
);

DROP TABLE IF EXISTS altice_eligibleUsers_full;
CREATE TABLE altice_eligibleUsers_full AS (
    SELECT user_id as id, municipalhome AS Municipal, "Tower Density (Km2 per Cell)" AS TowerDensity, m."Population" AS Population, y.number_intermediatetowers_h_w + y.number_intermediatetowers_w_h AS number_intermediatetowers
    FROM (
         SELECT distinct id
         FROM altice_experiment4_2_3_universe
         WHERE (intermediatehome_h_w = 0 AND intermediateworkplace_h_W = 0)
            AND (intermediatehome_w_h = 0 AND intermediateworkplace_W_h = 0)
    ) p

    INNER JOIN altice_aux_eligibleUsers y
    ON id = user_id
    INNER JOIN altice_statsmunicipals m
    ON municipalhome = m.name_2
);


DROP TABLE IF EXISTS altice_tempaux;
CREATE TEMPORARY TABLE altice_tempaux AS (
  SELECT municipal, count(distinct user_id) AS datasetusers
  FROM (
    SELECT municipalhome AS municipal, user_id
    FROM altice_subset_users t
    WHERE municipalhome IS NOT NULL AND municipalworkplace = municipalhome

    UNION

    SELECT municipalworkplace AS municipal, user_id
    FROM altice_subset_users t
    WHERE municipalworkplace IS NOT NULL AND municipalworkplace = municipalhome

  ) p
  GROUP BY municipal
);

DROP TABLE IF EXISTS altice_eligibleUsers_byMunicipal;
CREATE TABLE altice_eligibleUsers_byMunicipal AS (
  SELECT t.municipal, Population, datasetusers,count(distinct ii.id) AS userscommuting, 10/(0.0000001+CAST(TowerDensity AS FLOAT)) AS "Towers per 10 Km2"
  FROM altice_eligibleUsers ii
  INNER JOIN altice_tempaux t
      ON ii.Municipal = t.municipal
  GROUP BY t.municipal, TowerDensity, Population, datasetusers
  ORDER BY  userscommuting DESC
);

DROP TABLE IF EXISTS altice_eligibleUsers_full_byMunicipal;
CREATE TABLE altice_eligibleUsers_full_byMunicipal AS (
  SELECT t.municipal, Population, datasetusers,count(distinct ii.id) AS userscommuting, 10/CAST(TowerDensity AS FLOAT) AS "Towers per 10 Km2"
  FROM altice_eligibleUsers_full ii
  INNER JOIN altice_tempaux t
      ON ii.Municipal = t.municipal
  GROUP BY t.municipal, TowerDensity, Population, datasetusers
  ORDER BY  userscommuting DESC
);


DROP TABLE IF EXISTS altice_final_eligibleUsers;
CREATE TABLE altice_final_eligibleUsers AS (
  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Lisboa' ORDER BY number_intermediatetowers DESC LIMIT 500) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Porto' ORDER BY number_intermediatetowers DESC LIMIT 500) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Coimbra' ORDER BY number_intermediatetowers DESC LIMIT 500) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Braga' ORDER BY number_intermediatetowers DESC LIMIT 500) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Aveiro' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Setúbal' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Bragança' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Vila Real' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Leiria' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Viana do Castelo' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Viseu' ORDER BY number_intermediatetowers DESC LIMIT 300) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Castelo Branco' ORDER BY number_intermediatetowers DESC LIMIT 150) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Santarém' ORDER BY number_intermediatetowers DESC LIMIT 150) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Faro' ORDER BY number_intermediatetowers DESC LIMIT 150) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Évora' ORDER BY number_intermediatetowers DESC LIMIT 150) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Beja' ORDER BY number_intermediatetowers DESC LIMIT 100) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Guarda' ORDER BY number_intermediatetowers DESC LIMIT 100) u

  UNION ALL

  SELECT * FROM (SELECT * FROM altice_eligibleUsers_full  WHERE municipal = 'Portalegre' ORDER BY number_intermediatetowers DESC LIMIT 97) u

);

DROP TABLE IF EXISTS altice_final_eligibleUsers_byMunicipal;
CREATE TABLE altice_final_eligibleUsers_byMunicipal AS (
  SELECT t.municipal, Population, datasetusers,count(distinct ii.id) AS userscommuting, 10/CAST(TowerDensity AS FLOAT) AS "Towers per 10 Km2"
  FROM altice_final_eligibleUsers ii
  INNER JOIN altice_tempaux t
      ON ii.Municipal = t.municipal
  GROUP BY t.municipal, TowerDensity, Population, datasetusers
  ORDER BY  userscommuting DESC, t.municipal DESC
);


---------------------------------------------------------------------------
DROP TABLE IF EXISTS altice_temp;
CREATE TEMPORARY TABLE altice_temp AS (
  SELECT *
  FROM altice_users_characterization_final
  WHERE user_id IN (SELECT id FROM altice_eligibleUsers)
);

---------------------------------------------------------------------------
DROP TABLE IF EXISTS altice_frequencies_intermediateTowers_H_W;
CREATE TABLE altice_frequencies_intermediateTowers_H_W AS (
  SELECT intermediatetowers_h_wid, tower, latitude, longitude, count(*) AS frequencia
  FROM altice_intermediateTowers_H_W_u

  INNER JOIN altice_temp
  ON intermediatetowers_h_wid = user_id

  GROUP BY intermediatetowers_h_wid, tower, latitude, longitude
);

---------------------------------------------------------------------------
DROP TABLE IF EXISTS altice_frequencies_intermediateTowers_W_H;
CREATE TABLE altice_frequencies_intermediateTowers_W_H AS (
  SELECT intermediatetowers_w_hid, tower, latitude, longitude, count(*) AS frequencia
  FROM altice_intermediateTowers_W_H_u

  INNER JOIN altice_temp
  ON intermediatetowers_w_hid = user_id

  GROUP BY intermediatetowers_w_hid, tower, latitude, longitude
);

ALTER TABLE altice_frequencies_intermediateTowers_H_W ADD COLUMN geom_point_dest GEOMETRY(Point, 4326);
UPDATE altice_frequencies_intermediateTowers_H_W SET geom_point_dest=st_SetSrid(st_MakePoint(longitude, latitude), 4326);

ALTER TABLE altice_frequencies_intermediateTowers_W_H ADD COLUMN geom_point_dest GEOMETRY(Point, 4326);
UPDATE altice_frequencies_intermediateTowers_W_H SET geom_point_dest=st_SetSrid(st_MakePoint(longitude, latitude), 4326);
