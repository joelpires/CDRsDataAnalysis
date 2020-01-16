/*FIRST THINGS FIRST:
    1- Import the csv containing altice data to a table in PostgresSQL called "raw_altice_call_fct"
    2- Then just run the SQL script :)
    3- Play close attention to "WARNING" keywords throughout the commentaries
 */

----------------------------------------------
DROP TABLE IF EXISTS altice_call_dim;
CREATE TABLE altice_call_dim AS (
    SELECT DISTINCT ON (latitude, longitude) longitude, latitude FROM raw_altice_call_fct
);

ALTER TABLE altice_call_dim ADD COLUMN cell_id SERIAL PRIMARY KEY;

SELECT AddGeometryColumn('altice_call_dim', 'geom_point', 4326, 'POINT', 2);
UPDATE altice_call_dim
SET geom_point = st_setsrid(st_point(longitude, latitude), 4326);

INSERT INTO altice_call_dim (cell_id, latitude, longitude)
VALUES (-1,-1,-1);

----------------------------------------------
DROP TABLE IF EXISTS altice_call_fct;
CREATE TABLE altice_call_fct AS (
    SELECT user_id, st_time, cell_id, end_time, radius, st_angle, end_angle
    FROM raw_altice_call_fct p
    INNER JOIN altice_call_dim a
    ON p.longitude = a.longitude
    AND p.latitude = a.latitude
);
----------------------------------------------------------
-- DELETE NEGATIVE OR NULL VALUES (only 16 values removed) - WARNING: DELETES CAN TAKE MULTIPLE DAYS OF PROCESSING, SPECIALLY IF RECORDS ARE IN THE ORDER OF THOUSANDS OF MILLIONS

DELETE
FROM altice_call_fct
WHERE user_id < 1
      OR st_time < 1
      OR end_time < 1
      OR cell_id < 1
      OR radius < 1
      OR st_angle < 1
      OR end_angle < 1
      OR user_id IS NULL
      OR st_time IS NULL
      OR end_time IS NULL
      OR cell_id IS NULL
      OR radius IS NULL
      OR st_angle IS NULL
      OR end_angle IS NULL;

----------------------------------------------------------
-- CHECK IF THERE ARE DUPLICATES ON CALL_DIM
SELECT COUNT(*) FROM altice_call_dim;
SELECT COUNT (DISTINCT cell_id) FROM altice_call_dim;

----------------------------------------------------------
-- CHECK IF CELL TOWERS LOCATIONS MAKE SENSE
-- already seen on ArcGIS and yes, all the cellular towers are within the Portugal territory
-----------------------------------------------------------
----------------------------------
-- CHECK IF THERE ARE CALLS THAT HAVE terminating_cell_ids AND originating_cell_ids THAT DO NOT BELONG TO THE TOWERS WE HAVE

----------------------------------------------------------
-- GET RID OFF CALLS WITH HAVE BOTH UNKNOWN CELL TOWERS AND DUPLICATED RECORDS
-- (look that we can't rid off records with a caller OR a callee from an unknown cell)
----------------------------------------------
DROP TABLE IF EXISTS altice_unique_call_fct;        -- WARNING: -1 is intended to nullify the destination fields of the calls that were present in the CDRs from orange, without compromising the rest of the code.
CREATE TABLE altice_unique_call_fct AS(                         -- The altice dataset has also other fields like "st_angle",etc that were not integrated. For their integration, adjustments are needed.
  SELECT user_id as originating_id,
         cell_id originating_cell_id,
         -1 as terminating_id,
         -1 as terminating_cell_id,
        st_time as date_id,
        to_timestamp(st_time)::date as date,
        end_time-st_time as duration_amt
  FROM (SELECT DISTINCT * FROM altice_call_fct) c
  WHERE c.cell_id IN (SELECT cell_id FROM altice_call_dim)
);

----------------------------------------------------------
-- LET'S CHECK IF THERE ARE RECORDS THAT HAVE EVERYTHING EQUAL MINUS THE DURATION
SELECT *
FROM altice_unique_call_fct ca
INNER JOIN (SELECT originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, COUNT(0) qtd
            FROM altice_unique_call_fct
            GROUP BY originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id
            HAVING COUNT(0) > 1) ss
ON ca.originating_id = ss.originating_id
  AND ca.originating_cell_id = ss.originating_cell_id
  AND ca.terminating_id = ss.terminating_id
  AND ca.terminating_cell_id = ss.terminating_cell_id
  AND ca.date_id = ss.date_id;


-- LET'S DELETE THE RECORDS THAT HAVE EVERYTHING EQUAL MINUS THE DURATION - 104 records deleted
START TRANSACTION;
DELETE -- WARNING: DELETES CAN TAKE MULTIPLE DAYS OF PROCESSING, SPECIALLY IF RECORDS ARE IN THE ORDER OF THOUSANDS OF MILLIONS
FROM altice_unique_call_fct ca
USING (SELECT originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, COUNT(0) qtd
            FROM altice_unique_call_fct
            GROUP BY originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id
            HAVING COUNT(0) > 1) ss
WHERE ca.originating_id = ss.originating_id
  AND ca.originating_cell_id = ss.originating_cell_id
  AND ca.terminating_id = ss.terminating_id
  AND ca.terminating_cell_id = ss.terminating_cell_id
  AND ca.date_id = ss.date_id;
COMMIT;


----------------------------------------------------------
-- CHECK IF DATES AND DURATIONS ARE WITHIN A VALID INTERVAL
SELECT min(duration_amt) FROM altice_unique_call_fct;
SELECT max(duration_amt) FROM altice_unique_call_fct;
SELECT min(date_id) FROM altice_unique_call_fct;
SELECT max(date_id) FROM altice_unique_call_fct;
-- PERIOD OF THE STUDY: 2 de April of 2006 01:00:00 to 30 June of 2007 21:44:09 (424 different days of communication)

-- --------------------------------------- CHARACTERIZATION OF THE MUNICIPALS (in portugal continental) IN ORDER TO CHOOSE THE RIGHT ONES TO STUDY ------------------------------------------------------------
-- GET THE NUMBER OF TOWERS AND AVERAGE TOWER DENSITY PER REGION --
/*-- "municipalareas" contains official data of the areas of each municipal as they were considered in 2009 (the closest data we can get to 2007) (more info: https://www.pordata.pt/Municipios/Superf%C3%ADcie-57)*/
DROP TABLE IF EXISTS altice_cell_idsByRegions;
CREATE TABLE altice_cell_idsByRegions AS(  -- associate each cell_id to the respective municipal
      SELECT name_2, cell_id AS cell_id, longitude, latitude, geom_point
      FROM (SELECT * FROM altice_call_dim) c2, municipals m1
      WHERE st_intersects(c2.geom_point, m1.geom)
      GROUP BY name_2, cell_id , longitude, latitude, geom_point
);

----------------------------------------------------
-- issue: RETIFICATION OF BRAGA AND GUIMARAES
UPDATE altice_cell_idsByRegions
SET name_2 = 'Braga' WHERE cell_id IN (1471, 1571, 1671, 1771, 3471, 3871, 7371, 9171, 12171, 13571, 16171, 16471, 18771, 19571, 21471, 22671, 22871, 24871, 24971, 30571, 30771, 30871, 31171, 31571, 31921, 39871, 40971);

UPDATE altice_cell_idsByRegions
SET name_2 = 'Guimarães' WHERE cell_id IN (1871, 1971, 2071, 2171, 4771, 6571, 9071, 9271, 11471, 11671, 17171, 24671, 24771, 26071, 30171);

DELETE
FROM municipalareas
WHERE municipal = 'Braga e Guimarães';

INSERT INTO municipalareas (municipal, areakm2)
   VALUES ('Braga', 183);

INSERT INTO municipalareas (municipal, areakm2)
   VALUES ('Guimarães', 241);

DELETE
FROM municipalpops
WHERE municipal = 'Braga e Guimarães';

INSERT INTO municipalpops (municipal, population)
 VALUES ('Braga', 181494);

INSERT INTO municipalpops (municipal, population)
 VALUES ('Guimarães', 158124);

-----------------------------------------------------------------
-- issue: RETIFICATION OF ALCOUTIM AND TAVIRA
UPDATE altice_cell_idsByRegions
SET name_2 = 'Tavira' WHERE cell_id IN (4931, 5031, 5531, 8631, 9831, 15631, 16931, 17831, 19231, 23131, 23231, 24331, 25631);

UPDATE altice_cell_idsByRegions
SET name_2 = 'Alcoutim' WHERE cell_id IN (11231, 15731);


DELETE
FROM municipalareas
WHERE municipal = 'Alcoutim e Tavira';

INSERT INTO municipalareas (municipal, areakm2)
   VALUES ('Alcoutim', 575);

INSERT INTO municipalareas (municipal, areakm2)
   VALUES ('Tavira', 607);

DELETE
FROM municipalpops
WHERE municipal = 'Alcoutim e Tavira';

INSERT INTO municipalpops (municipal, population)
 VALUES ('Alcoutim', 2917);

INSERT INTO municipalpops (municipal, population)
 VALUES ('Tavira', 26167);
------------------------------------------------------------------------------------------------
-- issue: RETIFICATION FOR THE THE FOLLOWING TOWERS: 8341, 40041, 19741, 20641, 60641, 18621, 40141, 19241 (done)
INSERT INTO altice_cell_idsByRegions (name_2, cell_id, longitude, latitude,geom_point)
     VALUES ('Barreiro', 19241, -9.0815646, 38.6650406, '0101000020E6100000CC05D3D5C22922C090C1E50C20554340');
 INSERT INTO altice_cell_idsByRegions (name_2, cell_id, longitude, latitude,geom_point)
 VALUES ('Almada', 40141, -9.14488, 38.689852, '0101000020E610000060B01BB62D4A22C0448A01124D584340');

 INSERT INTO altice_cell_idsByRegions (name_2, cell_id, longitude, latitude,geom_point)
     VALUES ('Lisboa', 18621, -9.0903374, 38.7766902, '0101000020E61000001A3625B4402E22C0D50FA0956A634340');

INSERT INTO altice_cell_idsByRegions (name_2, cell_id, longitude, latitude,geom_point)
     VALUES ('Alcochete', 60641, -8.9595787, 38.7588388, '0101000020E61000005BE03CE64DEB21C0CB773AA121614340');

INSERT INTO altice_cell_idsByRegions (name_2, cell_id, longitude, latitude,geom_point)
VALUES ('Sintra', 20641, -9.4746143, 38.8195037, '0101000020E6100000896D41A500F322C0BA394B7FE5684340');

INSERT INTO altice_cell_idsByRegions (name_2, cell_id, longitude, latitude,geom_point)
     VALUES ('Seixal', 19741, -9.1022799, 38.6505152, '0101000020E61000008813F3075E3422C084C6021544534340');

 INSERT INTO altice_cell_idsByRegions (name_2, cell_id, longitude, latitude,geom_point)
 VALUES ('Alcochete', 40041, -8.9619045, 38.7572536, '0101000020E61000001EC022BF7EEC21C09E639BAFED604340');

 INSERT INTO altice_cell_idsByRegions (name_2, cell_id, longitude, latitude,geom_point)
     VALUES ('Almada', 8341, -9.2303794, 38.6746651, '0101000020E61000001E58E949F47522C054200E6D5B564340');
------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS altice_numbTowersByRegions;
CREATE TEMPORARY TABLE altice_numbTowersByRegions AS( -- calculate the number of towers of each municipal
    SELECT name_2, CAST(count(cell_id) AS FLOAT) AS numbTowers
    FROM altice_cell_idsByRegions
    GROUP BY name_2
);

----------------------------------------------
DROP TABLE IF EXISTS altice_infoMunicipals;
CREATE TABLE altice_infoMunicipals AS ( -- diverse indicators of the municipals extracted
  SELECT name_2,
         numbTowers,
         areakm2 AS areaInKm2,
         CAST (c.areakm2 AS FLOAT)/CAST (numbTowers AS FLOAT) AS averageKm2PerCell
  FROM altice_numbTowersByRegions ss
  INNER JOIN (SELECT * FROM municipalareas) c
  ON name_2 = c.municipal
);

-------------------------------------------------------------------------
-- NECESSARY ADJUSTMENTS
INSERT INTO altice_infoMunicipals (name_2, numbTowers, areaInKm2, averageKm2PerCell) -- municipals without any tower within need to have a very little value in order to compute the next divisions
VALUES ('Pedrógão Grande', 0, null, 0);
INSERT INTO altice_infoMunicipals (name_2, numbTowers, areaInKm2, averageKm2PerCell)
VALUES ('Vila do Porto', 0, null, 0);
INSERT INTO altice_infoMunicipals (name_2, numbTowers, areaInKm2, averageKm2PerCell)
VALUES ('Nordeste', 0, null, 0);
INSERT INTO altice_infoMunicipals (name_2, numbTowers, areaInKm2, averageKm2PerCell)
VALUES ('Povoação', 0, null, 0);
INSERT INTO altice_infoMunicipals (name_2, numbTowers, areaInKm2, averageKm2PerCell)
VALUES ('Santa Cruz da Graciosa', 0, null, 0);
INSERT INTO altice_infoMunicipals (name_2, numbTowers, areaInKm2, averageKm2PerCell)
VALUES ('Lajes das Flores', 0, null, 0);
INSERT INTO altice_infoMunicipals (name_2, numbTowers, areaInKm2, averageKm2PerCell)
VALUES ('Santa Cruz das Flores', 0, null, 0);
INSERT INTO altice_infoMunicipals (name_2, numbTowers, areaInKm2, averageKm2PerCell)
VALUES ('Corvo', 0, null, 0);
INSERT INTO altice_infoMunicipals (name_2, numbTowers, areaInKm2, averageKm2PerCell)
VALUES ('Porto Moniz', 0, null, 0);

----------------------------------------------------------
-- COMPLETE CHARACTERIZATION OF THE VARIOUS INDICATORS OF EACH MUNICIPAL --
/*"municipalpops" contains official data of the population of each municipal as they were considered in 2008 (the closest data we can get to 2007) (more info: https://www.pordata.pt/DB/Municipios/Ambiente+de+Consulta/Tabela)*/
DROP TABLE IF EXISTS altice_statsMunicipals;
CREATE TABLE altice_statsMunicipals AS (
  SELECT temp.name_2,
         temp1.population AS "Population",
         numbTowers AS "Nº of Towers",
         areaInKm2 AS "Area in Km2",
         count(temp.name_2) AS "Total Calls (Received and Made)",
         count(DISTINCT(userid)) AS "Active Users",
         averageKm2PerCell AS "Tower Density (Km2 per Cell)",
         CAST (count(temp.name_2) AS FLOAT)/count(DISTINCT date) AS "Average Calls Made/Received Per Day",
         CAST(count(DISTINCT date) AS FLOAT) *100/424 AS "Different Active Days / Period of the Study (%)",
         CAST (count(DISTINCT(userid)) AS FLOAT)/count(DISTINCT date) AS "Average Active Users Per Day",
         CAST(count(DISTINCT(userid)) AS FLOAT)*100/temp1.population AS "Active Users / Population (%)",
         count(DISTINCT date) AS "Different Active Days"
  FROM (
   SELECT  c.name_2, originating_id AS userid, originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, date, duration_amt, cell_id, longitude, geom_point
    FROM altice_unique_call_fct a
    INNER JOIN altice_cell_idsByRegions c
    ON a.originating_cell_id = c.cell_id

    UNION ALL

    SELECT  c.name_2, terminating_cell_id AS userid, originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, date, duration_amt, cell_id, longitude, geom_point
    FROM altice_unique_call_fct a
    INNER JOIN altice_cell_idsByRegions c
    ON a.terminating_cell_id = c.cell_id
  ) temp

  INNER JOIN municipalpops temp1
  ON temp.name_2 = temp1.municipal

  INNER JOIN altice_infoMunicipals temp2
  ON temp.name_2 = temp2.name_2

  GROUP BY temp.name_2, population, numbTowers, areaInKm2, averageKm2PerCell

  ORDER BY averageKm2PerCell ASC,                     -- order preferences
           "Average Calls Made/Received Per Day" DESC,
           "Different Active Days / Period of the Study (%)" DESC,
           "Average Active Users Per Day" DESC,
           "Active Users / Population (%)" DESC,
           "Different Active Days" DESC,
           numbTowers DESC,
           "Active Users" DESC,
           "Total Calls (Received and Made)" DESC,
           "Population" ASC,
           areaInKm2 ASC
);
