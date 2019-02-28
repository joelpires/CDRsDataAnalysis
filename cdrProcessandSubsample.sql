-- ISSUES --
-- issue: decidir o que fazer com os dados de oscillation case4
-- issue: create oscillations table from case4 table
-- issue: verificar se há duplicados nos ranked
-- issue: manter tracking dos registos cuja torre de origem ou destino nao estao na base de dados de torres
-- issue: if there's more than one most visited cell, analyze...(is it an oscillation?)
          -- see if this issue happens frequently
-- issue: ver o que faço com os comentários do delete e do insert into
-- issue: eliminar "just a fun experiment"

-- ------------------------------- PROCESS ALL THE DATA ----------------------------- --
-- DELETE NEGATIVE OR NULL VALUES (only 16 values removed)
DELETE
FROM call_fct
WHERE duration_amt < 1
      OR originating_id < 1
      OR terminating_id < 1
      OR originating_cell_id < 1
      OR terminating_cell_id < 1
      OR date_id < 1
      OR duration_amt IS NULL
      OR originating_id IS NULL
      OR terminating_id IS NULL
      OR originating_cell_id IS NULL
      OR terminating_cell_id IS NULL
      OR date_id IS NULL;

-- CHECK IF THERE ARE DUPLICATES ON CALL_DIM
SELECT COUNT(*) FROM call_dim; -- 6511 towers
SELECT COUNT (DISTINCT cell_id) FROM call_dim; -- 6511 towers

-- CHECK IF CELL TOWERS LOCATIONS MAKE SENSE
-- already seen on ArcGIS and yes, all the cellular towers are within the Portugal territory

-- CHECK IF THERE ARE DUPLICATES ON CDR'S
SELECT COUNT(*) FROM call_fct; -- 435701811 records
SELECT DISTINCT COUNT(*) FROM call_fct; -- 435699639 (which means that there are 2172 duplicated records)

-- CHECK IF THERE ARE CALLS THAT HAVE terminating_cell_ids and originating_cell_ids THAT DO NOT BELONG TO THE TOWERS WE HAVE
SELECT * -- the result was 422891766 records (which means that there are, in fact, 12810045 records to be eliminated)
FROM call_fct
WHERE originating_cell_id IN (SELECT cell_id FROM call_dim)
      AND terminating_cell_id IN (SELECT cell_id FROM call_dim);

-- GET RID OFF CALLS WITH UNKNOWN CELL TOWERS AND DUPLICATED RECORDS
CREATE TABLE unique_call_fct AS(
  SELECT originating_id,
         originating_cell_id,
         terminating_id,
         terminating_cell_id,
         date_id,
         to_timestamp(floor(((732677 - 719528)* 86400) + (((date_id/100000.0)-1)*24*60*60)))::date as date,
         duration_amt
  FROM (SELECT DISTINCT * FROM call_fct) c
  WHERE c.originating_cell_id IN (SELECT cell_id FROM call_dim)
        AND c.terminating_cell_id IN (SELECT cell_id FROM call_dim)
);

SELECT COUNT(*) FROM unique_call_fct; -- 422891734 remaining records (12810077 records eliminated)

-- ------------------------------- REMOVING OSCILLATION SEQUENCES ----------------------------- -- 16 +  records deleted from call_fct_porto
/*These series of calculations were done in the subset of the CDR's of the specific region due to the high demand for computational power.
  Check the continuity of the same call through different records:
    -- (1) if the difference between the date_id's of two registers between the exact same users is equal to 0 and there is a difference of towers --> delete all of them!
    -- if the difference between the date_id's of two registers between the exact same users is equal to the duration of the first record, then we have a call continuity. It can be:
        -- (2) merge the cdr's: once the originating_cell_ids or the terminating_cell_ids are the same
        -- (3) user is moving and changed legitimately his/her cell tower: once at least one of the originating_cell_ids or the terminating_cell_ids changed and is not an oscillation
        -- (4) oscillation: once it is noticed the changed previously described was done at a ridiculous speed (400 km/h). In this case we assume that the first record is the true one and fuse.
                        -- there are cases where users switch cells more than once??? If so, another action needs to be done*/

-- CREATE SEQUENCE serial START 1
DROP TABLE differences;
CREATE TEMPORARY TABLE differences AS( -- creating a temporary table that calculates difference between date_ids of the calls between the same users and potentially identify call continuity
  SELECT *,
            CASE
                  WHEN (g.originating_id = lagOriginating_id AND g.terminating_id = lagterminating_id AND diffDates = lagduration_amt) THEN currval('serial')
                  ELSE nextval('serial')
              END AS mySequence
  FROM (
    SELECT
      originating_id,
      originating_cell_id,
      terminating_id,
      terminating_cell_id,
      date_id,
      duration_amt,
      date_id - lag(date_id) OVER (PARTITION BY originating_id, terminating_id ORDER BY date_id) AS diffDates,
      lag(originating_id) OVER (PARTITION BY originating_id, terminating_id ORDER BY date_id) AS lagOriginating_id,
      lag(originating_cell_id) OVER (PARTITION BY originating_id, terminating_id ORDER BY date_id) AS lagoriginating_cell_id,
      lag(terminating_id) OVER (PARTITION BY originating_id, terminating_id ORDER BY date_id) AS lagterminating_id,
      lag(terminating_cell_id) OVER (PARTITION BY originating_id, terminating_id ORDER BY date_id) AS lagterminating_cell_id,
      lag(date_id) OVER (PARTITION BY originating_id, terminating_id ORDER BY date_id) AS lagdate_id,
      lag(duration_amt) OVER (PARTITION BY originating_id, terminating_id ORDER BY date_id) AS lagduration_amt
    FROM (
      SELECT *
      FROM unique_call_fct
    ) f
    ORDER BY originating_id, terminating_id, date_id
  )g

);

-- case (1)
CREATE TEMPORARY TABLE case1 AS (
  SELECT *
  FROM differences
  WHERE diffDates = 0
);
SELECT * FROM case1; -- x records deleted

START TRANSACTION;
DELETE
FROM unique_call_fct u
USING case1 d
WHERE (u.originating_id = d.originating_id
  AND u.originating_cell_id = d.originating_cell_id
  AND u.terminating_id = d.terminating_id
  AND u.terminating_cell_id = d.terminating_cell_id
  AND u.date_id = d.date_id
  AND u.duration_amt = d.duration_amt)
OR (u.originating_id = d.lagoriginating_id
  AND u.originating_cell_id = d.lagoriginating_cell_id
  AND u.terminating_id = d.lagterminating_id
  AND u.terminating_cell_id = d.lagterminating_cell_id
  AND u.date_id = d.lagdate_id
  AND u.duration_amt = d.lagduration_amt);
COMMIT;

-- case (2)
CREATE TEMPORARY TABLE case2 AS (
  SELECT *
  FROM differences
  WHERE diffDates = lagduration_amt
        AND originating_cell_id = lagoriginating_cell_id
        AND terminating_cell_id = lagterminating_cell_id
);
SELECT count(*) FROM case2; -- 284*2 = 568 records that will be merged

CREATE TEMPORARY TABLE mergecase2 AS (
  SELECT lagoriginating_id AS originating_id,
         lagoriginating_cell_id AS originating_cell_id,
         lagterminating_id AS terminating_id,
         lagterminating_cell_id AS terminating_cell_id,
         lagdate_id AS date_id,
         soma + lagduration_amt AS soma
  FROM (
    SELECT DISTINCT ON (mySequence) *
    FROM (SELECT * FROM differences ORDER BY date_id) a
    WHERE diffDates = lagduration_amt
          AND originating_cell_id = lagoriginating_cell_id
          AND terminating_cell_id = lagterminating_cell_id
  ) a
  INNER JOIN (SELECT mySequence AS seq,  sum(duration_amt) AS soma
              FROM differences
              GROUP BY mySequence) r
  ON a.mySequence = seq
);

SELECT count(*) FROM mergecase2;  -- (284-247) records were merged = 37.

START TRANSACTION; -- 568-37 = 531 records will be deleted
DELETE
FROM unique_call_fct u
USING case2 d
WHERE (u.originating_id = d.originating_id
  AND u.originating_cell_id = d.originating_cell_id
  AND u.terminating_id = d.terminating_id
  AND u.terminating_cell_id = d.terminating_cell_id
  AND u.date_id = d.date_id
  AND u.duration_amt = d.duration_amt)
OR (u.originating_id = d.lagoriginating_id
  AND u.originating_cell_id = d.lagoriginating_cell_id
  AND u.terminating_id = d.lagterminating_id
  AND u.terminating_cell_id = d.lagterminating_cell_id
  AND u.date_id = d.lagdate_id
  AND u.duration_amt = d.lagduration_amt);
COMMIT;

START TRANSACTION; -- insert 247 records. -- The total dataset will have -531 + 247 records = 284 records
INSERT INTO unique_call_fct (originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, duration_amt)
(SELECT originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, soma
  FROM mergecase2);
COMMIT;

-- case (3) and (4)
CREATE TEMPORARY TABLE case3and4 AS (
  SELECT *, geom1 AS ori_geom_point, geom2 AS term_geom_point, geom3 AS lagori_geom_point, geom4 AS lagterm_geom_point
  FROM(
    SELECT *
    FROM differences
    WHERE diffDates = lagduration_amt
          AND (originating_cell_id != lagoriginating_cell_id OR terminating_cell_id != lagterminating_cell_id)
  ) b

  INNER JOIN (SELECT cell_id AS cid, geom_point AS geom1 FROM call_dim) u
  ON originating_cell_id = cid

  INNER JOIN (SELECT cell_id AS cide, geom_point AS geom2 FROM call_dim) v
  ON terminating_cell_id = cide

  INNER JOIN (SELECT cell_id AS cida, geom_point AS geom3 FROM call_dim) s
  ON lagoriginating_cell_id = cida

  INNER JOIN (SELECT cell_id AS cidu, geom_point AS geom4 FROM call_dim) d
  ON lagterminating_cell_id = cidu
);

-- case (4)
DROP table switchspeedscase4;
CREATE TEMPORARY TABLE switchspeedscase4 AS (
  SELECT *,
             (CAST(distanciaOrig AS FLOAT)/1000)/(CAST(lagduration_amt AS FLOAT)/3600) AS "Switch Speed Origin - Km per hour",
             (CAST(distanciaTerm AS FLOAT)/1000)/(CAST(lagduration_amt AS FLOAT)/3600) AS "Switch Speed Terminating - Km per hour"
  FROM (SELECT *,
           st_distance(ST_Transform(ori_geom_point, 3857), ST_Transform(lagori_geom_point, 3857)) AS distanciaOrig,
           st_distance(ST_Transform(term_geom_point, 3857), ST_Transform(lagterm_geom_point, 3857)) AS distanciaTerm
        FROM case3and4) r
);

SELECT COUNT(*) FROM unique_call_fct; -- 422891734 remaining records (12810077 records eliminated)

-- LET'S CHECK IF THERE ARE RECORDS THAT HAVE EVERYTHING EQUAL MINUS THE DURATION
SELECT *
FROM unique_call_fct ca
INNER JOIN (SELECT originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, COUNT(0) qtd
            FROM unique_call_fct
            GROUP BY originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id
            HAVING COUNT(0) > 1) ss
ON ca.originating_id = ss.originating_id
  AND ca.originating_cell_id = ss.originating_cell_id
  AND ca.terminating_id = ss.terminating_id
  AND ca.terminating_cell_id = ss.terminating_cell_id
  AND ca.date_id = ss.date_id;

-- LET'S DELETE THE RECORDS THAT HAVE EVERYTHING EQUAL MINUS THE DURATION - 104 records deleted
DELETE
FROM unique_call_fct ca
USING (SELECT originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, COUNT(0) qtd
            FROM unique_call_fct
            GROUP BY originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id
            HAVING COUNT(0) > 1) ss
WHERE ca.originating_id = ss.originating_id
  AND ca.originating_cell_id = ss.originating_cell_id
  AND ca.terminating_id = ss.terminating_id
  AND ca.terminating_cell_id = ss.terminating_cell_id
  AND ca.date_id = ss.date_id;

-- CHECK IF DATES AND DURATIONS ARE WITHIN A VALID INTERVAL
SELECT min(duration_amt) FROM unique_call_fct; -- is 1 seconds (is valid)
SELECT max(duration_amt) FROM unique_call_fct; -- is 14865 seconds (is valid. Corresponds to 4,12 hours and is valid. Worry would be if, for example, a call took more than 12 hours)
SELECT min(date_id) FROM unique_call_fct; -- is 9200000 - corresponds to Sunday, 2 de April of 2006 01:00:00 (is valid)
SELECT max(date_id) FROM unique_call_fct; -- is 54686399 - corresponds to Saturday, 30 June of 2007 21:44:09 (is valid)
-- PERIOD OF THE STUDY: 2 de April of 2006 01:00:00 to 30 June of 2007 21:44:09 (424 different days of communication)

-- OBTAIN THE TOTAL CALLS MADE BY EACH USER --
CREATE TEMPORARY TABLE totalCallsByUser AS(
  SELECT id, count(id) AS totalCalls
  FROM (
    SELECT originating_id AS id
    FROM unique_call_fct

    UNION ALL

    SELECT terminating_id AS id
    FROM unique_call_fct
  ) a
  GROUP BY id
);

SELECT count(DISTINCT uid)  -- calculate the total number of users that we ended up with after the processing: 1899216 users
FROM(
    SELECT originating_id AS uid
    FROM unique_call_fct

    UNION ALL
      SELECT terminating_id AS uid
      FROM unique_call_fct
) t;
 SELECT count(*) FROM unique_call_fct; -- calculate the total number of records that we ended up with after the processing: 422891630 records. Which means that a total of 12808009 records were eliminated after the processing.

-- ------------------------------- CHARACTERIZATION OF THE MUNICIPALS IN ORDER TO CHOOSE THE RIGHT ONES TO STUDY -----------------------------

-- GET THE NUMBER OF TOWERS AND AVERAGE TOWER DENSITY PER REGION --
/*-- "municipalareas" contains official data of the areas of each municipal as they were considered in 2009 (the closest data we can get to 2007) (more info: https://www.pordata.pt/Municipios/Superf%C3%ADcie-57)*/
CREATE TEMPORARY TABLE cell_idsByRegions AS(  -- associate each cell_id to the respective municipal
      SELECT name_2, cell_id, longitude, latitude, geom_point
      FROM call_dim c2, municipals m1
      WHERE st_intersects(c2.geom_point, m1.geom)
      GROUP BY name_2, cell_id, longitude, latitude, geom_point
);

CREATE TEMPORARY TABLE numbTowersByRegions AS( -- calculate the number of towers of each municipal
    SELECT name_2, CAST(count(cell_id) AS FLOAT) AS numbTowers
    FROM cell_idsByRegions
    GROUP BY name_2
);

INSERT INTO numbTowersByRegions (name_2, numbTowers) -- municipals without any tower within need to have an very little value in order to compute the next divisions
VALUES ('Pedrógão Grande', 0.000001);
INSERT INTO numbTowersByRegions (name_2, numbTowers)
VALUES ('Vila do Porto', 0.000001);
INSERT INTO numbTowersByRegions (name_2, numbTowers)
VALUES ('Nordeste', 0.000001);
INSERT INTO numbTowersByRegions (name_2, numbTowers)
VALUES ('Povoação', 0.000001);
INSERT INTO numbTowersByRegions (name_2, numbTowers)
VALUES ('Santa Cruz da Graciosa', 0.000001);
INSERT INTO numbTowersByRegions (name_2, numbTowers)
VALUES ('Lajes das Flores', 0.000001);
INSERT INTO numbTowersByRegions (name_2, numbTowers)
VALUES ('Santa Cruz das Flores', 0.000001);
INSERT INTO numbTowersByRegions (name_2, numbTowers)
VALUES ('Corvo', 0.000001);
INSERT INTO numbTowersByRegions (name_2, numbTowers)
VALUES ('Porto Moniz', 0.000001);

CREATE TEMPORARY TABLE infoMunicipals AS ( -- diverse indicators of the municipals extracted
  SELECT name_2,
         numbTowers,
         areakm2 AS areaInKm2,
         CAST (c.areakm2 AS FLOAT)/CAST (numbTowers AS FLOAT) AS averageKm2PerCell
  FROM numbTowersByRegions ss
  INNER JOIN (SELECT * FROM municipalareas) c
  ON name_2 = c.municipal
);

-- COMPLETE CHARACTERIZATION OF THE VARIOUS INDICATORS OF EACH MUNICIPAL --
/*"municipalpops" contains official data of the population of each municipal as they were considered in 2008 (the closest data we can get to 2007) (more info: https://www.pordata.pt/DB/Municipios/Ambiente+de+Consulta/Tabela)*/

CREATE TABLE statsMunicipals AS (
  SELECT temp.name_2,
         temp1.population AS "Population",
         numbTowers AS "Nº of Towers",
         areaInKm2 AS "Area in Km2",
         count(temp.name_2) AS "Total Calls (Received and Made)",
         count(DISTINCT(userid)) AS "Active Users",
         averageKm2PerCell "Tower Density (Km2 per Cell)",
         CAST (count(temp.name_2) AS FLOAT)/count(DISTINCT date) AS "Average Calls Made/Received Per Day",
         CAST(count(DISTINCT date) AS FLOAT) *100/424 AS "Different Active Days / Period of the Study (%)",
         CAST (count(DISTINCT(userid)) AS FLOAT)/count(DISTINCT date) AS "Average Active Users Per Day",
         CAST(count(DISTINCT(userid)) AS FLOAT)*100/temp1.population AS "Active Users / Population (%)",
         count(DISTINCT date) AS "Different Active Days"
  FROM (
   SELECT  c.name_2, originating_id AS userid, originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, date, duration_amt, cell_id, longitude, geom_point
    FROM unique_call_fct a
    INNER JOIN cell_idsByRegions c
    ON a.originating_cell_id = c.cell_id

    UNION ALL

    SELECT  c.name_2, terminating_cell_id AS userid, originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, date, duration_amt, cell_id, longitude, geom_point
    FROM unique_call_fct a
    INNER JOIN cell_idsByRegions c
    ON a.terminating_cell_id = c.cell_id
  ) temp

  INNER JOIN municipalpops temp1
  ON temp.name_2 = temp1.municipal

  INNER JOIN infoMunicipals temp2
  ON temp.name_2 = temp2.name_2

  GROUP BY temp.name_2, population, numbTowers, areaInKm2, averageKm2PerCell

  ORDER BY averageKm2PerCell ASC,                     -- order preferences
           "Average Calls Made/Received Per Day" DESC,
           "Different Active Days / Period of the Study (%)" DESC,
           "Average Active Users Per Day" DESC, "Active Users / Population (%)" DESC,
           "Different Active Days" DESC, numbTowers DESC,
           "Active Users" DESC,
           "Total Calls (Received and Made)" DESC,
           "Population" ASC,
           areaInKm2 ASC
);

-- ------------------------------- SUBSAMPLING THE WHOLE DATASET TO THE SUBSET OF THE REGION OF PORTO -----------------------------

--  OBTAIN THE CELL TOWERS OF THE SPECIFIC REGION OF PORTO --
CREATE TABLE call_dim_porto AS (
  SELECT *
  FROM cell_idsByRegions
  WHERE name_2 = 'Porto'
);

--  OBTAIN THE CALLS MADE/RECEIVED FROM TOWERS OF PORTO  --
CREATE TABLE call_fct_porto AS (
  SELECT originating_id,
       originating_cell_id,
       terminating_id,
       terminating_cell_id,
       date_id,
       date,
       to_timestamp(floor(((732677 - 719528)* 86400) + (((date_id/100000.0)-1)*24*60*60)))::time AS time,
       duration_amt
  FROM unique_call_fct
  INNER JOIN call_dim_porto
  ON unique_call_fct.originating_cell_id = call_dim_porto.cell_id

  UNION

  SELECT originating_id,
       originating_cell_id,
       terminating_id,
       terminating_cell_id,
       date_id,
       date,
       to_timestamp(floor(((732677 - 719528)* 86400) + (((date_id/100000.0)-1)*24*60*60)))::time AS time,
       duration_amt
  FROM unique_call_fct
  INNER JOIN call_dim_porto
  ON unique_call_fct.terminating_cell_id = call_dim_porto.cell_id
);

-- ------------------------------- PROCESS THE DATA FROM THE SPECIFIC REGION ----------------------------- --

-- AMOUNT OF TALK BY USER --
CREATE TEMPORARY TABLE durationsByUser AS(
  SELECT uid, sum(duration) as amountOfTalk
  FROM(
      SELECT originating_id AS uid, duration_amt AS duration
      FROM call_fct_porto
      GROUP BY originating_id, duration_amt

      UNION ALL
        SELECT terminating_id AS uid, duration_amt AS duration
        FROM call_fct_porto
        GROUP BY terminating_id, duration_amt
  ) t
  GROUP BY uid
);

-- MOST VISITED CELLS --
-- WORK --
CREATE TEMPORARY TABLE visitedCellsByIds_W AS(  -- table with the visited cells by each user during the working hours
  SELECT id, cell_id, sum(qtd) AS qtd
  FROM(
      SELECT originating_id AS id, originating_cell_id AS cell_id, count(*) AS qtd
      FROM call_fct_porto
      WHERE time > '22:00:00'::time OR time < '07:00:00'::time
      GROUP BY originating_id, originating_cell_id

      UNION ALL
        SELECT terminating_id AS id, terminating_cell_id AS cell_id, count(*) AS qtd
        FROM call_fct_porto
        WHERE time > '22:00:00'::time OR time < '07:00:00'::time
        GROUP BY terminating_id, terminating_cell_id
  ) t
  GROUP BY id, cell_id
);

/*
Let's create a table that tells us which users have a well defined cellular tower for their workplace
  . 0, OR the user does not have registered calls during the hours that is supposed to be at work OR it was not possible to identify only one cellular tower with the most activity
  . 1, otherwise
*/

CREATE TABLE mostVisitedCells_W AS (
  SELECT id, cell_id AS mostVisitedCell, qtd
  FROM visitedCellsByIds_W
  WHERE (id, qtd) IN (
      SELECT id, max(qtd) AS max
      FROM visitedCellsByIds_W
      GROUP BY id
  )
  GROUP BY id, cell_id, qtd
);

CREATE TEMPORARY TABLE hasMostVisitedCell_W AS(
  SELECT uid AS id
  FROM durationsByUser
);

ALTER TABLE hasMostVisitedCell_W
ADD "has?" INTEGER DEFAULT 0; -- by default none of the users are eligible

UPDATE hasMostVisitedCell_W -- enabling the users that had registered call activity during the hours that is supposed to be working
SET "has?" = 1
WHERE id IN ( SELECT DISTINCT id FROM mostVisitedCells_W);

UPDATE hasMostVisitedCell_W -- disabling the users that registered more than one cell with max activity
SET "has?" = 0
WHERE id IN (
  SELECT id
  FROM mostVisitedCells_W ca
  INNER JOIN (SELECT id AS userid
              FROM mostVisitedCells_W
              GROUP BY id
              HAVING COUNT(0) > 1) ss
  ON ca.qtd = qtd
  AND ca.id = userid
);


-- HOME --
CREATE TEMPORARY TABLE visitedCellsByIds_H AS(  -- table with the visited cells by each user during the non-working hours
  SELECT id, cell_id, sum(qtd) AS qtd
  FROM(
      SELECT originating_id AS id, originating_cell_id AS cell_id, count(*) AS qtd
      FROM call_fct_porto
      WHERE time > '9:00:00'::time AND time < '17:00:00'::time
      GROUP BY originating_id, originating_cell_id

      UNION ALL
        SELECT terminating_id AS id, terminating_cell_id AS cell_id, count(*) AS qtd
        FROM call_fct_porto
        WHERE time > '9:00:00'::time AND time < '17:00:00'::time
        GROUP BY terminating_id, terminating_cell_id
  ) t
  GROUP BY id, cell_id
);


/*
Lets create a table that tells us which users have a well defined cellular tower for their home
  . 0, OR the user does not have registered calls during the hours that is supposed to be at home OR it was not possible to identify only one cellular tower with the most activity
  . 1, otherwise
*/
CREATE TABLE mostVisitedCells_H AS (
        SELECT id, cell_id as mostVisitedCell, qtd
        FROM visitedCellsByIds_H
        WHERE (id, qtd) IN (
            SELECT id, max(qtd) as max
            FROM visitedCellsByIds_H
            GROUP BY id
        )
        GROUP BY id, cell_id, qtd
        ORDER BY id, cell_id, qtd
);

CREATE TEMPORARY TABLE hasMostVisitedCell_H AS(
  SELECT uid AS id
  FROM durationsByUser
);

ALTER TABLE hasMostVisitedCell_H
ADD "has?" INTEGER DEFAULT 0; -- by default none of the users are eligible

UPDATE hasMostVisitedCell_H -- enabling the users that had registered call activity during the hours that is supposed to be at home
SET "has?" = 1
WHERE id IN (
    SELECT DISTINCT id FROM mostVisitedCells_H
);

UPDATE hasMostVisitedCell_H -- disabling the users that registered more than one cell with max activity
SET "has?" = 0
WHERE id IN (
  SELECT id
  FROM mostVisitedCells_H ca
  INNER JOIN (SELECT id AS userid
              FROM mostVisitedCells_H
              GROUP BY id
              HAVING COUNT(0) > 1) ss
  ON ca.qtd = qtd
  AND ca.id = userid
);

CREATE TEMPORARY TABLE visitedCellsByIds_G AS( -- DIFFERENT VISITED CELLS IN GENERAL, GROUPED BY USER ID --
  SELECT id, cell_id, sum(qtd) AS qtd
  FROM(
    SELECT originating_id AS id, originating_cell_id AS cell_id, count(*) AS qtd
    FROM call_fct_porto
    GROUP BY originating_id, originating_cell_id

    UNION ALL
      SELECT terminating_id AS id, terminating_cell_id AS cell_id, count(*) AS qtd
      FROM call_fct_porto
      GROUP BY terminating_id, terminating_cell_id
  ) t
  GROUP BY id, cell_id
);

-- ------------------------------- CHARACTERIZE USERS BY MULTIPLE PARAMETERS ----------------------------- --
CREATE TABLE porto_users_characterization AS (
  SELECT ss2.id,
         amountOfTalk AS "Total Amount of Talk",
         (numberCalls/ activeDays) AS "Average Calls Per Day",
         sumDifferencesDays/activeDays AS "Average of Days Until Call",
         CAST(numberCalls AS FLOAT) * 100/totalCalls AS "Calls in Porto (%)",
         CAST(activeDays* 100/ 424 AS FLOAT) AS "Active Days / Period of the Study (%)",
         numberCalls AS "Nº Calls (Made/Received)",
         activeDays AS "Nº Active Days",
         CAST(differentvisitedplaces AS FLOAT)/ activeDays AS "Avg Different Places Visited Per Day",
         differentvisitedplaces as "Different Places Visited",
         CAST(amountOfTalk AS FLOAT)/ activeDays AS "Average Talk Per Day",
         CAST(amountOfTalk AS FLOAT)/ numberCalls AS "Average Amount of Talk Per Call",
         e."has?" AS "Has Most Visited Cell at Work",
         d."has?" AS "Has Most Visited Cell at Home"
  FROM (
      SELECT id,
             count(date) AS activeDays,
             sum(qtd) AS numberCalls,
             sum(diffDays) AS sumDifferencesDays
      FROM (
          SELECT id,
                 date,
                 sum(qtd) qtd,
                 COALESCE(ROUND(ABS((date_part('day',age(date, lag(date) OVER (PARTITION BY id order by id)))/365 + date_part('month',age(date, lag(date) OVER (PARTITION BY id order by id)))/12 + date_part('year',age(date, lag(date) OVER (PARTITION BY id order by id))))*365 )), 0) as diffDays
          FROM (
              SELECT originating_id AS id, date, count(*) AS qtd
              FROM call_fct_porto
              GROUP BY originating_id, date

              UNION ALL
                SELECT terminating_id AS id, date, count(*) AS qtd
                FROM call_fct_porto
                GROUP BY terminating_id, date
               )ss
          GROUP BY id, date
      ) ss1
      GROUP BY id
  ) ss2

  INNER JOIN (
    SELECT id as userid, count(cell_id) as differentVisitedPlaces
    FROM visitedCellsByIds_G
    GROUP BY id
  ) b
  ON ss2.id = b.userid

  INNER JOIN (SELECT * FROM durationsByUser) c
  ON ss2.id = c.uid

  INNER JOIN (SELECT * FROM hasMostVisitedCell_H) d
  ON ss2.id = d.id

  INNER JOIN (SELECT * FROM hasMostVisitedCell_W) e
  ON ss2.id = e.id

  INNER JOIN (SELECT * FROM totalCallsByUser) f
  ON ss2.id = f.id

  GROUP BY ss2.id, "Calls in Porto (%)", "Has Most Visited Cell at Work", "Has Most Visited Cell at Home", differentvisitedplaces, amountOfTalk, activeDays, numberCalls, sumDifferencesDays
);

SELECT count(id) FROM porto_users_characterization; -- total of 763156 users of Porto that made/received calls in Porto, almost half of our total number of users
SELECT count(*) FROM call_fct_porto; -- in a total amount of 40122144 records

-- ------------------------------- SUBSAMPLING THE DATA BASED ON A SET OF PREFERENCES ----------------------------- --
/*
ESTABLISHING THE PARAMETERS AND PRIORITIZE THE INDICATORS FOR THE USERS' PROFILES THAT WE WANT. IGNORE USERS THAT:
        -- TALK A LOT DURING THE DAY (can be a bot, shared phone, call center, or people that to not have a job so that we can't infer his/her workplace)
        -- DO NOT HAVE A WELL IDENTIFIED HOME OR WORKPLACE
        -- HAVE A PERCENTAGE OF CALLS INSIDE PORTO MORE THAN 80 %. THAT'S BECAUSE WE NEED TO HAVE INTO ACCOUNT THAT:
            . There are people that can work in Porto but live outside of it (and vice-versa)
            . During the period of study, some of the people could change the home and/or the workplace locations
            . People can take vacations and travel abroad
*/

CREATE TEMPORARY TABLE call_fct_porto_users AS (
  SELECT *
  FROM porto_users_characterization
  WHERE "Calls in Porto (%)" >= 80
        AND "Has Most Visited Cell at Home" = 1
        AND "Has Most Visited Cell at Work" = 1
        AND "Average Talk Per Day" < 18000 -- less than 5 hours of talk per day

  ORDER BY "Average Calls Per Day" DESC,  -- order the set of preferences
           "Active Days / Period of the Study (%)" DESC,
           "Average of Days Until Call" DESC,
           "Calls in Porto (%)" DESC,
           "Nº Calls (Made/Received)" DESC,
           "Nº Active Days" DESC,
           "Avg Different Places Visited Per Day" ASC,
           "Different Places Visited" DESC  -- "Total Amount of Talk", "Average Talk Per Day" and "Average Amount of Talk Per Call" doesn't matter
  LIMIT 500 -- number of users depends on the dimension of our study
);

-- Establishing the parameters that we want
CREATE TABLE sub_call_fct_porto AS(
  SELECT *
  FROM call_fct_porto
  WHERE originating_id IN (SELECT id FROM call_fct_porto_users)
        OR terminating_id IN (SELECT id FROM call_fct_porto_users)
);

SELECT count(*) FROM sub_call_fct_porto; -- we ended up with 300 users and x records


-- ------------------------------- EXTRA MEASURES ----------------------------- --
-- MOST VISITED CELLS IN GENERAL, GROUPED BY USER ID --
CREATE TEMPORARY TABLE mostVisitedCells_G AS (
  SELECT id, cell_id AS mostVisitedCell, qtd
  FROM visitedCellsByIds_G
  WHERE (id, qtd) IN (
      SELECT id, max(qtd) AS max
      FROM visitedCellsByIds_G
      GROUP BY id
  )
  GROUP BY id, cell_id, qtd
  ORDER BY id
);

-- LESS VISITED CELLS IN GENERAL, GROUPED BY USER ID --
CREATE TEMPORARY TABLE lessVisitedCells_G AS (
  SELECT id, cell_id AS lessVisitedCells, qtd
  FROM visitedCellsByIds_G
  WHERE (id, qtd) IN (
      SELECT id, min(qtd) AS min
      FROM visitedCellsByIds_G
      GROUP BY id
  )
  GROUP BY id, cell_id, qtd
  ORDER BY id
);

-- LESS VISITED CELLS DURING OFF-WORKING HOURS, GROUPED BY USER ID --
CREATE TEMPORARY TABLE lessVisitedCells_H AS (
  SELECT id, cell_id AS lessVisitedCells, qtd
  FROM visitedCellsByIds_H
  WHERE (id, qtd) IN (
      SELECT id, min(qtd) AS min
      FROM visitedCellsByIds_H
      GROUP BY id
  )
  GROUP BY id, cell_id, qtd
  ORDER BY id
);

-- LESS VISITED CELLS DURING WORKING HOURS, GROUPED BY USER ID --
CREATE TEMPORARY TABLE lessVisitedCells_W AS (
  SELECT id, cell_id AS lessVisitedCells, qtd
  FROM visitedCellsByIds_W
  WHERE (id, qtd) IN (
      SELECT id, min(qtd) AS min
      FROM visitedCellsByIds_W
      GROUP BY id
  )
  GROUP BY id, cell_id, qtd
  ORDER BY id
);

