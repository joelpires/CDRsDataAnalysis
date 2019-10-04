------------------------------------------------------------------------------------------------------------------------
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
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE unique_fct_restructured AS (
  SELECT originating_id AS id,
       originating_cell_id AS cell_id,
       date_id,
       date,
       to_timestamp(trunc((((732677 - 719528) + (date_id/100000.0)-1))*24*60*60))::time AS time,
       duration_amt
  FROM unique_call_fct

  UNION ALL

  SELECT  terminating_id AS id,
          terminating_cell_id AS cell_id,
          date_id,
          date,
          to_timestamp(trunc((((732677 - 719528) + (date_id/100000.0)-1))*24*60*60))::time AS time,
          duration_amt
  FROM unique_call_fct
);

DROP TABLE unique_call_fct;
CREATE INDEX unique_index_date ON unique_fct_restructured (date);
CREATE INDEX unique_index_id ON unique_fct_restructured (id);

----------------------------------------------------------
CREATE TABLE visitedCellsByIds_G_u AS( -- DIFFERENT VISITED CELLS IN GENERAL, GROUPED BY USER ID --
  SELECT id, cell_id, count(*) AS qtd
  FROM unique_fct_restructured
  GROUP BY id, cell_id
);

----------------------------------------------------------
-- AMOUNT OF TALK BY USER IN THE REGION -- 1899216 users in total
CREATE TABLE durationsByUser_u AS(
  SELECT id, sum(duration_amt) as amountOfTalk
  FROM unique_fct_restructured
  GROUP BY id
);

----------------------------------------------------------
-- DIFFERENT ACTIVE DAYS, DIFFERENT NUMBER OF CALLS AND FREQUENCY OF CALLING --
CREATE TEMPORARY TABLE intermediate1 AS (SELECT id, date, count(*) AS qtd
                                         FROM unique_fct_restructured
                                         GROUP BY id, date);

CREATE TEMPORARY TABLE intermediate2 AS (
    SELECT id,
           date,
           sum(qtd) qtd,
           COALESCE(ROUND(ABS((date_part('day',age(date, lag(date) OVER (PARTITION BY id order by id)))/365 + date_part('month',age(date, lag(date) OVER (PARTITION BY id order by id)))/12 + date_part('year',age(date, lag(date) OVER (PARTITION BY id order by id))))*365 )), 0) as diffDays
    FROM intermediate1
    GROUP BY id, date
);

CREATE TABLE frequenciesByUser_u AS (
    SELECT id,
           count(date) AS activeDays,
           sum(qtd) AS numberCalls,
           sum(diffDays) AS sumDifferencesDays
    FROM intermediate2 b
    GROUP BY id
);
DROP TABLE intermediate1;
DROP TABLE intermediate2;

--issue: why this user has 0 activedays???
UPDATE frequenciesByUser_u
SET activeDays = 1
WHERE frequenciesID = 24965020;

/*
----------------------------------------------------------
-- OBTAIN THE TOTAL CALLS MADE BY EACH USER --
CREATE TEMPORARY TABLE totalCallsInsideRegionU AS(
  SELECT id, count(*) AS callsInsideRegion
  FROM unique_fct_restructured
  GROUP BY id
);
*/

----------------------------------------------------------
--  OBTAIN THE CALLS MADE/RECEIVED DURING THE WEEKDAYS  --
CREATE TABLE unique_fct_weekdays_u AS (
  SELECT *
  FROM unique_fct_restructured
  WHERE extract(isodow from date) -1 < 5
);

-- CREATING THE NECESSARY INDEXES
DROP TABLE unique_fct_restructured;
CREATE INDEX unique_fct_weekdays_id ON unique_fct_weekdays_u (id);
CREATE INDEX unique_fct_weekdays_cell_id ON unique_fct_weekdays_u (cell_id);
CREATE INDEX unique_fct_weekdays_time ON unique_fct_weekdays_u (time);

----------------------------------------------------------
--  OBTAIN THE NUMBER OF CALLS MADE/RECEIVED DURING THE WEEKDAYS GROUP BY USERID  --
CREATE TEMPORARY TABLE numberCallsWeekdays_u AS (
  SELECT id, count(*) AS numberCallsWeekdays
  FROM unique_fct_weekdays_u
  GROUP BY id
);

----------------------------------------------------------
--  OBTAIN THE NUMBER OF CALLS MADE/RECEIVED DURING THE HOME HOURS GROUP BY USERID  --
CREATE TEMPORARY TABLE numberCalls_home_hours_u AS (
  SELECT id, count(*) AS qtd
  FROM unique_fct_weekdays_u
  WHERE time > '22:00:00'::time OR time < '07:00:00'::time
  GROUP BY id
);

----------------------------------------------------------
--  OBTAIN THE NUMBER OF CALLS MADE/RECEIVED DURING THE WORKING HOURS GROUP BY USERID  --
CREATE TEMPORARY TABLE numberCalls_working_hours_u AS (
  SELECT id, count(*) AS qtd
  FROM unique_fct_weekdays_u
  WHERE (time > '9:00:00'::time AND time < '12:00:00'::time) OR (time > '14:30:00'::time AND time < '17:00:00'::time)
  GROUP BY id
);

----------------------------------------------------------
-- MOST VISITED CELLS --
-- HOME --
CREATE TEMPORARY TABLE visitedCellsByIds_H_u AS(  -- table with the visited cells by each user during the home hours
  SELECT id, cell_id, count(*) AS qtd
  FROM unique_fct_weekdays_u
  WHERE time > '22:00:00'::time OR time < '07:00:00'::time
  GROUP BY id, cell_id
);
----------------------------------------------------------
/*
Let's create a table that tells us which users have a well defined cellular tower for their home
  . 0, OR the user does not have registered calls during the hours that is supposed to be at home OR it was not possible to identify only one cellular tower with the most activity
  . 1, otherwise
*/

CREATE TABLE mostVisitedCells_H_u AS (
  SELECT id, cell_id AS mostVisitedCell, qtd
  FROM visitedCellsByIds_H_u
  WHERE (id, qtd) IN (
      SELECT id, max(qtd) AS max
      FROM visitedCellsByIds_H_u
      GROUP BY id
  )
  GROUP BY id, cell_id, qtd
);

CREATE TEMPORARY TABLE hasMostVisitedCell_H_u AS(
  SELECT id
  FROM durationsByUser_u
);


ALTER TABLE hasMostVisitedCell_H_u
ADD "has?" INTEGER DEFAULT 0; -- by default none of the users are eligible

UPDATE hasMostVisitedCell_H_u -- enabling the users that had registered call activity during the hours that is supposed to be working
SET "has?" = 1
WHERE id IN ( SELECT DISTINCT id FROM mostVisitedCells_H_u);

UPDATE hasMostVisitedCell_H_u -- disabling the users that registered more than one cell with max activity
SET "has?" = 0
WHERE id IN (
  SELECT id
  FROM mostVisitedCells_H_u ca
  INNER JOIN (SELECT id AS userid
              FROM mostVisitedCells_H_u
              GROUP BY id
              HAVING COUNT(0) > 1) ss
  ON ca.qtd = qtd
  AND ca.id = userid
);
----------------------------------------------------------

-- WORK --
CREATE TEMPORARY TABLE visitedCellsByIds_W_u AS(  -- table with the visited cells by each user during the working hours
  SELECT id, cell_id, count(*) AS qtd
  FROM unique_fct_weekdays_u
  WHERE (time > '9:00:00'::time AND time < '12:00:00'::time) OR (time > '14:30:00'::time AND time < '17:00:00'::time) -- respecting launch hours
  GROUP BY id, cell_id
);

----------------------------------------------------------
/*
Lets create a table that tells us which users have a well defined cellular tower for their workplace
  . 0, OR the user does not have registered calls during the hours that is supposed to be at work OR it was not possible to identify only one cellular tower with the most activity
  . 1, otherwise
*/

CREATE TABLE mostVisitedCells_W_u AS (
        SELECT id, cell_id as mostVisitedCell, qtd
        FROM visitedCellsByIds_W_u
        WHERE (id, qtd) IN (
            SELECT id, max(qtd) as max
            FROM visitedCellsByIds_W_u
            GROUP BY id
        )
        GROUP BY id, cell_id, qtd
        ORDER BY id, cell_id, qtd
);

CREATE TEMPORARY TABLE hasMostVisitedCell_W_u AS(
  SELECT id
  FROM durationsByUser_u
);

ALTER TABLE hasMostVisitedCell_W_u
ADD "has?" INTEGER DEFAULT 0; -- by default none of the users are eligible

UPDATE hasMostVisitedCell_W_u -- enabling the users that had registered call activity during the hours that is supposed to be at home
SET "has?" = 1
WHERE id IN (
    SELECT DISTINCT id FROM mostVisitedCells_W_u
);

UPDATE hasMostVisitedCell_W_u -- disabling the users that registered more than one cell with max activity
SET "has?" = 0
WHERE id IN (
  SELECT id
  FROM mostVisitedCells_W_u ca
  INNER JOIN (SELECT id AS userid
              FROM mostVisitedCells_W_u
              GROUP BY id
              HAVING COUNT(0) > 1) ss
  ON ca.qtd = qtd
  AND ca.id = userid
);

----------------------------------------------------------
CREATE TEMPORARY TABLE home_id_by_user_u AS (
  SELECT h.hid AS hid, home_id, latitude AS home_latitude, longitude AS home_longitude, geom_point AS geom_point_home

  FROM hasMostVisitedCell_H_u l

  LEFT JOIN (SELECT id AS Hid, mostVisitedCell AS home_id FROM mostVisitedCells_H_u) h
  ON "has?" = 1 AND id = Hid

  INNER JOIN call_dim p
  ON home_id = cell_id

);

CREATE TEMPORARY TABLE workplace_id_by_user_u AS (
  SELECT h.wid AS wid, workplace_id, latitude AS work_latitude, longitude AS work_longitude, geom_point AS geom_point_work
  FROM hasMostVisitedCell_W_u

  LEFT JOIN (SELECT id AS Wid, mostVisitedCell AS workplace_id FROM mostVisitedCells_W_u) h
  ON "has?" = 1 AND id = Wid

  INNER JOIN call_dim p
  ON workplace_id = cell_id
);


CREATE TEMPORARY TABLE home_workplace_by_user_u AS (
  SELECT hid AS eid, home_id, home_latitude, home_longitude, geom_point_home, workplace_id, work_latitude, work_longitude, geom_point_work
  FROM home_id_by_user_u j
  LEFT JOIN (SELECT Wid AS userid,* FROM workplace_id_by_user_u) l
  ON hid = userid

  UNION

  SELECT Wid AS eid, home_id, home_latitude, home_longitude, geom_point_home, workplace_id, work_latitude, work_longitude, geom_point_work
  FROM workplace_id_by_user_u j
  LEFT JOIN (SELECT hid AS userid,* FROM home_id_by_user_u) l
  ON wid = userid
);

----------------------------------------------------------
--  OBTAIN THE NUMBER OF CALLS MADE/RECEIVED DURING THE MORNING HOURS GROUP BY USERID  --
CREATE TEMPORARY TABLE numberCalls_morning_hours_u AS (
  SELECT id, count(*) AS numberCalls_morning_hours  -- calculating all the calls made during the morning
  FROM unique_fct_weekdays_u
  WHERE (time > '5:00:00'::time AND time < '12:00:00'::time)
  GROUP BY id
);

----------------------------------------------------------
--  OBTAIN THE NUMBER OF CALLS MADE/RECEIVED DURING THE EVENING HOURS
CREATE TEMPORARY TABLE morning_calls_u AS (
  SELECT *  -- calculating all the calls made during the morning
  FROM unique_fct_weekdays_u
  WHERE (time > '5:00:00'::time AND time < '12:00:00'::time)
  ORDER BY id, date_id
);

------------------------------------------------------------ TRAVEL TIMES HOME -> WORK (we are assuming people go to work in the morning) --------------------------------------------------------------
-- calculate the number of calls made at home during the morning group by user
CREATE TEMPORARY TABLE number_calls_home_morning_u AS(
  SELECT id, count(DISTINCT date_id) AS number_calls_home_morning
  FROM (
    SELECT *
    FROM (
      SELECT id,
             date,
             time,
             date_id,
             cell_id,
             home_id
      FROM morning_calls_u
      INNER JOIN (SELECT eid AS userid, home_id FROM home_workplace_by_user_u WHERE home_id IS NOT NULL) u
      ON id = userid
    ) h
    WHERE cell_id = home_id
  ) a
  GROUP BY id
);

------------------------------------------------------------
-- calculate the number of calls made at workplace during the morning group by user
CREATE TEMPORARY TABLE number_calls_work_morning_u AS(
  SELECT id, count(DISTINCT date_id) AS number_calls_work_morning
  FROM (
    SELECT *
    FROM (
      SELECT id,
             date,
             time,
             date_id,
             cell_id,
             workplace_id
      FROM morning_calls_u
      INNER JOIN (SELECT eid AS userid, workplace_id FROM home_workplace_by_user_u WHERE workplace_id IS NOT NULL) u
      ON id = userid
    ) h
    WHERE cell_id = workplace_id
  ) a
  GROUP BY id
);

------------------------------------------------------------
-- calculate the calls made at home or at workplace during the morning
CREATE TEMPORARY TABLE commuting_calls_morning_u AS(
  SELECT *
  FROM (
    SELECT id,
           date,
           time,
           date_id,
           cell_id,
           home_id,
           workplace_id
    FROM morning_calls_u
    INNER JOIN (SELECT eid AS userid, home_id, workplace_id FROM home_workplace_by_user_u WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL AND home_id != workplace_id) u
    ON id = userid
  ) h
  WHERE cell_id = home_id OR cell_id = workplace_id
  ORDER BY date_id
);


----------------------------------------------------------
 -- joining the last call made at home and the first call made in the workplace, during the morning, by each day by each user
CREATE TEMPORARY TABLE all_transitions_commuting_calls_morning_u AS(
  SELECT *, lag(cell_id) OVER(PARTITION BY id, date ORDER BY id, date_id) AS lagCell_id
  FROM (
    SELECT DISTINCT ON(id, date) *
    FROM (
       SELECT *
       FROM commuting_calls_morning_u
       WHERE cell_id = home_id
       ORDER BY id, date, time DESC, cell_id
    ) n


    UNION ALL

    SELECT DISTINCT ON(id, date) *
    FROM (
       SELECT *
       FROM commuting_calls_morning_u
       WHERE cell_id = workplace_id
       ORDER BY id, date, time ASC, cell_id
    ) n

    ORDER BY id, date_id

  ) xD
);

------------------------------------------------------------
-- cleaning the records of days in which the user did not made/received a call at work and at home
-- calculating already commuting traveltimes

CREATE TEMPORARY TABLE transitions_commuting_calls_morning_u AS (
  SELECT *
  FROM (
    SELECT id,
           date,
           time,
           date_id,
           cell_id,
           lagCell_id,
           home_id,
           workplace_id,
           (trunc((((732677 - 719528) + (date_id/100000.0)-1))*24*60*60) - trunc((((732677 - 719528) + ((lag(date_id) OVER(PARTITION BY id, date ORDER BY id, date_id))/100000.0)-1)*24*60*60)))  AS travelTime,
           lag(time) OVER(PARTITION BY id,date ORDER BY id, date_id) AS startdate_H_W,
           time AS finishdate_H_W

    FROM all_transitions_commuting_calls_morning_u ca
    INNER JOIN (SELECT id AS id_user, date AS datess, COUNT (0) qtd
                FROM all_transitions_commuting_calls_morning_u
                GROUP BY id, date
                HAVING COUNT (0) > 1
    ) ss
    ON id = id_user
    AND date = datess
  ) y
  WHERE lagCell_id = home_id AND cell_id = workplace_id     -- we are forcing that only people with the diurnal jobs are allowed
        OR lagCell_id IS NULL AND cell_id = home_id
);


------------------------------------------------------------
-- computing the average travel time and minimal travel times
-- WE ARE BELIEVING THAT THE MIN VALUE REPRESENTS THE MORE PROBABLE DURATION OF THE COMMUTING ROUTE
CREATE TEMPORARY TABLE travelTimes_H_W_u AS(
  SELECT id,
         averageTravelTime_H_W,
         minTravelTime_H_W,
         date AS date_H_W,
         startdate_H_W,
         finishdate_H_W

  FROM (
    SELECT id as idUser, min(travelTime) AS minTravelTime_H_W, CAST(sum(travelTime) AS FLOAT)/count(DISTINCT date) AS averageTravelTime_H_W
    FROM transitions_commuting_calls_morning_u
    GROUP BY id
  ) o
  INNER JOIN (SELECT *
              FROM transitions_commuting_calls_morning_u
  ) l
  ON id = idUser
  AND travelTime = minTravelTime_H_W
);
-- issue: solve this problem more properly. The problem is that there are users that call in a specific originating_cell_id at a specific date_id and at the same time
-- they are misteriously receiveing a call in a different terminating_cell_id
-- this needs to be deeply analyzed in the unique_call_fct dataset as a case 5 table
-- poderá ainda haver o caso de fazer table case 6 para users que chamam ou recebem mais que uma chamada ao mesmo tempo em células diferentes???

DELETE
FROM travelTimes_H_W_u
WHERE minTravelTime_H_W = 0;

--issue: e se houver mais que um minimum travel time, há que fazer merge!
CREATE TABLE new_travelTimes_H_W_u AS (
  SELECT DISTINCT ON(id, averageTravelTime_H_W, minTravelTime_H_W) *
  FROM travelTimes_H_W_u
);

----------------------------------------------------------
--  OBTAIN THE NUMBER OF CALLS MADE/RECEIVED DURING THE EVENING HOURS GROUP BY USERID  --
CREATE TEMPORARY TABLE numberCalls_evening_hours_u AS (
  SELECT id, count(*) AS numberCalls_evening_hours -- calculating all the calls made during the morning
  FROM unique_fct_weekdays_u
  WHERE (time > '15:00:00'::time AND time < '24:00:00'::time)
  GROUP BY id
);

------------------------------------------------------------
-- TRAVEL TIMES WORK -> HOME (we are assuming people go to home in the evening/night) --
-- calculating all the calls that took place at home or in the workplace during the evening
CREATE TEMPORARY TABLE evening_calls_u AS (
  SELECT *  -- calculating all the calls made during the morning
  FROM unique_fct_weekdays_u
  WHERE (time > '15:00:00'::time AND time < '24:00:00'::time)
  ORDER BY id, date_id
);


------------------------------------------------------------
-- calculate the number of calls made at home during the evening group by user
CREATE TEMPORARY TABLE number_calls_home_evening_u AS(
  SELECT id, count(DISTINCT date_id) AS number_calls_home_evening
  FROM (
    SELECT *
    FROM (
      SELECT id,
             date,
             time,
             date_id,
             cell_id,
             home_id,
             workplace_id
      FROM evening_calls_u
      INNER JOIN (SELECT eid AS userid, home_id, workplace_id FROM home_workplace_by_user_u WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL AND home_id != workplace_id) u
      ON id = userid
    ) h
    WHERE cell_id = home_id
  ) a
  GROUP BY id
);

------------------------------------------------------------
-- calculate the number of calls made at workplace during the evening group by user
CREATE TEMPORARY TABLE number_calls_work_evening_u AS(
  SELECT id, count(DISTINCT date_id) AS number_calls_work_evening
  FROM (
    SELECT *
    FROM (
      SELECT id,
             date,
             time,
             date_id,
             cell_id,
             home_id,
             workplace_id
      FROM evening_calls_u
      INNER JOIN (SELECT eid AS userid, home_id, workplace_id FROM home_workplace_by_user_u WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL AND home_id != workplace_id) u
      ON id = userid
    ) h
    WHERE cell_id = workplace_id
  ) a
  GROUP BY id
);

------------------------------------------------------------
-- calculate the calls made at home or at workplace during the evening
CREATE TEMPORARY TABLE commuting_calls_evening_u AS(
  SELECT *
  FROM (
    SELECT id,
           date,
           time,
           date_id,
           cell_id,
           home_id,
           workplace_id
    FROM evening_calls_u
    INNER JOIN (SELECT eid AS userid, home_id, workplace_id FROM home_workplace_by_user_u WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL AND home_id != workplace_id) u
    ON id = userid
  ) h
  WHERE cell_id = home_id OR cell_id = workplace_id
);

------------------------------------------------------------
 -- joining the last call made at work and the first call made at home, during the evening, by each day by each user
CREATE TEMPORARY TABLE all_transitions_commuting_calls_evening_u AS(
  SELECT *, lag(cell_id) OVER(PARTITION BY id, date ORDER BY id, date_id) AS lagCell_id
  FROM (
    SELECT DISTINCT ON(id, date) *
    FROM (
       SELECT *
       FROM commuting_calls_evening_u
       WHERE cell_id = home_id
       ORDER BY id, date, time ASC, cell_id
    ) n


    UNION ALL

    SELECT DISTINCT ON(id, date) *
    FROM (
       SELECT *
       FROM commuting_calls_evening_u
       WHERE cell_id = workplace_id
       ORDER BY id, date, time DESC, cell_id
    ) n

    ORDER BY id, date_id

  ) xD
);

------------------------------------------------------------
-- cleaning the records of days in which the user did not made/received a call at work and at home
-- calculating already commuting traveltimes
CREATE TEMPORARY TABLE transitions_commuting_calls_evening_u AS (
  SELECT *
  FROM (
    SELECT id,
           date,
           time,
           date_id,
           cell_id,
           lagCell_id,
           home_id,
           workplace_id,
           (trunc((((732677 - 719528) + (date_id/100000.0)-1))*24*60*60) - trunc((((732677 - 719528) + ((lag(date_id) OVER(PARTITION BY id, date ORDER BY id, date_id))/100000.0)-1)*24*60*60)))  AS travelTime,
           lag(time) OVER(PARTITION BY id,date ORDER BY id, date_id) AS startdate_W_H,
           time AS finishdate_W_H
    FROM all_transitions_commuting_calls_evening_u ca
    INNER JOIN (SELECT id AS id_user, date AS datess, COUNT (0) qtd
                FROM all_transitions_commuting_calls_evening_u
                GROUP BY id, date
                HAVING COUNT (0) > 1
    ) ss
    ON id = id_user
    AND date = datess
  ) q
    WHERE lagCell_id = home_id AND cell_id = workplace_id     -- we are forcing that only people with the diurnal jobs are allowed
        OR lagCell_id IS NULL AND cell_id = home_id
);

------------------------------------------------------------
-- computing the average travel time and minimal travel times
-- WE ARE BELIEVING THAT THE MIN VALUE REPRESENTS THE MORE PROBABLE DURATION OF THE COMMUTING ROUTE
CREATE TEMPORARY TABLE travelTimes_W_H_u AS(
  SELECT id,
         averageTravelTime_W_H,
         minTravelTime_W_H,
         date AS date_W_H,
         startdate_W_H,
         finishdate_W_H

  FROM (
    SELECT id as idUser, min(travelTime) AS minTravelTime_W_H, CAST(sum(travelTime) AS FLOAT)/count(DISTINCT date) AS averageTravelTime_W_H
    FROM transitions_commuting_calls_evening_u
    GROUP BY id
  ) o
  INNER JOIN (SELECT *
              FROM transitions_commuting_calls_evening_u
  ) l
  ON id = idUser
  AND travelTime = minTravelTime_W_H
);

-- issue: solve this problem more properly. The problem is that there are users that call in a specific originating_cell_id at a specific date_id and at the same time
-- they are misteriously receiveing a call in a different terminating_cell_id
-- this needs to be deeply analyzed in the unique_call_fct dataset as a case 5 table
-- poderá ainda haver o caso de fazer table case 6 para users que chamam ou recebem mais que uma chamada ao mesmo tempo em células diferentes???
DELETE
FROM travelTimes_W_H_u
WHERE minTravelTime_W_H = 0;

CREATE TABLE new_travelTimes_W_H_u AS (
  SELECT DISTINCT ON(id, averageTravelTime_W_H, minTravelTime_W_H) *
  FROM travelTimes_W_H_u
);

-- --------------------------------------------------------------------------- INTERMEDIATE CELL TOWERS WITHIN TRAVEL TIME ------------------------------------------------------------ --
-- HOME -> WORK

ALTER TABLE new_travelTimes_H_W_u
RENAME COLUMN id TO hwid;

CREATE TABLE intermediateTowers_H_W_u AS (
  SELECT intermediateTowers_H_WID, tower, longitude, latitude, geom_point,  CASE
                                                                            WHEN (tower = home_id) THEN 1
                                                                            ELSE 0
                                                                          END AS intermediateHome_H_W,   CASE
                                                                                                          WHEN (tower = workplace_id) THEN 1
                                                                                                          ELSE 0
                                                                                                        END AS intermediateWorkplace_H_W
  FROM (
    SELECT id AS intermediateTowers_H_WID, *
    FROM (
          SELECT *
          FROM unique_fct_weekdays_u p
          INNER JOIN (SELECT * FROM new_travelTimes_H_W_u) h ON hwid = p.id

          WHERE time > startdate_H_W
          AND time < finishdate_H_W
    ) t
    INNER JOIN (SELECT cell_id AS tower, longitude, latitude, geom_point FROM call_dim) u
    ON t.cell_id = tower
  ) g
  INNER JOIN home_workplace_by_user_u f
  ON intermediateTowers_H_WID = f.eid

);

ALTER TABLE new_travelTimes_W_H_u
RENAME COLUMN id TO whid;

-- WORK -> HOME
CREATE TABLE intermediateTowers_W_H_u AS (
  SELECT intermediateTowers_W_HID, tower, longitude, latitude, geom_point,  CASE
                                                                            WHEN (tower = home_id) THEN 1
                                                                            ELSE 0
                                                                          END AS intermediateHome_W_H,   CASE
                                                                                                          WHEN (tower = workplace_id) THEN 1
                                                                                                          ELSE 0
                                                                                                        END AS intermediateWorkplace_W_H
  FROM (
    SELECT id AS intermediateTowers_W_HID, *
    FROM (
          SELECT *
          FROM unique_fct_weekdays_u p
          INNER JOIN (SELECT * FROM new_travelTimes_W_H_u) h ON whid = p.id

          WHERE time > startdate_W_H
          AND time < finishdate_W_H
    ) t
    INNER JOIN (SELECT cell_id AS tower, longitude, latitude, geom_point FROM call_dim) u
    ON t.cell_id = tower
  ) g
  INNER JOIN home_workplace_by_user_u f
  ON intermediateTowers_W_HID = f.eid
);

ALTER TABLE frequenciesByUser_u
RENAME COLUMN id TO frequenciesID;

-- --------------------------------------------------------------------------- CHARACTERIZE USERS BY MULTIPLE PARAMETERS ------------------------------------------------------------ --
CREATE TEMPORARY TABLE users_characterization AS (
  SELECT *
  FROM (
    SELECT frequenciesID AS user_id,
           amountOfTalk AS "Total Amount of Talk",
           (numberCalls/ (1+sumDifferencesDays)) AS "Average Calls Per Day",
           CAST((1+sumDifferencesDays) AS FLOAT)/activeDays AS "Call Every x Days (on Average)",
           --CAST(callsInsideRegion AS FLOAT) * 100/numberCalls AS "Calls inside Region (%)",
           CAST(activeDays AS FLOAT)* 100 / 424  AS "Active Days / Period of the Study (%)",
           numberCalls AS "Nº Calls (Made/Received)",
           activeDays AS "Nº Active Days",
           differentvisitedplaces AS "Different Places Visited",
           CAST(amountOfTalk AS FLOAT)/ activeDays AS "Average Talk Per Day",
           CAST(amountOfTalk AS FLOAT)/ numberCalls AS "Average Amount of Talk Per Call",
           home_id,
           home_latitude,
           home_longitude,
           workplace_id,
           work_latitude,
           work_longitude,
           CAST(st_distance(ST_Transform(geom_point_home, 3857), ST_Transform(geom_point_work, 3857)) AS FLOAT)/1000 AS "Distance_H_W (kms)",
           averageTravelTime_H_W,
           minTravelTime_H_W,
           date_H_W,
           startdate_H_W,
           finishdate_H_W,
           (CAST(st_distance(ST_Transform(geom_point_home, 3857), ST_Transform(geom_point_work, 3857)) AS FLOAT)/1000)/(CAST(minTravelTime_H_W AS FLOAT)/60/60) AS "Travel Speed H_W (Km/h)",
           averageTravelTime_W_H,
           minTravelTime_W_H,
           date_W_H,
           startdate_W_H,
           finishdate_W_H,
           (CAST(st_distance(ST_Transform(geom_point_home, 3857), ST_Transform(geom_point_work, 3857)) AS FLOAT)/1000)/ (CAST(minTravelTime_W_H AS FLOAT)/60/60)  AS "Travel Speed W_H (Km/h)",
           number_intermediateTowers_H_W,
           number_intermediateTowers_W_H,
           numberCallsWeekdays AS "Number of Calls Made/Received During the Weekdays",
           numberCalls_home_hours AS "Number of Calls Made/Received During the Non-Working Hours",
           numberCalls_working_hours AS "Number of Calls Made/Received During the Working Hours",
           numberCalls_morning_hours AS "Number of Calls Made/Received During the Morning",
           numberCalls_evening_hours AS "Number of Calls Made/Received During the Evening",
           number_calls_home_morning AS "Number of Calls Made/Received at Home During the Morning",
           number_calls_work_morning AS "Number of Calls Made/Received in The Workplace During the Morning",
           number_calls_home_evening AS "Number of Calls Made/Received at Home During the Evening",
           number_calls_work_evening AS "Number of Calls Made/Received in The Workplace During the Evening"

    FROM frequenciesByUser_u aa

    INNER JOIN (
      SELECT id AS userid, count(cell_id) AS differentVisitedPlaces
      FROM visitedCellsByIds_G_u
      GROUP BY id
    ) b
    ON frequenciesID = userid

    INNER JOIN (SELECT id AS durationsID, amountOfTalk FROM durationsByUser_u) c
    ON frequenciesID = durationsID

    --INNER JOIN (SELECT id AS totalcallsID, callsInsideRegion FROM totalCallsInsideRegionU) f
    --ON frequenciesID = totalcallsID

    LEFT JOIN (SELECT eid AS localIDS,
                      home_id,
                      home_latitude,
                      home_longitude,
                      geom_point_home,
                      workplace_id,
                      work_latitude,
                      work_longitude,
                      geom_point_work
               FROM home_workplace_by_user_u) i
    ON frequenciesID = localIDS

    LEFT JOIN (SELECT id AS weekdaysID, numberCallsWeekdays
               FROM numberCallsWeekdays_u) rr
    ON frequenciesID = weekdaysID

    LEFT JOIN (SELECT id AS home_hoursID, qtd AS numberCalls_home_hours
               FROM numberCalls_home_hours_u) oo
    ON frequenciesID = home_hoursID

    LEFT JOIN (SELECT id AS working_hoursID, qtd AS numberCalls_working_hours
               FROM numberCalls_working_hours_u) ss
    ON frequenciesID = working_hoursID

    LEFT JOIN (SELECT id AS morning_hoursID, numberCalls_morning_hours
               FROM numberCalls_morning_hours_u) ww
    ON frequenciesID = morning_hoursID

    LEFT JOIN (SELECT  id AS evening_hoursID, numberCalls_evening_hours
               FROM numberCalls_evening_hours_u) ll
    ON frequenciesID = evening_hoursID

    LEFT JOIN (SELECT id AS home_morningID, number_calls_home_morning
               FROM number_calls_home_morning_u) ooo
    ON frequenciesID = home_morningID

    LEFT JOIN (SELECT id AS work_morningID, number_calls_work_morning
               FROM number_calls_work_morning_u) ppp
    ON frequenciesID = work_morningID

    LEFT JOIN (SELECT id AS home_eveningID, number_calls_home_evening
               FROM number_calls_home_evening_u) lll
    ON frequenciesID = home_eveningID

    LEFT JOIN (SELECT id AS work_eveningID, number_calls_work_evening
               FROM number_calls_work_evening_u) kkk
    ON frequenciesID = work_eveningID

    LEFT JOIN (SELECT id AS travelTimes_H_WID,
                      averageTravelTime_H_W,
                      minTravelTime_H_W,
                      date_H_W,
                      startdate_H_W,
                      finishdate_H_W
               FROM new_travelTimes_H_W_u) kkk1
    ON frequenciesID = travelTimes_H_WID

    LEFT JOIN (SELECT id AS travelTimes_W_HID,
                      averageTravelTime_W_H,
                      minTravelTime_W_H,
                      date_W_H,
                      startdate_W_H,
                      finishdate_W_H
               FROM new_travelTimes_W_H_u) kkk2
    ON frequenciesID = travelTimes_W_HID

    LEFT JOIN ( SELECT intermediateTowers_H_WID,
                       count(DISTINCT tower) AS number_intermediateTowers_H_W
                FROM intermediateTowers_H_W_u
                GROUP BY intermediateTowers_H_WID
              ) kkkl
    ON frequenciesID = intermediateTowers_H_WID

   LEFT JOIN ( SELECT intermediateTowers_W_HID,
                      count(DISTINCT tower)  AS number_intermediateTowers_W_H
                FROM intermediateTowers_W_H_u
                GROUP BY intermediateTowers_W_HID
              ) kkko
    ON frequenciesID = intermediateTowers_W_HID

  ) llli
  /*minimum requirements*/
  --WHERE "Average Talk Per Day" < 18000 -- less than 5 hours of talk per day
  --AND "Average Calls Per Day" < 4 * 12 -- someone that is working is not able to constantly being on the phone, so we limited to 3 calls per hour on average
  --AND "Average Calls Per Day" > 1 -- at least (almost) two calls per day on average in order to us being able compute commuting trips
  --AND "Nº Active Days" > 1 * 7 -- at least one week of call activity
  --AND "Different Places Visited" >= 2 -- visited at least two different places
);
SELECT * FROM statsmunicipals;
--------------------------------- FILTRATING THE USERS FROM MADEIRA AND AZORES -----------------------------------------
UPDATE experiment_stats
SET total_users_characterization = (SELECT count(*) FROM users_characterization);

DELETE
FROM users_characterization
WHERE home_id IN (SELECT cell_id FROM call_dim WHERE region = 1);

DELETE
FROM users_characterization
WHERE home_id IN (SELECT cell_id FROM call_dim WHERE region = 2);

DELETE
FROM users_characterization
WHERE workplace_id IN (SELECT cell_id FROM call_dim WHERE region = 1);

DELETE
FROM users_characterization
WHERE workplace_id IN (SELECT cell_id FROM call_dim WHERE region = 2);

DROP TABLE IF EXISTS infomunicipals_and_cells;
CREATE TEMPORARY TABLE infomunicipals_and_cells AS (
  SELECT f.name_2, "Tower Density (Km2 per Cell)" AS "Tower Density (Km2 per Cell)", g.cell_id
  FROM cell_idsbyregions g
  INNER JOIN statsmunicipals f
  ON g.name_2 = f.name_2
);

CREATE TEMPORARY TABLE megatable AS (
  SELECT f.*, name_2 AS municipalHome, "Tower Density (Km2 per Cell)" AS densityHome
  FROM infomunicipals_and_cells g
  RIGHT JOIN users_characterization f
  ON g.cell_id = f.home_id
);
DROP TABLE users_characterization_final
CREATE TABLE users_characterization_final AS ( -- aparently users only work or live in 275 municipals from 297 municipals that have cell towers. Remember as well that initially there are 306 municipals in Portugal, but only 297 municipals have towers
  SELECT f.*, name_2 AS municipalWorkplace, "Tower Density (Km2 per Cell)" AS densityWorkplace
  FROM infomunicipals_and_cells g
  RIGHT JOIN megatable f
  ON g.cell_id = f.workplace_id
);

UPDATE experiment_stats
SET total_users_characterization_final = (SELECT count(*) FROM users_characterization_final);
SELECT * FROM experiment_stats;

-- ----------------------------------------------------------------------------------------- EXPERIMENT 5: Relation Between the 3 varibles -------------------------------------------------------------------- --
CREATE TABLE experiment5 AS (
  SELECT "Call Every x Days (on Average)", "Average Calls Per Day", "Nº Active Days"
  FROM users_characterization_final
);

-- ----------------------------------------------------------------------------------------- EXPERIMENT 4: TOWER DENSITY -------------------------------------------------------------------- --

/*
Experiment 4.1: Universe is every user that has house and workplace well-defined, both inside the municipal
*/

CREATE TEMPORARY TABLE users_characterization_experiment4_1 AS (
  SELECT *, CASE
            WHEN (home_id = workplace_id) THEN 1
            ELSE 0
          END AS notdistinct
  FROM users_characterization_final
  WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL AND municipalHome IS NOT NULL AND municipalWorkplace IS NOT NULL
);

UPDATE experiment_stats
SET users_characterization_experiment4_1 = (SELECT count(*) FROM users_characterization_experiment4_1);


CREATE TEMPORARY TABLE usersDEN_byMunicipal AS(
  SELECT municipalHome , densityHome, count(*) AS usersDEN
  FROM (
    SELECT *
    FROM users_characterization_experiment4_1
  ) u
  GROUP BY municipalHome, densityHome
);

CREATE TEMPORARY TABLE usersNUM_byMunicipal AS(
  SELECT municipalWorkplace, densityWorkplace, count(*) AS usersNUM
  FROM (
    SELECT *
    FROM users_characterization_experiment4_1
    WHERE notdistinct = 0
  ) u
  GROUP BY municipalWorkplace, densityWorkplace
);

CREATE  TABLE experiment_4_1 AS (
  SELECT municipalHome AS municipal, densityHome AS tower_density, usersNUM, usersDEN,  CASE WHEN CAST(usersNUM AS FLOAT)*100/usersDEN IS NULL
                                                                                         THEN 0
                                                                                         ELSE CAST(usersNUM AS FLOAT)*100/usersDEN

                                                                                 END AS racio
  FROM usersNUM_byMunicipal h
  RIGHT  JOIN usersDEN_byMunicipal j
      ON municipalHome = municipalWorkplace
  ORDER BY tower_density
);

SELECT sum(usersnum) FROM experiment_4_1;
-- THE JUSTIFICATION FOR THESE RESULTS CAN BE BECAUSE WE CAN'T DISTINGUISH THE USERS THAT ACTUALLY WORK AND LIVE WITHIN THE SAME CELL AREA AND FROM THE ONES THAT ARE WORK AND LIVE IN DIFFERENT LOCATIONS IF WE COULD AUGMENT THE SPATIAL RESOLUTION

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
Experiment 4.2: Universe is every user that intermediate calls
*/

CREATE TEMPORARY TABLE aux_intermediateTowers_H_W AS (
  SELECT f.intermediateTowers_H_WID AS id, name_2, "Tower Density (Km2 per Cell)", tower, intermediateHome_H_W, intermediateWorkplace_H_W
  FROM intermediateTowers_H_W_u f
  INNER JOIN infomunicipals_and_cells g
  ON g.cell_id = f.tower
);

CREATE TEMPORARY TABLE aux_intermediateTowers_W_H AS (
  SELECT f.intermediateTowers_W_HID AS id, name_2, "Tower Density (Km2 per Cell)", tower, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM intermediateTowers_W_H_u f
  INNER JOIN infomunicipals_and_cells g
  ON g.cell_id = f.tower
);

CREATE TABLE aux_experiment4_2 AS (
  SELECT f."Tower Density (Km2 per Cell)", f.id, f.name_2,  f.tower, intermediateHome_H_W, intermediateWorkplace_H_W, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM aux_intermediateTowers_H_W f
  LEFT JOIN aux_intermediateTowers_W_H g
  ON f.id = g.id

  UNION

  SELECT f."Tower Density (Km2 per Cell)", f.id, f.name_2,  f.tower, intermediateHome_H_W, intermediateWorkplace_H_W, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM aux_intermediateTowers_W_H f
  LEFT JOIN aux_intermediateTowers_H_W g
  ON f.id = g.id

);

SELECT count(*) FROM aux_experiment4_2; -- should be equal to SELECT count(*) FROM users_characterization WHERE number_intermediateTowers_W_H IS NOT NULL AND number_intermediateTowers_H_W IS NOT NULL

-- EXPERIMENT 4_2_1:
CREATE TEMPORARY TABLE experiment4_2_1_universe AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", id, intermediateHome_H_W, intermediateWorkplace_H_W
  FROM aux_experiment4_2
  WHERE intermediateHome_H_W IS NOT NULL AND intermediateWorkplace_H_W IS NOT NULL
);

UPDATE experiment_stats
SET users_characterization_experiment4_2_1 = (SELECT count(DISTINCT id) FROM experiment4_2_1_universe);


CREATE TEMPORARY TABLE experiment4_2_1_usersDEN_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W) AS usersDEN
  FROM (
      SELECT name_2, "Tower Density (Km2 per Cell)", id, count(*) AS intermediateTowersH_W
      FROM experiment4_2_1_universe
      GROUP BY name_2, "Tower Density (Km2 per Cell)", id
  ) t
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

CREATE TEMPORARY TABLE experiment4_2_1_usersNUM_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_notH_notW) AS usersNUM
  FROM (
    SELECT p.name_2, "Tower Density (Km2 per Cell)", id, coalesce(count(*),0) AS intermediateTowersH_W_notH_notW
    FROM (
      SELECT *
      FROM experiment4_2_1_universe
      WHERE intermediateHome_H_W = 0 AND intermediateWorkplace_H_W = 0
    ) y
    RIGHT JOIN (SELECT DISTINCT name_2 FROM (SELECT DISTINCT ON (name_2) * FROM experiment4_2_1_universe)u)p
    ON p.name_2 = y.name_2
    GROUP BY p.name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);
--------------------------------------------------------------------------------------
-- EXPERIMENT 4_2_2:
CREATE TEMPORARY TABLE experiment4_2_2_universe AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", id, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM aux_experiment4_2
  WHERE intermediateHome_W_H IS NOT NULL AND intermediateWorkplace_W_H IS NOT NULL
);

UPDATE experiment_stats
SET users_characterization_experiment4_2_2 = (SELECT count(DISTINCT id) FROM experiment4_2_2_universe);


CREATE TEMPORARY TABLE experiment4_2_2_usersDEN_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersW_H) AS usersDEN
  FROM (
    SELECT name_2, "Tower Density (Km2 per Cell)", id, count(*) AS intermediateTowersW_H
    FROM experiment4_2_2_universe
    GROUP BY name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

CREATE TEMPORARY TABLE experiment4_2_2_usersNUM_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersW_H_notH_notW) AS usersNUM
  FROM (
    SELECT p.name_2, "Tower Density (Km2 per Cell)", id, coalesce(count(*),0) AS intermediateTowersW_H_notH_notW
    FROM (
        SELECT *
        FROM experiment4_2_2_universe
        WHERE intermediateHome_W_H = 0 AND intermediateWorkplace_W_H = 0
    ) y
    RIGHT JOIN (SELECT DISTINCT name_2 FROM (SELECT DISTINCT ON (name_2) * FROM experiment4_2_2_universe)u)p
    ON p.name_2 = y.name_2

    GROUP BY p.name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

--------------------------------------------------------------------------------------
-- EXPERIMENT 4_2_3:

CREATE TEMPORARY TABLE experiment4_2_3_universe AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", id, intermediateHome_H_W, intermediateWorkplace_H_W, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM aux_experiment4_2
    WHERE (intermediateHome_H_W IS NOT NULL AND intermediateWorkplace_H_W IS NOT NULL) OR (intermediateHome_W_H IS NOT NULL AND intermediateWorkplace_W_H IS NOT NULL)
);

UPDATE experiment_stats
SET users_characterization_experiment4_2_3 = (SELECT count(DISTINCT id) FROM experiment4_2_3_universe);

CREATE TEMPORARY TABLE experiment4_2_3_usersDEN_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_H) AS usersDEN
  FROM (
    SELECT name_2, "Tower Density (Km2 per Cell)", id, count(*) AS intermediateTowersH_W_H
    FROM experiment4_2_3_universe
    GROUP BY name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

CREATE TEMPORARY TABLE experiment4_2_3_usersNUM_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_H_notH_notW) AS usersNUM
  FROM (
    SELECT p.name_2, "Tower Density (Km2 per Cell)", id, coalesce(count(*),0) AS intermediateTowersH_W_H_notH_notW
    FROM (
        SELECT *
        FROM experiment4_2_3_universe
        WHERE ((intermediateHome_H_W = 0 AND intermediateWorkplace_H_W = 0) OR (intermediateHome_W_H = 0 AND intermediateWorkplace_W_H = 0))
    ) y
    RIGHT JOIN (SELECT DISTINCT name_2 FROM (SELECT DISTINCT ON (name_2) * FROM experiment4_2_3_universe)u)p
    ON p.name_2 = y.name_2
    GROUP BY p.name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

--------------------------------------------------------------------------------------
-- EXPERIMENT 4_2_4:

CREATE TEMPORARY TABLE experiment4_2_4_universe AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", id, intermediateHome_H_W, intermediateWorkplace_H_W, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM aux_experiment4_2
    WHERE (intermediateHome_H_W IS NOT NULL AND intermediateWorkplace_H_W IS NOT NULL) AND (intermediateHome_W_H IS NOT NULL AND intermediateWorkplace_W_H IS NOT NULL)
);

UPDATE experiment_stats
SET users_characterization_experiment4_2_4 = (SELECT count(DISTINCT id) FROM experiment4_2_4_universe);

CREATE TEMPORARY TABLE experiment4_2_4_usersDEN_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_H) AS usersDEN
  FROM (
    SELECT name_2, "Tower Density (Km2 per Cell)", id, count(*) AS intermediateTowersH_W_H
    FROM experiment4_2_4_universe
    GROUP BY name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

CREATE TEMPORARY TABLE experiment4_2_4_usersNUM_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_H_notH_notW) AS usersNUM
  FROM (
    SELECT p.name_2, "Tower Density (Km2 per Cell)", id, coalesce(count(*),0) AS intermediateTowersH_W_H_notH_notW
    FROM (
        SELECT *
        FROM experiment4_2_4_universe
        WHERE ((intermediateHome_H_W = 0 AND intermediateWorkplace_H_W = 0) AND (intermediateHome_W_H = 0 AND intermediateWorkplace_W_H = 0))
      ) y
    RIGHT JOIN (SELECT DISTINCT name_2 FROM (SELECT DISTINCT ON (name_2) * FROM experiment4_2_4_universe)u)p
    ON p.name_2 = y.name_2
    GROUP BY p.name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

--------------------------------------------------------------------------------------
CREATE TABLE experiment_4_2 AS (
  SELECT a.name_2, a."Tower Density (Km2 per Cell)", coalesce(CAST(b.usersNUM AS FLOAT)*100/a.usersDEN,0) AS racioH_W, coalesce(CAST(d.usersNUM AS FLOAT)*100/c.usersDEN,0) AS racioW_H, coalesce(CAST(f.usersNUM AS FLOAT)*100/e.usersDEN,0) AS racioH_W_or_W_H, coalesce(CAST(h.usersNUM AS FLOAT)*100/g.usersDEN,0) AS racioH_W_and_W_H
  FROM experiment4_2_1_usersDEN_byMunicipal a

  LEFT JOIN experiment4_2_1_usersNUM_byMunicipal b
  ON a.name_2 = b.name_2

  LEFT JOIN experiment4_2_2_usersDEN_byMunicipal c
  ON a.name_2 = c.name_2

  LEFT JOIN experiment4_2_2_usersNUM_byMunicipal d
  ON a.name_2 = d.name_2

  LEFT JOIN experiment4_2_3_usersDEN_byMunicipal e
  ON a.name_2 = e.name_2

  LEFT JOIN experiment4_2_3_usersNUM_byMunicipal f
  ON a.name_2 = f.name_2

  LEFT JOIN experiment4_2_4_usersDEN_byMunicipal g
  ON a.name_2 = g.name_2

  LEFT JOIN experiment4_2_4_usersNUM_byMunicipal h
  ON a.name_2 = h.name_2

);


--------------------------------------------------------------------------------------


-- ----------------------------------------------------------------------------------------- EXPERIMENT 3: NUMBER OF DIFFERENT DAYS -------------------------------------------------------------------- --
/*
Experiment 3.1: Universe is every user < x ActiveDays
*/

CREATE TEMPORARY TABLE experiment_3_universe AS (
  SELECT "Nº Active Days", user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" AS M_H, "Number of Calls Made/Received in The Workplace During the Morning" AS M_W, "Number of Calls Made/Received at Home During the Evening" AS E_H, "Number of Calls Made/Received in The Workplace During the Evening" AS E_W, "Number of Calls Made/Received During the Weekdays" AS nweekdays
  FROM users_characterization
  GROUP BY "Nº Active Days", user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" , "Number of Calls Made/Received in The Workplace During the Morning", "Number of Calls Made/Received at Home During the Evening", "Number of Calls Made/Received in The Workplace During the Evening", "Number of Calls Made/Received During the Weekdays"
  ORDER BY "Nº Active Days"
);

CREATE TEMPORARY TABLE experiment_3_usersDEN AS (
  SELECT "Nº Active Days", sum(pre_usersDEN) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersDEN  -- cumulative sum of users
  FROM (
    SELECT "Nº Active Days", count(DISTINCT user_id) AS pre_usersDEN
    FROM experiment_3_universe
    GROUP BY "Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersDEN
);

-----------------------------------------------------
--EXPERIMENT 3.1.1:
CREATE TEMPORARY TABLE experiment_3_1_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
         SELECT *
         FROM experiment_3_universe
         WHERE home_id IS NOT NULL
    ) t
     RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
      ON p."Nº Active Days" = t."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_1_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE home_id IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 3.1.2:
CREATE TEMPORARY TABLE experiment_3_1_2_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_3_universe
      WHERE workplace_id IS NOT NULL
    ) y
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = y."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_1_2 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE workplace_id IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 3.1.3:
CREATE TEMPORARY TABLE experiment_3_1_3_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_3_universe
      WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL
    ) i
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = i."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_1_3 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL
                                             );
-----------------------------------------------------
--Results experiment_3_1:
CREATE TABLE experiment_3_1 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace
  FROM experiment_3_usersDEN a
  LEFT JOIN experiment_3_1_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
  LEFT JOIN experiment_3_1_2_usersNUM c
  ON a."Nº Active Days" = c."Nº Active Days"
  LEFT JOIN experiment_3_1_3_usersNUM d
  ON a."Nº Active Days" = d."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM experiment_3_1;

/*
Experiment 3.2: Universe is every user < x ActiveDays
*/

-----------------------------------------------------
--EXPERIMENT 3.2.1:
CREATE TEMPORARY TABLE experiment_3_2_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_3_universe
      WHERE M_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_2_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE M_H IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 3.2.2:
CREATE TEMPORARY TABLE experiment_3_2_2_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_3_universe
      WHERE M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_2_2 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE M_W IS NOT NULL
                                             );
-----------------------------------------------------
--EXPERIMENT 3.2.3:
CREATE TEMPORARY TABLE experiment_3_2_3_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_3_universe
      WHERE M_H IS NOT NULL AND M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_2_3 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE M_H IS NOT NULL AND M_W IS NOT NULL
                                             );
-----------------------------------------------------
--Results:
CREATE TABLE experiment_3_2 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Morning,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Morning,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Morning
  FROM experiment_3_usersDEN a
  LEFT JOIN experiment_3_2_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
  LEFT JOIN experiment_3_2_2_usersNUM c
  ON a."Nº Active Days" = c."Nº Active Days"
  LEFT JOIN experiment_3_2_3_usersNUM d
  ON a."Nº Active Days" = d."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM experiment_3_2;



/*
Experiment 3.3: Universe is every user < x ActiveDays
*/

-----------------------------------------------------
--EXPERIMENT 3.3.1:
CREATE TEMPORARY TABLE experiment_3_3_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id)) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_3_universe
      WHERE E_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_3_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE E_H IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 3.3.2:
CREATE TEMPORARY TABLE experiment_3_3_2_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_3_universe
      WHERE E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_3_2 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE E_W IS NOT NULL
                                             );
-----------------------------------------------------
--EXPERIMENT 3.3.3:
CREATE TEMPORARY TABLE experiment_3_3_3_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_3_universe
      WHERE E_H IS NOT NULL AND E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_3_3 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE E_H IS NOT NULL AND E_W IS NOT NULL
                                             );
-----------------------------------------------------
--Results:
CREATE TABLE experiment_3_3 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Evening,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Evening,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Evening
  FROM experiment_3_usersDEN a
  LEFT JOIN experiment_3_3_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
  LEFT JOIN experiment_3_3_2_usersNUM c
  ON a."Nº Active Days" = c."Nº Active Days"
  LEFT JOIN experiment_3_3_3_usersNUM d
  ON a."Nº Active Days" = d."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM experiment_3_3;

/*
Experiment 3.4: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 3.4.1:
CREATE TEMPORARY TABLE experiment_3_4_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f."Nº Active Days", f.user_id
      FROM experiment_3_universe f
      INNER JOIN intermediateTowers_H_W_u g
          ON f.user_id = g.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = u."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);


UPDATE experiment_stats
SET users_characterization_experiment3_4_1 = (SELECT cumulative_usersNUM
                                              FROM experiment_3_4_1_usersNUM
                                              ORDER BY "Nº Active Days" DESC
                                              LIMIT 1
                                             );

-----------------------------------------------------
--EXPERIMENT 3.4.2:

CREATE TEMPORARY TABLE experiment_3_4_2_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f."Nº Active Days", f.user_id
      FROM experiment_3_universe f
      INNER JOIN intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
    ) u
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = u."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_4_2 = (SELECT cumulative_usersNUM
                                              FROM experiment_3_4_2_usersNUM
                                              ORDER BY "Nº Active Days" DESC
                                              LIMIT 1
                                             );

-----------------------------------------------------
--EXPERIMENT 3.4.3:
CREATE TEMPORARY TABLE experiment_3_4_3_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f."Nº Active Days", f.user_id
      FROM experiment_3_universe f
      INNER JOIN aux_experiment4_2 g
          ON f.user_id = g.id
    ) u
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = u."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_4_3 = (SELECT cumulative_usersNUM
                                              FROM experiment_3_4_3_usersNUM
                                              ORDER BY "Nº Active Days" DESC
                                              LIMIT 1
                                             );

-----------------------------------------------------
--EXPERIMENT 3.4.4:
CREATE TEMPORARY TABLE experiment_3_4_4_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f."Nº Active Days", f.user_id
      FROM experiment_3_universe f
      INNER JOIN intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
      INNER JOIN intermediateTowers_H_W_u t
          ON f.user_id = t.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = u."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);
SELECT * FROM experiment_3_4_3_usersNUM;

UPDATE experiment_stats
SET users_characterization_experiment3_4_4 = (SELECT cumulative_usersNUM
                                              FROM experiment_3_4_4_usersNUM
                                              ORDER BY "Nº Active Days" DESC
                                              LIMIT 1
                                             );


-----------------------------------------------------
-- Results:
CREATE TABLE experiment_3_4 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioW_H,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_or_W_H,
         coalesce(CAST(e.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_and_W_H
  FROM experiment_3_usersDEN a
  LEFT JOIN experiment_3_4_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
  LEFT JOIN experiment_3_4_2_usersNUM c
  ON a."Nº Active Days" = c."Nº Active Days"
  LEFT JOIN experiment_3_4_3_usersNUM d
  ON a."Nº Active Days" = d."Nº Active Days"
  LEFT JOIN experiment_3_4_4_usersNUM e
  ON a."Nº Active Days" = e."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM experiment_3_4;

/*
Experiment 3.5: Universe is every user < x averageCalls
*/
-----------------------------------------------------
--EXPERIMENT 3.5.1:
CREATE TEMPORARY TABLE experiment_3_5_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_3_universe
      WHERE nweekdays IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment3_5_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_3_universe
                                              WHERE nweekdays IS NOT NULL
                                             );

-----------------------------------------------------
--Results:
CREATE TABLE experiment_3_5 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWeekdays
  FROM experiment_3_usersDEN a
  LEFT JOIN experiment_3_5_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM experiment_3_5;

-- ----------------------------------------------------------------------------------------- EXPERIMENT 2: REGULARITY -------------------------------------------------------------------- --
/*
Experiment 2.1: Universe is every user < x ActiveDays
*/

CREATE TEMPORARY TABLE experiment_2_universe AS (
  SELECT ROUND(CAST("Call Every x Days (on Average)" AS NUMERIC),1) AS regularity, user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" AS M_H, "Number of Calls Made/Received in The Workplace During the Morning" AS M_W, "Number of Calls Made/Received at Home During the Evening" AS E_H, "Number of Calls Made/Received in The Workplace During the Evening" AS E_W, "Number of Calls Made/Received During the Weekdays" AS nweekdays
  FROM users_characterization_final
  GROUP BY "Call Every x Days (on Average)", user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" , "Number of Calls Made/Received in The Workplace During the Morning", "Number of Calls Made/Received at Home During the Evening", "Number of Calls Made/Received in The Workplace During the Evening", "Number of Calls Made/Received During the Weekdays"
  ORDER BY "Call Every x Days (on Average)"
);

CREATE TEMPORARY TABLE experiment_2_usersDEN AS (
  SELECT regularity, sum(pre_usersDEN) OVER (ORDER BY regularity DESC) AS cumulative_usersDEN  -- cumulative sum of users
  FROM (
    SELECT regularity, count(DISTINCT user_id) AS pre_usersDEN
    FROM experiment_2_universe
    GROUP BY regularity
  ) t
  GROUP BY regularity, pre_usersDEN
);

-----------------------------------------------------
--EXPERIMENT 2.1.1:
CREATE TEMPORARY TABLE experiment_2_1_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT y.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
         SELECT *
         FROM experiment_2_universe
         WHERE home_id IS NOT NULL
    ) t
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)y
        ON y.regularity = t.regularity
    GROUP BY y.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);



UPDATE experiment_stats
SET users_characterization_experiment2_1_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE home_id IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 2.1.2:
CREATE TEMPORARY TABLE experiment_2_1_2_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_2_universe
      WHERE workplace_id IS NOT NULL
    ) y
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = y.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_1_2 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE workplace_id IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 2.1.3:
CREATE TEMPORARY TABLE experiment_2_1_3_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_2_universe
      WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL
    ) i
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = i.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_1_3 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL
                                             );
-----------------------------------------------------
--Results experiment_2_1:
CREATE TABLE experiment_2_1 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace
  FROM experiment_2_usersDEN a
  LEFT JOIN experiment_2_1_1_usersNUM b
  ON a.regularity = b.regularity
  LEFT JOIN experiment_2_1_2_usersNUM c
  ON a.regularity = c.regularity
  LEFT JOIN experiment_2_1_3_usersNUM d
  ON a.regularity = d.regularity
);
-- check if the number of different days are preserved
SELECT * FROM experiment_2_1 ORDER BY regularity DESC;


/*
Experiment 2.2: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 2.2.1:
CREATE TEMPORARY TABLE experiment_2_2_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_2_universe
      WHERE M_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_2_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE M_H IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 2.2.2:
CREATE TEMPORARY TABLE experiment_2_2_2_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_2_universe
      WHERE M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_2_2 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE M_W IS NOT NULL
                                             );
-----------------------------------------------------
--EXPERIMENT 2.2.3:
CREATE TEMPORARY TABLE experiment_2_2_3_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_2_universe
      WHERE M_H IS NOT NULL AND M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_2_3 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE M_H IS NOT NULL AND M_W IS NOT NULL
                                             );
-----------------------------------------------------
--Results:
CREATE TABLE experiment_2_2 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Morning,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Morning,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Morning
  FROM experiment_2_usersDEN a
  LEFT JOIN experiment_2_2_1_usersNUM b
  ON a.regularity = b.regularity
  LEFT JOIN experiment_2_2_2_usersNUM c
  ON a.regularity = c.regularity
  LEFT JOIN experiment_2_2_3_usersNUM d
  ON a.regularity = d.regularity
);
-- check if the number of different days are preserved
SELECT * FROM experiment_2_2;



/*
Experiment 2.3: Universe is every user < x ActiveDays
*/

-----------------------------------------------------
--EXPERIMENT 2.3.1:
CREATE TEMPORARY TABLE experiment_2_3_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_2_universe
      WHERE E_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_3_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE E_H IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 2.3.2:
CREATE TEMPORARY TABLE experiment_2_3_2_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_2_universe
      WHERE E_W IS NOT NULL
    ) o

    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_3_2 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE E_W IS NOT NULL
                                             );
-----------------------------------------------------
--EXPERIMENT 2.3.3:
CREATE TEMPORARY TABLE experiment_2_3_3_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_2_universe
      WHERE E_H IS NOT NULL AND E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_3_3 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE E_H IS NOT NULL AND E_W IS NOT NULL
                                             );
-----------------------------------------------------
--Results:
DROP TABLE experiment_2_3;
CREATE TABLE experiment_2_3 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Evening,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Evening,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Evening
  FROM experiment_2_usersDEN a
  LEFT JOIN experiment_2_3_1_usersNUM b
  ON a.regularity = b.regularity
  LEFT JOIN experiment_2_3_2_usersNUM c
  ON a.regularity = c.regularity
  LEFT JOIN experiment_2_3_3_usersNUM d
  ON a.regularity = d.regularity
);
-- check if the number of different days are preserved
SELECT * FROM experiment_2_3;

/*
Experiment 2.4: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 2.4.1:
CREATE TEMPORARY TABLE experiment_2_4_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.regularity, f.user_id
      FROM experiment_2_universe f
      INNER JOIN intermediateTowers_H_W_u g
          ON f.user_id = g.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = u.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);


UPDATE experiment_stats
SET users_characterization_experiment2_4_1 = (SELECT cumulative_usersNUM
                                              FROM experiment_2_4_1_usersNUM
                                              ORDER BY regularity ASC
                                              LIMIT 1
                                             );

-----------------------------------------------------
--EXPERIMENT 2.4.2:

CREATE TEMPORARY TABLE experiment_2_4_2_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.regularity, f.user_id
      FROM experiment_2_universe f
      INNER JOIN intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
    ) u
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = u.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_4_2 = (SELECT cumulative_usersNUM
                                              FROM experiment_2_4_2_usersNUM
                                              ORDER BY regularity ASC
                                              LIMIT 1
                                             );

-----------------------------------------------------
--EXPERIMENT 2.4.3:
CREATE TEMPORARY TABLE experiment_2_4_3_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.regularity, f.user_id
      FROM experiment_2_universe f
      INNER JOIN aux_experiment4_2 g
          ON f.user_id = g.id
    ) u
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = u.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_4_3 = (SELECT cumulative_usersNUM
                                              FROM experiment_2_4_3_usersNUM
                                              ORDER BY regularity ASC
                                              LIMIT 1
                                             );

-----------------------------------------------------
--EXPERIMENT 2.4.4:
CREATE TEMPORARY TABLE experiment_2_4_4_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.regularity, f.user_id
      FROM experiment_2_universe f
      INNER JOIN intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
      INNER JOIN intermediateTowers_H_W_u t
          ON f.user_id = t.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = u.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);
SELECT * FROM experiment_2_4_4_usersNUM;

UPDATE experiment_stats
SET users_characterization_experiment2_4_4 = (SELECT cumulative_usersNUM
                                              FROM experiment_2_4_4_usersNUM
                                              ORDER BY regularity ASC
                                              LIMIT 1
                                             );


-----------------------------------------------------
-- Results:
CREATE TABLE experiment_2_4 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioW_H,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_or_W_H,
         coalesce(CAST(e.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_and_W_H
  FROM experiment_2_usersDEN a
  LEFT JOIN experiment_2_4_1_usersNUM b
  ON a.regularity = b.regularity
  LEFT JOIN experiment_2_4_2_usersNUM c
  ON a.regularity = c.regularity
  LEFT JOIN experiment_2_4_3_usersNUM d
  ON a.regularity = d.regularity
  LEFT JOIN experiment_2_4_4_usersNUM e
  ON a.regularity = e.regularity
);
-- check if the number of different days are preserved
SELECT * FROM experiment_2_4;

/*
Experiment 2.5: Universe is every user < x averageCalls
*/
-----------------------------------------------------
--EXPERIMENT 2.5.1:
CREATE TEMPORARY TABLE experiment_2_5_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_2_universe
      WHERE nweekdays IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment2_5_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_2_universe
                                              WHERE nweekdays IS NOT NULL
                                             );

-----------------------------------------------------
--Results:
CREATE TABLE experiment_2_5 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWeekdays
  FROM experiment_2_usersDEN a
  LEFT JOIN experiment_2_5_1_usersNUM b
  ON a.regularity = b.regularity
);
-- check if the number of different days are preserved
SELECT * FROM experiment_2_5;

-- ----------------------------------------------------------------------------------------- EXPERIMENT 1: AVERAGE CALLS PER DAY -------------------------------------------------------------------- --
/*
Experiment 1.1: Universe is every user < x ActiveDays
*/

CREATE TEMPORARY TABLE experiment_1_universe AS (
  SELECT ROUND(CAST("Average Calls Per Day" AS NUMERIC),1) AS averageCalls, user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" AS M_H, "Number of Calls Made/Received in The Workplace During the Morning" AS M_W, "Number of Calls Made/Received at Home During the Evening" AS E_H, "Number of Calls Made/Received in The Workplace During the Evening" AS E_W, "Number of Calls Made/Received During the Weekdays" AS nweekdays
  FROM users_characterization_final
  GROUP BY "Average Calls Per Day", user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" , "Number of Calls Made/Received in The Workplace During the Morning", "Number of Calls Made/Received at Home During the Evening", "Number of Calls Made/Received in The Workplace During the Evening","Number of Calls Made/Received During the Weekdays"
  ORDER BY "Average Calls Per Day"
);

CREATE TEMPORARY TABLE experiment_1_usersDEN AS (
  SELECT averageCalls, sum(pre_usersDEN) OVER (ORDER BY averageCalls ASC) AS cumulative_usersDEN  -- cumulative sum of users
  FROM (
    SELECT averageCalls, count(DISTINCT user_id) AS pre_usersDEN
    FROM experiment_1_universe
    GROUP BY averageCalls
  ) t
  GROUP BY averageCalls, pre_usersDEN
);

-----------------------------------------------------
--EXPERIMENT 1.1.1:
CREATE TEMPORARY TABLE experiment_1_1_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
         SELECT *
         FROM experiment_1_universe
         WHERE home_id IS NOT NULL
    ) t
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = t.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_1_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE home_id IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 1.1.2:
CREATE TEMPORARY TABLE experiment_1_1_2_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_1_universe
      WHERE workplace_id IS NOT NULL
    ) y
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = y.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_1_2 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE workplace_id IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 1.1.3:
CREATE TEMPORARY TABLE experiment_1_1_3_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_1_universe
      WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL
    ) i
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = i.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_1_3 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL
                                             );
-----------------------------------------------------
--Results experiment_1_1:

CREATE TABLE experiment_1_1 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace
  FROM experiment_1_usersDEN a
  LEFT JOIN experiment_1_1_1_usersNUM b
  ON a.averageCalls = b.averageCalls
  LEFT JOIN experiment_1_1_2_usersNUM c
  ON a.averageCalls = c.averageCalls
  LEFT JOIN experiment_1_1_3_usersNUM d
  ON a.averageCalls = d.averageCalls
);

/*
Experiment 1.2: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 1.2.1:
CREATE TEMPORARY TABLE experiment_1_2_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_1_universe
      WHERE M_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_2_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE M_H IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 1.2.2:
CREATE TEMPORARY TABLE experiment_1_2_2_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_1_universe
      WHERE M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls

    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_2_2 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE M_W IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 1.2.3:
CREATE TEMPORARY TABLE experiment_1_2_3_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_1_universe
      WHERE M_H IS NOT NULL AND M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_2_3 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE M_H IS NOT NULL AND M_W IS NOT NULL
                                             );
-----------------------------------------------------
--Results:
CREATE TABLE experiment_1_2 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Morning,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Morning,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Morning
  FROM experiment_1_usersDEN a
  LEFT JOIN experiment_1_2_1_usersNUM b
  ON a.averageCalls = b.averageCalls
  LEFT JOIN experiment_1_2_2_usersNUM c
  ON a.averageCalls = c.averageCalls
  LEFT JOIN experiment_1_2_3_usersNUM d
  ON a.averageCalls = d.averageCalls
);
-- check if the number of different days are preserved
SELECT * FROM experiment_1_2;



/*
Experiment 1.3: Universe is every user < x ActiveDays
*/

-----------------------------------------------------
--EXPERIMENT 1.3.1:
CREATE TEMPORARY TABLE experiment_1_3_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_1_universe
      WHERE E_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_3_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE E_H IS NOT NULL
                                             );

-----------------------------------------------------
--EXPERIMENT 1.3.2:
CREATE TEMPORARY TABLE experiment_1_3_2_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_1_universe
      WHERE E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_3_2 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE E_W IS NOT NULL
                                             );
-----------------------------------------------------
--EXPERIMENT 1.3.3:
CREATE TEMPORARY TABLE experiment_1_3_3_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_1_universe
      WHERE E_H IS NOT NULL AND E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_3_3 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE E_H IS NOT NULL AND E_W IS NOT NULL
                                             );
-----------------------------------------------------
--Results:
CREATE TABLE experiment_1_3 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Evening,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Evening,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Evening
  FROM experiment_1_usersDEN a
  LEFT JOIN experiment_1_3_1_usersNUM b
  ON a.averageCalls = b.averageCalls
  LEFT JOIN experiment_1_3_2_usersNUM c
  ON a.averageCalls = c.averageCalls
  LEFT JOIN experiment_1_3_3_usersNUM d
  ON a.averageCalls = d.averageCalls
);
-- check if the number of different days are preserved
SELECT * FROM experiment_1_3;

/*
Experiment 1.4: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 1.4.1:
CREATE TEMPORARY TABLE experiment_1_4_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.averageCalls, f.user_id
      FROM experiment_1_universe f
      INNER JOIN intermediateTowers_H_W_u g
          ON f.user_id = g.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = u.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);
SELECT * FROM experiment_1_4_1_usersNUM;

UPDATE experiment_stats
SET users_characterization_experiment1_4_1 = (SELECT cumulative_usersNUM
                                              FROM experiment_1_4_1_usersNUM
                                              ORDER BY averageCalls DESC
                                              LIMIT 1
                                             );

-----------------------------------------------------
--EXPERIMENT 1.4.2:

CREATE TEMPORARY TABLE experiment_1_4_2_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.averageCalls, f.user_id
      FROM experiment_1_universe f
      INNER JOIN intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
    ) u
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = u.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_4_2 = (SELECT cumulative_usersNUM
                                              FROM experiment_1_4_2_usersNUM
                                              ORDER BY averageCalls DESC
                                              LIMIT 1
                                             );

-----------------------------------------------------
--EXPERIMENT 1.4.3:
CREATE TEMPORARY TABLE experiment_1_4_3_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.averageCalls, f.user_id
      FROM experiment_1_universe f
      INNER JOIN aux_experiment4_2 g
          ON f.user_id = g.id
    ) u
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = u.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_4_3 = (SELECT cumulative_usersNUM
                                              FROM experiment_1_4_3_usersNUM
                                              ORDER BY averageCalls DESC
                                              LIMIT 1
                                             );

-----------------------------------------------------
--EXPERIMENT 1.4.4:
CREATE TEMPORARY TABLE experiment_1_4_4_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.averageCalls, f.user_id
      FROM experiment_1_universe f
      INNER JOIN intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
      INNER JOIN intermediateTowers_H_W_u t
          ON f.user_id = t.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = u.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);


UPDATE experiment_stats
SET users_characterization_experiment1_4_4 = (SELECT cumulative_usersNUM
                                              FROM experiment_1_4_4_usersNUM
                                              ORDER BY averageCalls DESC
                                              LIMIT 1
                                             );

-----------------------------------------------------
-- Results:
CREATE TABLE experiment_1_4 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioW_H,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_or_W_H,
         coalesce(CAST(e.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_and_W_H
  FROM experiment_1_usersDEN a
  LEFT JOIN experiment_1_4_1_usersNUM b
  ON a.averageCalls = b.averageCalls
  LEFT JOIN experiment_1_4_2_usersNUM c
  ON a.averageCalls = c.averageCalls
  LEFT JOIN experiment_1_4_3_usersNUM d
  ON a.averageCalls = d.averageCalls
  LEFT JOIN experiment_1_4_4_usersNUM e
  ON a.averageCalls = e.averageCalls
);
-- check if the number of different days are preserved
SELECT * FROM experiment_1_4;

/*
Experiment 1.5: Universe is every user < x averageCalls
*/
-----------------------------------------------------
--EXPERIMENT 1.5.1:
CREATE TEMPORARY TABLE experiment_1_5_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM experiment_1_universe
      WHERE nweekdays IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

UPDATE experiment_stats
SET users_characterization_experiment1_5_1 = (SELECT count(DISTINCT user_id)
                                              FROM experiment_1_universe
                                              WHERE nweekdays IS NOT NULL
                                             );

-----------------------------------------------------
--Results:
CREATE TABLE experiment_1_5 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWeekdays
  FROM experiment_1_usersDEN a
  LEFT JOIN experiment_1_5_1_usersNUM b
  ON a.averageCalls = b.averageCalls
);
-- check if the number of different days are preserved
SELECT * FROM experiment_2_2;
SELECT * FROM experiment_2_3;

SELECT * FROM statsmunicipals
