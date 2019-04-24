

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
CREATE TEMPORARY TABLE new_travelTimes_H_W_u AS (
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

CREATE TEMPORARY TABLE new_travelTimes_W_H_u AS (
  SELECT DISTINCT ON(id, averageTravelTime_W_H, minTravelTime_W_H) *
  FROM travelTimes_W_H_u
);

-- --------------------------------------------------------------------------- INTERMEDIATE CELL TOWERS WITHIN TRAVEL TIME ------------------------------------------------------------ --
-- HOME -> WORK

ALTER TABLE travelTimes_H_W_u
RENAME COLUMN id TO hwid;

CREATE TEMPORARY TABLE intermediateTowers_H_W_u AS (
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
          INNER JOIN (SELECT * FROM travelTimes_H_W_u) h ON hwid = p.id

          WHERE time > startdate_H_W
          AND time < finishdate_H_W
    ) t
    INNER JOIN (SELECT cell_id AS tower, longitude, latitude, geom_point FROM call_dim) u
    ON t.cell_id = tower
  ) g
  INNER JOIN home_workplace_by_user_u f
  ON intermediateTowers_H_WID = f.eid

);

ALTER TABLE travelTimes_W_H_u
RENAME COLUMN id TO whid;

-- WORK -> HOME
CREATE TEMPORARY TABLE intermediateTowers_W_H_u AS (
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
          INNER JOIN (SELECT * FROM travelTimes_W_H_u) h ON whid = p.id

          WHERE time > startdate_W_H
          AND time < finishdate_W_H
    ) t
    INNER JOIN (SELECT cell_id AS tower, longitude, latitude, geom_point FROM call_dim) u
    ON t.cell_id = tower
  ) g
  INNER JOIN home_workplace_by_user_u f
  ON intermediateTowers_W_HID = f.eid
);


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

DROP TABLE infomunicipals_and_cells;
CREATE TEMPORARY TABLE infomunicipals_and_cells AS (
  SELECT f.name_2, averagekm2percell AS "Tower Density (Km2 per Cell)", g.cell_id
  FROM cell_idsbyregions g
  INNER JOIN infoMunicipals f
  ON g.name_2 = f.name_2
);


CREATE TEMPORARY TABLE megatable AS (
  SELECT f.*, name_2 AS municipalHome, "Tower Density (Km2 per Cell)" AS densityHome
  FROM infomunicipals_and_cells g
  RIGHT JOIN users_characterization f
  ON g.cell_id = f.home_id
);

CREATE TEMPORARY TABLE users_characterization_final AS ( -- aparently users only work or live in 275 municipals from 297 municipals that have cell towers. Remember as well that initially there are 306 municipals in Portugal, but only 297 municipals have towers
  SELECT f.*, name_2 AS municipalWorkplace, "Tower Density (Km2 per Cell)" AS densityWorkplace
  FROM infomunicipals_and_cells g
  RIGHT JOIN megatable f
  ON g.cell_id = f.workplace_id
  AND municipalhome = name_2 -- Don't forget to mention that we are dealing only with users that have home and workplace in the same municipal
);








































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

CREATE TABLE switchspeedscase4 AS (
  SELECT *,
             (CAST(distanciaOrig AS FLOAT)/1000)/(CAST(lagduration_amt AS FLOAT)/3600) AS "Switch Speed Origin - Km per hour",
             (CAST(distanciaTerm AS FLOAT)/1000)/(CAST(lagduration_amt AS FLOAT)/3600) AS "Switch Speed Terminating - Km per hour"
  FROM (SELECT *,
           st_distance(ST_Transform(ori_geom_point, 3857), ST_Transform(lagori_geom_point, 3857)) AS distanciaOrig,
           st_distance(ST_Transform(term_geom_point, 3857), ST_Transform(lagterm_geom_point, 3857)) AS distanciaTerm
        FROM case3and4) r
);