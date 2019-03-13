------------------------------------------- CREATING THE NECESSARY TABLES AND COLUMNS FOR POSTERIOR STATISTICAL ANALYSIS ----------------------------------------------------------
CREATE TABLE ODPorto_stats (
  number_users INTEGER,
  number_records INTEGER,
  number_activities INTEGER
);
INSERT INTO ODPorto_stats DEFAULT VALUES;

CREATE TABLE stats_number_users_subsample (
  total_users INTEGER,
  users_activity_weekdays INTEGER,
  users_activity_home_hours INTEGER,
  users_activity_working_hours INTEGER,
  users_with_home INTEGER,
  users_with_work INTEGER,
  users_with_home_or_work INTEGER,
  users_with_home_and_work INTEGER,
  users_with_home_and_work_not_same INTEGER,
  users_morning_calls INTEGER,
  users_evening_calls INTEGER,
  users_calls_morning_home INTEGER,
  users_calls_morning_work INTEGER,
  users_home_or_work_morning INTEGER,
  users_home_and_work_morning INTEGER,
  users_home_or_work_morning_not_same INTEGER,
  users_home_and_work_morning_not_same INTEGER,
  users_calls_evening_home INTEGER,
  users_calls_evening_work INTEGER,
  users_home_or_work_evening INTEGER,
  users_home_and_work_evening INTEGER,
  users_home_or_work_evening_not_same INTEGER,
  users_home_and_work_evening_not_same INTEGER,
  users_home_and_work_morning_or_evening_not_same INTEGER,
  users_home_and_work_morning_and_evening_not_same INTEGER,
  users_feasible_travelTimes_morning INTEGER,
  users_feasible_travelTimes_evening INTEGER,
  users_feasible_travelTimes_morning_or_evening INTEGER,
  users_feasible_travelTimes_morning_and_evening INTEGER,
  users_feasible_travelTimes_morning_or_evening_inside_Porto INTEGER,
  users_feasible_travelTimes_morning_and_evening_inside_Porto INTEGER
);
INSERT INTO stats_number_users_subsample DEFAULT VALUES;

CREATE TABLE stats_number_users_region (
  total_users INTEGER,
  users_activity_weekdays INTEGER,
  users_activity_home_hours INTEGER,
  users_activity_working_hours INTEGER,
  users_with_home INTEGER,
  users_with_work INTEGER,
  users_with_home_or_work INTEGER,
  users_with_home_and_work INTEGER,
  users_with_home_and_work_not_same INTEGER,
  users_morning_calls INTEGER,
  users_evening_calls INTEGER,
  users_calls_morning_home INTEGER,
  users_calls_morning_work INTEGER,
  users_home_or_work_morning INTEGER,
  users_home_and_work_morning INTEGER,
  users_home_or_work_morning_not_same INTEGER,
  users_home_and_work_morning_not_same INTEGER,
  users_calls_evening_home INTEGER,
  users_calls_evening_work INTEGER,
  users_home_or_work_evening INTEGER,
  users_home_and_work_evening INTEGER,
  users_home_or_work_evening_not_same INTEGER,
  users_home_and_work_evening_not_same INTEGER,
  users_home_and_work_morning_or_evening_not_same INTEGER,
  users_home_and_work_morning_and_evening_not_same INTEGER,
  users_feasible_travelTimes_morning INTEGER,
  users_feasible_travelTimes_evening INTEGER,
  users_feasible_travelTimes_morning_or_evening INTEGER,
  users_feasible_travelTimes_morning_and_evening INTEGER,
  users_feasible_travelTimes_morning_or_evening_inside_Porto INTEGER,
  users_feasible_travelTimes_morning_and_evening_inside_Porto INTEGER
);
INSERT INTO stats_number_users_region DEFAULT VALUES;

CREATE TABLE stats_number_records_subsample (
  total_records INTEGER,
  records_users_activity_weekdays INTEGER,
  records_users_with_home_and_work INTEGER,
  records_users_with_home_and_work_not_same INTEGER,
  records_users_morning_calls INTEGER,
  records_users_evening_calls INTEGER,
  records_users_feasible_travelTimes_morning INTEGER,
  records_users_feasible_travelTimes_evening INTEGER,
  records_users_feasible_travelTimes_morning_or_evening INTEGER,
  records_users_feasible_travelTimes_morning_and_evening INTEGER,
  records_users_feasible_travelTimes_morning_or_evening_inside_Porto INTEGER,
  records_users_feasible_travelTimes_morning_and_evening_inside_Porto INTEGER
);
INSERT INTO stats_number_records_subsample DEFAULT VALUES;

CREATE TABLE stats_number_records_region (
  total_records INTEGER,
  records_users_activity_weekdays INTEGER,
  records_users_with_home_and_work INTEGER,
  records_users_with_home_and_work_not_same INTEGER,
  records_users_morning_calls INTEGER,
  records_users_evening_calls INTEGER,
  records_users_feasible_travelTimes_morning INTEGER,
  records_users_feasible_travelTimes_evening INTEGER,
  records_users_feasible_travelTimes_morning_or_evening INTEGER,
  records_users_feasible_travelTimes_morning_and_evening INTEGER,
  records_users_feasible_travelTimes_morning_or_evening_inside_Porto INTEGER,
  records_users_feasible_travelTimes_morning_and_evening_inside_Porto INTEGER
);
INSERT INTO stats_number_records_region DEFAULT VALUES;

CREATE TABLE stats_number_records_preprocess (
  records_raw_data INTEGER, -- issue
  records_without_negative_or_null_values INTEGER, -- issue
  records_without_duplicates INTEGER,
  records_without_unknownCells INTEGER,
  records_without_duplicates_and_unknownCells INTEGER,
  records_without_case1 INTEGER,
  records_without_case1_and_case2 INTEGER,
  records_oscillations INTEGER,
  records_without_different_duration INTEGER
);
INSERT INTO stats_number_records_preprocess DEFAULT VALUES;


CREATE TABLE stats_number_users_preprocess (
  users_raw_data INTEGER, -- issue
  users_without_negative_or_null_values INTEGER, -- issue
  users_without_duplicates INTEGER,
  users_without_unknownCells INTEGER,
  users_without_duplicates_and_unknownCells INTEGER,
  users_without_case1 INTEGER,
  users_without_case1_and_case2 INTEGER,
  users_without_different_duration INTEGER
);
INSERT INTO stats_number_users_preprocess DEFAULT VALUES;







-- -------------------------------------------------------------------------------------------------- ELABORATING STATS ----------------------------------------------------------------------------------------- --

---------------------------------- STATS OF THE PORTO USERS THAT WILL BE USED TO CALCULATE ROUTES AND MEANS OF TRANSPORT ---------------------------
UPDATE ODPorto_stats
SET number_users = (SELECT count(*) FROM ODPorto_users_characterization);

UPDATE ODPorto_stats
SET number_records = (SELECT count(*) FROM subsample_ODPORTO);

UPDATE ODPorto_stats
SET number_activities = (SELECT count(*) FROM ODPORTO);


---------------------------------------------------- STATS OF THE SUBSAMPLE OF USERS SELECTED ACCORDINGLY TO THE CHANGE OF VARIABLES ---------------------------
UPDATE stats_number_users_subsample
SET total_users = (SELECT count(*) FROM subsample_users_characterization);

CREATE TEMPORARY TABLE temp_users_activity_weekdays AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE "Number of Calls Made/Received During the Weekdays" IS NOT NULL
);

UPDATE stats_number_users_subsample
SET users_activity_weekdays = (SELECT count(*) FROM temp_users_activity_weekdays);

UPDATE stats_number_users_subsample
SET users_activity_home_hours = (SELECT count(*)
                                 FROM subsample_users_characterization
                                 WHERE "Number of Calls Made/Received During the Non-Working Hours" IS NOT NULL
                                );

UPDATE stats_number_users_subsample
SET users_activity_working_hours = (SELECT count(*)
                                    FROM subsample_users_characterization
                                    WHERE "Number of Calls Made/Received During the Working Hours" IS NOT NULL
                                   );

UPDATE stats_number_users_subsample
SET users_with_home = (SELECT count(*)
                       FROM subsample_users_characterization
                       WHERE home_id IS NOT NULL
                       );

UPDATE stats_number_users_subsample
SET users_with_work = (SELECT count(*)
                       FROM subsample_users_characterization
                       WHERE workplace_id IS NOT NULL
                       );

UPDATE stats_number_users_subsample
SET users_with_home_or_work = (SELECT count(*)
                               FROM subsample_users_characterization
                               WHERE workplace_id IS NOT NULL
                               OR home_id IS NOT NULL
                               );

CREATE TEMPORARY TABLE temp_users_with_home_and_work AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE workplace_id IS NOT NULL
  AND home_id IS NOT NULL
);

UPDATE stats_number_users_subsample
SET users_with_home_and_work = (SELECT count(*) FROM temp_users_with_home_and_work);


CREATE TEMPORARY TABLE temp_users_with_home_and_work_not_same AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE workplace_id IS NOT NULL
  AND home_id IS NOT NULL
  AND home_id != workplace_id
);

UPDATE stats_number_users_subsample
SET users_with_home_and_work_not_same = (SELECT count(*) FROM temp_users_with_home_and_work_not_same);

CREATE TEMPORARY TABLE temp_users_morning_calls AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE "Number of Calls Made/Received During the Morning" IS NOT NULL
);

UPDATE stats_number_users_subsample
SET users_morning_calls = (SELECT count(*) FROM temp_users_morning_calls);

CREATE TEMPORARY TABLE temp_users_evening_calls AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE "Number of Calls Made/Received During the Evening" IS NOT NULL
);

UPDATE stats_number_users_subsample
SET users_evening_calls = (SELECT count(*) FROM temp_users_evening_calls);


UPDATE stats_number_users_subsample
SET users_calls_morning_home = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                );

UPDATE stats_number_users_subsample
SET users_calls_morning_work = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                );

UPDATE stats_number_users_subsample
SET users_home_or_work_morning = (SELECT count(*)
                                  FROM subsample_users_characterization
                                  WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                     OR "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                  );

UPDATE stats_number_users_subsample
SET users_home_and_work_morning = (SELECT count(*)
                                   FROM subsample_users_characterization
                                   WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                     AND "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                  );

UPDATE stats_number_users_subsample
SET users_home_or_work_morning_not_same = (SELECT count(*)
                                           FROM subsample_users_characterization
                                           WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                              OR "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                             AND home_id != workplace_id
                                          );

UPDATE stats_number_users_subsample
SET users_home_and_work_morning_not_same = (SELECT count(*)
                                            FROM subsample_users_characterization
                                            WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                              AND "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                              AND home_id != workplace_id
                                            );

UPDATE stats_number_users_subsample
SET users_calls_evening_home = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                );

UPDATE stats_number_users_subsample
SET users_calls_evening_work = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                );

UPDATE stats_number_users_subsample
SET users_home_or_work_evening = (SELECT count(*)
                                  FROM subsample_users_characterization
                                  WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                     OR "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                  );

UPDATE stats_number_users_subsample
SET users_home_and_work_evening = (SELECT count(*)
                                   FROM subsample_users_characterization
                                   WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                     AND "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                    );

UPDATE stats_number_users_subsample
SET users_home_or_work_evening_not_same = (SELECT count(*)
                                           FROM subsample_users_characterization
                                           WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                              OR "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                             AND home_id != workplace_id
                                    );

UPDATE stats_number_users_subsample
SET users_home_and_work_evening_not_same = (SELECT count(*)
                                            FROM subsample_users_characterization
                                            WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                              AND "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                              AND home_id != workplace_id
                                    );

UPDATE stats_number_users_subsample
SET users_home_and_work_morning_or_evening_not_same = (SELECT count(*)
                                                         FROM subsample_users_characterization
                                                        WHERE ("Number of Calls Made/Received at Home During the Morning" IS NOT NULL AND "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL)
                                                           OR ("Number of Calls Made/Received at Home During the Evening" IS NOT NULL AND "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL)
                                                          AND home_id != workplace_id
                                                        );

UPDATE stats_number_users_subsample
SET users_home_and_work_morning_and_evening_not_same = (SELECT count(*)
                                                         FROM subsample_users_characterization
                                                         WHERE ("Number of Calls Made/Received at Home During the Morning" IS NOT NULL AND "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL)
                                                           AND ("Number of Calls Made/Received at Home During the Evening" IS NOT NULL AND "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL)
                                                           AND home_id != workplace_id
                                                      );

CREATE TEMPORARY TABLE temp_users_feasible_travelTimes_morning AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE minTravelTime_H_W IS NOT NULL
  AND "Travel Speed H_W (Km/h)" <= 250
  AND "Travel Speed H_W (Km/h)" >= 3
);

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning = (SELECT count(*) FROM temp_users_feasible_travelTimes_morning);


CREATE TEMPORARY TABLE temp_users_feasible_travelTimes_evening AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE minTravelTime_W_H IS NOT NULL
    AND "Travel Speed W_H (Km/h)" <= 250
    AND "Travel Speed W_H (Km/h)" >= 3
);

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_evening = (SELECT count(*) FROM temp_users_feasible_travelTimes_evening);

CREATE TEMPORARY TABLE temp_users_feasible_travelTimes_morning_or_evening AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
     OR (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
);

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning_or_evening = (SELECT count(*) FROM temp_users_feasible_travelTimes_morning_or_evening);

CREATE TEMPORARY TABLE temp_users_feasible_travelTimes_morning_and_evening AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
    AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
);

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning_and_evening = (SELECT count(*) FROM temp_users_feasible_travelTimes_morning_and_evening);

CREATE TEMPORARY TABLE temp_users_feasible_travelTimes_morning_or_evening_inside_Porto AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE home_id IN (SELECT cell_id FROM call_dim_porto)
    AND workplace_id IN (SELECT cell_id FROM call_dim_porto)
    AND ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
     OR (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
);

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning_or_evening_inside_Porto = (SELECT count(*) FROM temp_users_feasible_travelTimes_morning_or_evening_inside_Porto);

CREATE TEMPORARY TABLE temp_users_feasible_travelTimes_morning_and_evening_inside_Porto AS (
  SELECT *
  FROM subsample_users_characterization
  WHERE home_id IN (SELECT cell_id FROM call_dim_porto)
    AND workplace_id IN (SELECT cell_id FROM call_dim_porto)
    AND ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
    AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
);

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning_and_evening_inside_Porto = (SELECT count(*) FROM temp_users_feasible_travelTimes_morning_and_evening_inside_Porto);


---------------------------------------------------- STATS OF THE SUBSAMPLE OF RECORDS SELECTED ACCORDINGLY TO THE CHANGE OF VARIABLES ---------------------------
UPDATE stats_number_records_subsample
SET total_records = (SELECT count(*) FROM call_fct_porto);

UPDATE stats_number_records_subsample
SET records_users_activity_weekdays = (SELECT count(*)
                                       FROM(
                                        SELECT *
                                        FROM call_fct_porto_weekdays
                                        WHERE originating_id IN (SELECT * FROM temp_users_activity_weekdays)

                                        UNION

                                        SELECT *
                                        FROM call_fct_porto_weekdays
                                        WHERE terminating_id IN (SELECT * FROM temp_users_activity_weekdays)
                                       ) b
                                      );

UPDATE stats_number_records_subsample
SET records_users_with_home_and_work = (SELECT count(*)
                                       FROM(
                                        SELECT *
                                        FROM call_fct_porto_weekdays
                                        WHERE originating_id IN (SELECT * FROM temp_users_with_home_and_work)
                                        UNION

                                        SELECT *
                                        FROM call_fct_porto_weekdays
                                        WHERE terminating_id IN (SELECT * FROM temp_users_with_home_and_work)
                                       ) b
                                      );

UPDATE stats_number_records_subsample
SET records_users_with_home_and_work_not_same = (SELECT count(*)
                                                 FROM(
                                                  SELECT *
                                                  FROM call_fct_porto_weekdays
                                                  WHERE originating_id IN (SELECT * FROM temp_users_with_home_and_work_not_same)

                                                  UNION

                                                  SELECT *
                                                  FROM call_fct_porto_weekdays
                                                  WHERE terminating_id IN (SELECT * FROM temp_users_with_home_and_work_not_same)
                                                 ) b
                                                );

UPDATE stats_number_records_subsample
SET records_users_morning_calls = (SELECT count(*)
                                                 FROM(
                                                  SELECT *
                                                  FROM call_fct_porto_weekdays
                                                  WHERE originating_id IN (SELECT * FROM temp_users_morning_calls)
                                                  UNION

                                                  SELECT *
                                                  FROM call_fct_porto_weekdays
                                                  WHERE terminating_id IN (SELECT * FROM temp_users_morning_calls)
                                                 ) b
                                                );

UPDATE stats_number_records_subsample
SET records_users_evening_calls = (SELECT count(*)
                                   FROM (
                                    SELECT *
                                    FROM call_fct_porto_weekdays
                                    WHERE originating_id IN (SELECT * FROM temp_users_evening_calls)
                                    UNION

                                    SELECT *
                                    FROM call_fct_porto_weekdays
                                    WHERE terminating_id IN (SELECT * FROM temp_users_evening_calls)
                                   ) b
                                  );

UPDATE stats_number_records_subsample
SET records_users_feasible_travelTimes_morning = (SELECT count(*)
                                                   FROM (
                                                    SELECT *
                                                    FROM call_fct_porto_weekdays
                                                    WHERE originating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning)
                                                    UNION

                                                    SELECT *
                                                    FROM call_fct_porto_weekdays
                                                    WHERE terminating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning)
                                                   ) b
                                                  );

UPDATE stats_number_records_subsample
SET records_users_feasible_travelTimes_evening = (SELECT count(*)
                                                   FROM (
                                                    SELECT *
                                                    FROM call_fct_porto_weekdays
                                                    WHERE originating_id IN (SELECT * FROM temp_users_feasible_travelTimes_evening)

                                                    UNION

                                                    SELECT *
                                                    FROM call_fct_porto_weekdays
                                                    WHERE terminating_id IN (SELECT * FROM temp_users_feasible_travelTimes_evening)
                                                   ) b
                                                  );

UPDATE stats_number_records_subsample
SET records_users_feasible_travelTimes_morning_or_evening = (SELECT count(*)
                                                             FROM (
                                                              SELECT *
                                                              FROM call_fct_porto_weekdays
                                                              WHERE originating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning_or_evening)

                                                              UNION

                                                              SELECT *
                                                              FROM call_fct_porto_weekdays
                                                              WHERE terminating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning_or_evening)
                                                             ) b
                                                            );
UPDATE stats_number_records_subsample
SET records_users_feasible_travelTimes_morning_and_evening = (SELECT count(*)
                                                             FROM (
                                                              SELECT *
                                                              FROM call_fct_porto_weekdays
                                                              WHERE originating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning_and_evening)
                                                              UNION

                                                              SELECT *
                                                              FROM call_fct_porto_weekdays
                                                              WHERE terminating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning_and_evening)
                                                             ) b
                                                            );

UPDATE stats_number_records_subsample
SET records_users_feasible_travelTimes_morning_or_evening_inside_Porto = (SELECT count(*)
                                                                   FROM (
                                                                    SELECT *
                                                                    FROM call_fct_porto_weekdays
                                                                    WHERE originating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning_or_evening_inside_Porto)
                                                                    UNION

                                                                    SELECT *
                                                                    FROM call_fct_porto_weekdays
                                                                    WHERE terminating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning_or_evening_inside_Porto)
                                                                   ) b
                                                                  );


UPDATE stats_number_records_subsample
SET records_users_feasible_travelTimes_morning_and_evening_inside_Porto = (SELECT count(*)
                                                                         FROM (
                                                                          SELECT *
                                                                          FROM call_fct_porto_weekdays
                                                                          WHERE originating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning_and_evening_inside_Porto)
                                                                          UNION

                                                                          SELECT *
                                                                          FROM call_fct_porto_weekdays
                                                                          WHERE terminating_id IN (SELECT * FROM temp_users_feasible_travelTimes_morning_and_evening_inside_Porto)
                                                                   ) b
                                                                  );

---------------------------------------------------- STATS OF THE USERS THAT MADE/RECEIVED CALLS INSIDE THE REGION --------------------------------
UPDATE stats_number_users_region
SET total_users = (SELECT count(*) FROM region_users_characterization);

CREATE TEMPORARY TABLE temp_region_users_activity_weekdays  AS (
  SELECT *
  FROM region_users_characterization
  WHERE "Number of Calls Made/Received During the Weekdays" IS NOT NULL
);

UPDATE stats_number_users_region
SET users_activity_weekdays = (SELECT count(*) FROM temp_region_users_activity_weekdays);

UPDATE stats_number_users_region
SET users_activity_home_hours = (SELECT count(*)
                                 FROM subsample_users_characterization
                                 WHERE "Number of Calls Made/Received During the Non-Working Hours" IS NOT NULL
                                );

UPDATE stats_number_users_region
SET users_activity_working_hours = (SELECT count(*)
                                    FROM subsample_users_characterization
                                    WHERE "Number of Calls Made/Received During the Working Hours" IS NOT NULL
                                   );

UPDATE stats_number_users_region
SET users_with_home = (SELECT count(*)
                       FROM subsample_users_characterization
                       WHERE home_id IS NOT NULL
                       );

UPDATE stats_number_users_region
SET users_with_work = (SELECT count(*)
                       FROM subsample_users_characterization
                       WHERE workplace_id IS NOT NULL
                       );

UPDATE stats_number_users_region
SET users_with_home_or_work = (SELECT count(*)
                               FROM subsample_users_characterization
                               WHERE workplace_id IS NOT NULL
                               OR home_id IS NOT NULL
                               );

CREATE TEMPORARY TABLE temp_region_users_with_home_and_work  AS (
  SELECT *
  FROM region_users_characterization
  WHERE workplace_id IS NOT NULL
  AND home_id IS NOT NULL
);

UPDATE stats_number_users_region
SET users_with_home_and_work = (SELECT count(*) FROM temp_region_users_with_home_and_work);

CREATE TEMPORARY TABLE temp_region_users_with_home_and_work_not_same AS (
  SELECT *
  FROM region_users_characterization
  WHERE workplace_id IS NOT NULL
  AND home_id IS NOT NULL
  AND home_id != workplace_id
);

UPDATE stats_number_users_region
SET users_with_home_and_work_not_same = (SELECT count(*) FROM temp_region_users_with_home_and_work_not_same);

CREATE TEMPORARY TABLE temp_region_users_morning_calls AS (
  SELECT *
  FROM region_users_characterization
  WHERE "Number of Calls Made/Received During the Morning" IS NOT NULL
);

UPDATE stats_number_users_region
SET users_morning_calls = (SELECT count(*) FROM temp_region_users_morning_calls);

CREATE TEMPORARY TABLE temp_region_users_evening_calls AS (
  SELECT *
  FROM region_users_characterization
  WHERE "Number of Calls Made/Received During the Evening" IS NOT NULL
);

UPDATE stats_number_users_region
SET users_evening_calls = (SELECT count(*) FROM temp_region_users_evening_calls);

UPDATE stats_number_users_region
SET users_calls_morning_home = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                );

UPDATE stats_number_users_region
SET users_calls_morning_work = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                );

UPDATE stats_number_users_region
SET users_home_or_work_morning = (SELECT count(*)
                                  FROM subsample_users_characterization
                                  WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                     OR "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                  );

UPDATE stats_number_users_region
SET users_home_and_work_morning = (SELECT count(*)
                                   FROM subsample_users_characterization
                                   WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                     AND "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                  );

UPDATE stats_number_users_region
SET users_home_or_work_morning_not_same = (SELECT count(*)
                                           FROM subsample_users_characterization
                                           WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                              OR "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                             AND home_id != workplace_id
                                          );

UPDATE stats_number_users_region
SET users_home_and_work_morning_not_same = (SELECT count(*)
                                            FROM subsample_users_characterization
                                            WHERE "Number of Calls Made/Received at Home During the Morning" IS NOT NULL
                                              AND "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL
                                              AND home_id != workplace_id
                                            );

UPDATE stats_number_users_region
SET users_calls_evening_home = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                );

UPDATE stats_number_users_region
SET users_calls_evening_work = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                );

UPDATE stats_number_users_region
SET users_home_or_work_evening = (SELECT count(*)
                                  FROM subsample_users_characterization
                                  WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                     OR "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                  );

UPDATE stats_number_users_region
SET users_home_and_work_evening = (SELECT count(*)
                                   FROM subsample_users_characterization
                                   WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                     AND "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                    );

UPDATE stats_number_users_region
SET users_home_or_work_evening_not_same = (SELECT count(*)
                                           FROM subsample_users_characterization
                                           WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                              OR "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                             AND home_id != workplace_id
                                    );

UPDATE stats_number_users_region
SET users_home_and_work_evening_not_same = (SELECT count(*)
                                            FROM subsample_users_characterization
                                            WHERE "Number of Calls Made/Received at Home During the Evening" IS NOT NULL
                                              AND "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL
                                              AND home_id != workplace_id
                                    );

UPDATE stats_number_users_region
SET users_home_and_work_morning_or_evening_not_same = (SELECT count(*)
                                                         FROM subsample_users_characterization
                                                        WHERE ("Number of Calls Made/Received at Home During the Morning" IS NOT NULL AND "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL)
                                                           OR ("Number of Calls Made/Received at Home During the Evening" IS NOT NULL AND "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL)
                                                          AND home_id != workplace_id
                                                        );

UPDATE stats_number_users_region
SET users_home_and_work_morning_and_evening_not_same = (SELECT count(*)
                                                         FROM subsample_users_characterization
                                                         WHERE ("Number of Calls Made/Received at Home During the Morning" IS NOT NULL AND "Number of Calls Made/Received in The Workplace During the Morning" IS NOT NULL)
                                                           AND ("Number of Calls Made/Received at Home During the Evening" IS NOT NULL AND "Number of Calls Made/Received in The Workplace During the Evening" IS NOT NULL)
                                                           AND home_id != workplace_id
                                    );

CREATE TEMPORARY TABLE temp_region_users_feasible_travelTimes_morning AS (
  SELECT *
  FROM region_users_characterization
  WHERE minTravelTime_H_W IS NOT NULL
    AND "Travel Speed H_W (Km/h)" <= 250
    AND "Travel Speed H_W (Km/h)" >= 3
);

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning = (SELECT count(*) FROM temp_region_users_feasible_travelTimes_morning);

CREATE TEMPORARY TABLE temp_region_users_feasible_travelTimes_evening AS (
  SELECT *
  FROM region_users_characterization
  WHERE minTravelTime_W_H IS NOT NULL
    AND "Travel Speed W_H (Km/h)" <= 250
    AND "Travel Speed W_H (Km/h)" >= 3
);

UPDATE stats_number_users_region
SET users_feasible_travelTimes_evening = (SELECT count(*) FROM temp_region_users_feasible_travelTimes_evening);

CREATE TEMPORARY TABLE temp_region_users_feasible_travelTimes_morning_or_evening AS (
  SELECT *
  FROM region_users_characterization
  WHERE ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
     OR (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
);

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning_or_evening = (SELECT count(*) FROM temp_region_users_feasible_travelTimes_morning_or_evening);

CREATE TEMPORARY TABLE temp_region_users_feasible_travelTimes_morning_and_evening AS (
  SELECT *
  FROM region_users_characterization
  WHERE ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
    AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
);

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning_and_evening = (SELECT count(*) FROM temp_region_users_feasible_travelTimes_morning_and_evening);

CREATE TEMPORARY TABLE temp_region_users_feasible_travelTimes_morning_or_evening_inside_Porto AS (
  SELECT *
  FROM region_users_characterization
  WHERE home_id IN (SELECT cell_id FROM call_dim_porto)
    AND workplace_id IN (SELECT cell_id FROM call_dim_porto)
    AND ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
    OR (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
);

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning_or_evening_inside_Porto = (SELECT count(*) FROM temp_region_users_feasible_travelTimes_morning_or_evening_inside_Porto);

CREATE TEMPORARY TABLE temp_region_users_feasible_travelTimes_morning_and_evening_inside_Porto AS (
  SELECT *
  FROM region_users_characterization
  WHERE home_id IN (SELECT cell_id FROM call_dim_porto)
    AND workplace_id IN (SELECT cell_id FROM call_dim_porto)
    AND ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
    AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
);

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning_and_evening_inside_Porto = (SELECT count(*) FROM temp_region_users_feasible_travelTimes_morning_and_evening_inside_Porto;

---------------------------------------------------- STATS OF THE RECORDS OF THE USERS THAT MADE/RECEIVED CALLS INSIDE THE REGION --------------------------------
UPDATE stats_number_records_region
SET total_records = (SELECT count(*) FROM call_fct_porto);

UPDATE stats_number_records_region
SET records_users_activity_weekdays = (SELECT count(*)
                                       FROM(
                                        SELECT *
                                        FROM call_fct_porto_weekdays
                                        WHERE originating_id IN (SELECT * FROM temp_region_users_activity_weekdays)

                                        UNION

                                        SELECT *
                                        FROM call_fct_porto_weekdays
                                        WHERE terminating_id IN (SELECT * FROM temp_region_users_activity_weekdays)
                                       ) b
                                      );

UPDATE stats_number_records_region
SET records_users_with_home_and_work = (SELECT count(*)
                                       FROM(
                                        SELECT *
                                        FROM call_fct_porto_weekdays
                                        WHERE originating_id IN (SELECT * FROM temp_region_users_with_home_and_work)
                                        UNION

                                        SELECT *
                                        FROM call_fct_porto_weekdays
                                        WHERE terminating_id IN (SELECT * FROM temp_region_users_with_home_and_work)
                                       ) b
                                      );

UPDATE stats_number_records_region
SET records_users_with_home_and_work_not_same = (SELECT count(*)
                                                 FROM(
                                                  SELECT *
                                                  FROM call_fct_porto_weekdays
                                                  WHERE originating_id IN (SELECT * FROM temp_region_users_with_home_and_work_not_same)

                                                  UNION

                                                  SELECT *
                                                  FROM call_fct_porto_weekdays
                                                  WHERE terminating_id IN (SELECT * FROM temp_region_users_with_home_and_work_not_same)
                                                 ) b
                                                );

UPDATE stats_number_records_region
SET records_users_morning_calls = (SELECT count(*)
                                                 FROM(
                                                  SELECT *
                                                  FROM call_fct_porto_weekdays
                                                  WHERE originating_id IN (SELECT * FROM temp_region_users_morning_calls)
                                                  UNION

                                                  SELECT *
                                                  FROM call_fct_porto_weekdays
                                                  WHERE terminating_id IN (SELECT * FROM temp_region_users_morning_calls)
                                                 ) b
                                                );

UPDATE stats_number_records_region
SET records_users_evening_calls = (SELECT count(*)
                                   FROM (
                                    SELECT *
                                    FROM call_fct_porto_weekdays
                                    WHERE originating_id IN (SELECT * FROM temp_region_users_evening_calls)
                                    UNION

                                    SELECT *
                                    FROM call_fct_porto_weekdays
                                    WHERE terminating_id IN (SELECT * FROM temp_region_users_evening_calls)
                                   ) b
                                  );

UPDATE stats_number_records_region
SET records_users_feasible_travelTimes_morning = (SELECT count(*)
                                                   FROM (
                                                    SELECT *
                                                    FROM call_fct_porto_weekdays
                                                    WHERE originating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning)
                                                    UNION

                                                    SELECT *
                                                    FROM call_fct_porto_weekdays
                                                    WHERE terminating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning)
                                                   ) b
                                                  );

UPDATE stats_number_records_region
SET records_users_feasible_travelTimes_evening = (SELECT count(*)
                                                   FROM (
                                                    SELECT *
                                                    FROM call_fct_porto_weekdays
                                                    WHERE originating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_evening)

                                                    UNION

                                                    SELECT *
                                                    FROM call_fct_porto_weekdays
                                                    WHERE terminating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_evening)
                                                   ) b
                                                  );

UPDATE stats_number_records_region
SET records_users_feasible_travelTimes_morning_or_evening = (SELECT count(*)
                                                             FROM (
                                                              SELECT *
                                                              FROM call_fct_porto_weekdays
                                                              WHERE originating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning_or_evening)

                                                              UNION

                                                              SELECT *
                                                              FROM call_fct_porto_weekdays
                                                              WHERE terminating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning_or_evening)
                                                             ) b
                                                            );
UPDATE stats_number_records_region
SET records_users_feasible_travelTimes_morning_and_evening = (SELECT count(*)
                                                             FROM (
                                                              SELECT *
                                                              FROM call_fct_porto_weekdays
                                                              WHERE originating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning_and_evening)
                                                              UNION

                                                              SELECT *
                                                              FROM call_fct_porto_weekdays
                                                              WHERE terminating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning_and_evening)
                                                             ) b
                                                            );

UPDATE stats_number_records_region
SET records_users_feasible_travelTimes_morning_or_evening_inside_Porto = (SELECT count(*)
                                                                   FROM (
                                                                    SELECT *
                                                                    FROM call_fct_porto_weekdays
                                                                    WHERE originating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning_or_evening_inside_Porto)
                                                                    UNION

                                                                    SELECT *
                                                                    FROM call_fct_porto_weekdays
                                                                    WHERE terminating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning_or_evening_inside_Porto)
                                                                   ) b
                                                                  );


UPDATE stats_number_records_region
SET records_users_feasible_travelTimes_morning_and_evening_inside_Porto = (SELECT count(*)
                                                                         FROM (
                                                                          SELECT *
                                                                          FROM call_fct_porto_weekdays
                                                                          WHERE originating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning_and_evening_inside_Porto)
                                                                          UNION

                                                                          SELECT *
                                                                          FROM call_fct_porto_weekdays
                                                                          WHERE terminating_id IN (SELECT * FROM temp_region_users_feasible_travelTimes_morning_and_evening_inside_Porto)
                                                                   ) b
                                                                  );

-------------------------------------------------- RESULTS OF ALL OPERATIONS IN ORDER TO MAKE THE STATISTICAL ANALYSIS ----------------------------------------------------------------------------------------------
SELECT * FROM stats_number_users_preprocess;
SELECT * FROM stats_number_records_preprocess;

SELECT * FROM stats_number_users_region;
SELECT * FROM stats_number_records_region;

SELECT * FROM stats_number_users_subsample;
SELECT * FROM stats_number_records_subsample;

SELECT * FROM ODPorto_stats;

DISCARD TEMP;