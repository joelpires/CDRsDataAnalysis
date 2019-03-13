------------------------------------------- CREATING THE NECESSARY TABLES AND COLUMNS FOR POSTERIOR STATISTICAL ANALYSIS ----------------------------------------------------------

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

CREATE TABLE stats_number_records_region (
  records_porto_users INTEGER,
  --records_by_minimum_requirements INTEGER,
  records_activity_weekdays INTEGER,
  records_activity_working_hours INTEGER,
  records_activity_home_hours INTEGER,
  records_with_home_or_work INTEGER,
  records_with_home_and_work INTEGER,
  records_morning_calls INTEGER,
  records_evening_calls INTEGER,
  records_evening_or_morning_calls INTEGER,
  records_evening_and_morning_calls INTEGER,
  records_calls_morning_home INTEGER,
  records_calls_morning_work INTEGER,
  records_home_or_work_morning INTEGER,
  records_home_and_work_morning INTEGER,
  records_calls_evening_home INTEGER,
  records_calls_evening_work INTEGER,
  records_home_or_work_evening INTEGER,
  records_home_and_work_evening INTEGER,
  records_home_and_work_morning_or_evening INTEGER,
  records_home_and_work_morning_and_evening INTEGER,
  cleaned_records_home_and_work_morning INTEGER,
  cleaned_records_home_and_work_evening INTEGER,
  cleaned_records_home_and_work_morning_and_evening INTEGER,

  records_with_home_and_work_inside_region INTEGER,
  records_with_home_work_inside_not_same INTEGER

);
INSERT INTO stats_number_records_region DEFAULT VALUES;


CREATE TABLE stats_number_records_subsample (
  id INTEGER,
  records_porto_users INTEGER,
  --records_by_minimum_requirements INTEGER,
  records_activity_weekdays INTEGER,
  records_activity_working_hours INTEGER,
  records_activity_home_hours INTEGER,
  records_with_home_or_work INTEGER,
  records_with_home_and_work INTEGER,
  records_morning_calls INTEGER,
  records_evening_calls INTEGER,
  records_evening_or_morning_calls INTEGER,
  records_evening_and_morning_calls INTEGER,
  records_calls_morning_home INTEGER,
  records_calls_morning_work INTEGER,
  records_home_or_work_morning INTEGER,
  records_home_and_work_morning INTEGER,
  records_calls_evening_home INTEGER,
  records_calls_evening_work INTEGER,
  records_home_or_work_evening INTEGER,
  records_home_and_work_evening INTEGER,
  records_home_and_work_morning_or_evening INTEGER,
  records_home_and_work_morning_and_evening INTEGER,
  cleaned_records_home_and_work_morning INTEGER,
  cleaned_records_home_and_work_evening INTEGER,
  cleaned_records_home_and_work_morning_and_evening INTEGER,

  records_with_home_and_work_inside_region INTEGER,
  records_with_home_work_inside_not_same INTEGER,
  records_subsample INTEGER
);
INSERT INTO stats_number_records_subsample DEFAULT VALUES;


-- -------------------------------------------------------------------------------------------------- ELABORATING STATS ----------------------------------------------------------------------------------------- --

---------------------------------- STATS OF THE PORTO USERS THAT WILL BE USED TO CALCULATE ROUTES AND MEANS OF TRANSPORT ---------------------------
UPDATE ODPorto_stats
SET number_users = (SELECT count(*) FROM ODPorto_users_characterization);

UPDATE ODPorto_stats
SET number_records = (SELECT count(*) FROM subsample_ODPORTO);

UPDATE ODPorto_stats
SET number_activities = (SELECT count(*) FROM ODPORTO);


---------------------------------------------------- STATS OF THE SELECTED SUBSET ACCORDINGLY TO THE CHANGE OF VARIABLES ---------------------------
UPDATE stats_number_users_subsample
SET total_users = (SELECT count(*) FROM subsample_users_characterization);

UPDATE stats_number_users_subsample
SET users_activity_weekdays = (SELECT count(*)
                               FROM subsample_users_characterization
                               WHERE "Number of Calls Made/Received During the Weekdays" IS NOT NULL
                              );

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

UPDATE stats_number_users_subsample
SET users_with_home_and_work = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE workplace_id IS NOT NULL
                                AND home_id IS NOT NULL
                               );

UPDATE stats_number_users_subsample
SET users_with_home_and_work_not_same = (SELECT count(*)
                                         FROM subsample_users_characterization
                                         WHERE workplace_id IS NOT NULL
                                         AND home_id IS NOT NULL
                                         AND home_id != workplace_id
                                        );

UPDATE stats_number_users_subsample
SET users_morning_calls = (SELECT count(*)
                           FROM subsample_users_characterization
                           WHERE "Number of Calls Made/Received During the Morning" IS NOT NULL
                          );

UPDATE stats_number_users_subsample
SET users_evening_calls = (SELECT count(*)
                           FROM subsample_users_characterization
                           WHERE "Number of Calls Made/Received During the Evening" IS NOT NULL
                          );

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

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning = (SELECT count(*)
                                           FROM subsample_users_characterization
                                          WHERE minTravelTime_H_W IS NOT NULL
                                            AND "Travel Speed H_W (Km/h)" <= 250
                                            AND "Travel Speed H_W (Km/h)" >= 3
                                          );

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_evening = (SELECT count(*)
                                           FROM subsample_users_characterization
                                          WHERE minTravelTime_W_H IS NOT NULL
                                            AND "Travel Speed W_H (Km/h)" <= 250
                                            AND "Travel Speed W_H (Km/h)" >= 3
                                    );

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning_or_evening = (SELECT count(*)
                                                       FROM subsample_users_characterization
                                                      WHERE ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
                                                         OR (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
                                    );

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning_and_evening = (SELECT count(*)
                                                       FROM subsample_users_characterization
                                                       WHERE ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
                                                         AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
                                    );

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning_or_evening_inside_Porto = (SELECT count(*)
                                                                   FROM subsample_users_characterization
                                                                   WHERE home_id IN (SELECT cell_id FROM call_dim_porto)
                                                                     AND workplace_id IN (SELECT cell_id FROM call_dim_porto)
                                                                     AND ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
                                                                     AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
                                                                  );

UPDATE stats_number_users_subsample
SET users_feasible_travelTimes_morning_and_evening_inside_Porto = (SELECT count(*)
                                                                   FROM subsample_users_characterization
                                                                   WHERE home_id IN (SELECT cell_id FROM call_dim_porto)
                                                                     AND workplace_id IN (SELECT cell_id FROM call_dim_porto)
                                                                     AND ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
                                                                     AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
                                                                  );

---------------------------------------------------- STATS OF THE USERS THAT MADE/RECEIVED CALLS INSIDE THE REGION --------------------------------
UPDATE stats_number_users_region
SET total_users = (SELECT count(*) FROM subsample_users_characterization);

UPDATE stats_number_users_region
SET users_activity_weekdays = (SELECT count(*)
                               FROM subsample_users_characterization
                               WHERE "Number of Calls Made/Received During the Weekdays" IS NOT NULL
                              );

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

UPDATE stats_number_users_region
SET users_with_home_and_work = (SELECT count(*)
                                FROM subsample_users_characterization
                                WHERE workplace_id IS NOT NULL
                                AND home_id IS NOT NULL
                               );

UPDATE stats_number_users_region
SET users_with_home_and_work_not_same = (SELECT count(*)
                                         FROM subsample_users_characterization
                                         WHERE workplace_id IS NOT NULL
                                         AND home_id IS NOT NULL
                                         AND home_id != workplace_id
                                        );

UPDATE stats_number_users_region
SET users_morning_calls = (SELECT count(*)
                           FROM subsample_users_characterization
                           WHERE "Number of Calls Made/Received During the Morning" IS NOT NULL
                          );

UPDATE stats_number_users_region
SET users_evening_calls = (SELECT count(*)
                           FROM subsample_users_characterization
                           WHERE "Number of Calls Made/Received During the Evening" IS NOT NULL
                          );

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

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning = (SELECT count(*)
                                           FROM subsample_users_characterization
                                          WHERE minTravelTime_H_W IS NOT NULL
                                            AND "Travel Speed H_W (Km/h)" <= 250
                                            AND "Travel Speed H_W (Km/h)" >= 3
                                          );

UPDATE stats_number_users_region
SET users_feasible_travelTimes_evening = (SELECT count(*)
                                           FROM subsample_users_characterization
                                          WHERE minTravelTime_W_H IS NOT NULL
                                            AND "Travel Speed W_H (Km/h)" <= 250
                                            AND "Travel Speed W_H (Km/h)" >= 3
                                    );

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning_or_evening = (SELECT count(*)
                                                       FROM subsample_users_characterization
                                                      WHERE ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
                                                         OR (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
                                    );

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning_and_evening = (SELECT count(*)
                                                       FROM subsample_users_characterization
                                                       WHERE ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
                                                         AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
                                    );

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning_or_evening_inside_Porto = (SELECT count(*)
                                                                   FROM subsample_users_characterization
                                                                   WHERE home_id IN (SELECT cell_id FROM call_dim_porto)
                                                                     AND workplace_id IN (SELECT cell_id FROM call_dim_porto)
                                                                     AND ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
                                                                     AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
                                                                  );

UPDATE stats_number_users_region
SET users_feasible_travelTimes_morning_and_evening_inside_Porto = (SELECT count(*)
                                                                   FROM subsample_users_characterization
                                                                   WHERE home_id IN (SELECT cell_id FROM call_dim_porto)
                                                                     AND workplace_id IN (SELECT cell_id FROM call_dim_porto)
                                                                     AND ((minTravelTime_H_W IS NOT NULL AND "Travel Speed H_W (Km/h)" <= 250 AND "Travel Speed H_W (Km/h)" >= 3)
                                                                     AND (minTravelTime_W_H IS NOT NULL AND "Travel Speed W_H (Km/h)" <= 250 AND "Travel Speed W_H (Km/h)" >= 3))
                                                                  );


-------------------------------------------------- RESULTS OF ALL OPERATIONS IN ORDER TO MAKE THE STATISTICAL ANALYSIS ----------------------------------------------------------------------------------------------
SELECT * FROM stats_number_users_preprocess;
SELECT * FROM stats_number_records_preprocess;

SELECT * FROM stats_number_users_region;
SELECT * FROM stats_number_records_region;

SELECT * FROM stats_number_users_subsample;
SELECT * FROM stats_number_records_subsample;

SELECT * FROM ODPorto_stats;