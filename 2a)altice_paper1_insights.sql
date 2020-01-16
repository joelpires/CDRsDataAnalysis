
-- ----------------------------------------------------------------------------------------- EXPERIMENT 5: Relation Between the 3 varibles -------------------------------------------------------------------- --
DROP TABLE IF EXISTS altice_experiment5;
CREATE TABLE altice_experiment5 AS (
  SELECT "Call Every x Days (on Average)", "Average Calls Per Day", "Nº Active Days"
  FROM altice_users_characterization_final
);

-- ----------------------------------------------------------------------------------------- EXPERIMENT 4: TOWER DENSITY -------------------------------------------------------------------- --

/*
Experiment 4.1: Universe is every user that has house and workplace well-defined, both inside the municipal
*/
DROP TABLE IF EXISTS altice_users_characterization_experiment4_1;
CREATE TEMPORARY TABLE altice_users_characterization_experiment4_1 AS (
  SELECT *, CASE
            WHEN (home_id = workplace_id) THEN 1
            ELSE 0
          END AS notdistinct
  FROM altice_users_characterization_final
  WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL AND municipalHome IS NOT NULL AND municipalWorkplace IS NOT NULL
);

-- ------------------------------------------------------
DROP TABLE IF EXISTS altice_usersDEN_byMunicipal;
CREATE TEMPORARY TABLE altice_usersDEN_byMunicipal AS(
  SELECT municipalHome , densityHome, count(*) AS usersDEN
  FROM (
    SELECT *
    FROM altice_users_characterization_experiment4_1
  ) u
  GROUP BY municipalHome, densityHome
);

-- ------------------------------------------------------
DROP TABLE IF EXISTS altice_usersNUM_byMunicipal;
CREATE TEMPORARY TABLE altice_usersNUM_byMunicipal AS(
  SELECT municipalWorkplace, densityWorkplace, count(*) AS usersNUM
  FROM (
    SELECT *
    FROM altice_users_characterization_experiment4_1
    WHERE notdistinct = 0
  ) u
  GROUP BY municipalWorkplace, densityWorkplace
);

-- ------------------------------------------------------
DROP TABLE IF EXISTS experiment_4_1;
CREATE TABLE experiment_4_1 AS (
  SELECT municipalHome AS municipal, densityHome AS tower_density, usersNUM, usersDEN,  CASE WHEN CAST(usersNUM AS FLOAT)*100/usersDEN IS NULL
                                                                                         THEN 0
                                                                                         ELSE CAST(usersNUM AS FLOAT)*100/usersDEN

                                                                                 END AS racio
  FROM altice_usersNUM_byMunicipal h
  RIGHT  JOIN altice_usersDEN_byMunicipal j
      ON municipalHome = municipalWorkplace
  ORDER BY tower_density
);

-- THE JUSTIFICATION FOR THESE RESULTS CAN BE BECAUSE WE CAN'T DISTINGUISH THE USERS THAT ACTUALLY WORK AND LIVE WITHIN THE SAME CELL AREA AND FROM altice_THE ONES THAT ARE WORK AND LIVE IN DIFFERENT LOCATIONS IF WE COULD AUGMENT THE SPATIAL RESOLUTION
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*
Experiment 4.2: Universe is every user that intermediate calls
*/
-- ------------------------------------------------------
DROP TABLE IF EXISTS altice_aux_intermediateTowers_H_W;
CREATE TEMPORARY TABLE altice_aux_intermediateTowers_H_W AS (
  SELECT f.intermediateTowers_H_WID AS id, name_2, "Tower Density (Km2 per Cell)", tower, intermediateHome_H_W, intermediateWorkplace_H_W
  FROM altice_intermediateTowers_H_W_u f
  INNER JOIN altice_infomunicipals_and_cells g
  ON g.cell_id = f.tower
);

-- ------------------------------------------------------
DROP TABLE IF EXISTS altice_aux_intermediateTowers_W_H;
CREATE TEMPORARY TABLE altice_aux_intermediateTowers_W_H AS (
  SELECT f.intermediateTowers_W_HID AS id, name_2, "Tower Density (Km2 per Cell)", tower, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM altice_intermediateTowers_W_H_u f
  INNER JOIN altice_infomunicipals_and_cells g
  ON g.cell_id = f.tower
);

-- ------------------------------------------------------
DROP TABLE IF EXISTS altice_aux_experiment4_2;
CREATE TABLE altice_aux_experiment4_2 AS (
  SELECT f."Tower Density (Km2 per Cell)", f.id, f.name_2,  f.tower, intermediateHome_H_W, intermediateWorkplace_H_W, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM altice_aux_intermediateTowers_H_W f
  LEFT JOIN altice_aux_intermediateTowers_W_H g
  ON f.id = g.id

  UNION

  SELECT f."Tower Density (Km2 per Cell)", f.id, f.name_2,  f.tower, intermediateHome_H_W, intermediateWorkplace_H_W, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM altice_aux_intermediateTowers_W_H f
  LEFT JOIN altice_aux_intermediateTowers_H_W g
  ON f.id = g.id

);

-- ------------------------------------------------------
SELECT count(*) FROM altice_aux_experiment4_2; -- should be equal to SELECT count(*) FROM altice_users_characterization WHERE number_intermediateTowers_W_H IS NOT NULL AND number_intermediateTowers_H_W IS NOT NULL

-- ------------------------------------------------------
-- EXPERIMENT 4_2_1:
DROP TABLE IF EXISTS altice_experiment4_2_1_universe;
CREATE TEMPORARY TABLE altice_experiment4_2_1_universe AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", id, intermediateHome_H_W, intermediateWorkplace_H_W
  FROM altice_aux_experiment4_2
  WHERE intermediateHome_H_W IS NOT NULL AND intermediateWorkplace_H_W IS NOT NULL
);

DROP TABLE IF EXISTS altice_experiment4_2_1_usersDEN_byMunicipal;
CREATE TEMPORARY TABLE altice_experiment4_2_1_usersDEN_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W) AS usersDEN
  FROM (
      SELECT name_2, "Tower Density (Km2 per Cell)", id, count(*) AS intermediateTowersH_W
      FROM altice_experiment4_2_1_universe
      GROUP BY name_2, "Tower Density (Km2 per Cell)", id
  ) t
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

DROP TABLE IF EXISTS altice_experiment4_2_1_usersNUM_byMunicipal;
CREATE TEMPORARY TABLE altice_experiment4_2_1_usersNUM_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_notH_notW) AS usersNUM
  FROM (
    SELECT p.name_2, "Tower Density (Km2 per Cell)", id, coalesce(count(*),0) AS intermediateTowersH_W_notH_notW
    FROM (
      SELECT *
      FROM altice_experiment4_2_1_universe
      WHERE intermediateHome_H_W = 0 AND intermediateWorkplace_H_W = 0
    ) y
    RIGHT JOIN (SELECT DISTINCT name_2 FROM (SELECT DISTINCT ON (name_2) * FROM altice_experiment4_2_1_universe)u)p
    ON p.name_2 = y.name_2
    GROUP BY p.name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

--------------------------------------------------------------------------------------
-- EXPERIMENT 4_2_2:
DROP TABLE IF EXISTS altice_experiment4_2_2_universe;
CREATE TEMPORARY TABLE altice_experiment4_2_2_universe AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", id, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM altice_aux_experiment4_2
  WHERE intermediateHome_W_H IS NOT NULL AND intermediateWorkplace_W_H IS NOT NULL
);

DROP TABLE IF EXISTS altice_experiment4_2_2_usersDEN_byMunicipal;
CREATE TEMPORARY TABLE altice_experiment4_2_2_usersDEN_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersW_H) AS usersDEN
  FROM (
    SELECT name_2, "Tower Density (Km2 per Cell)", id, count(*) AS intermediateTowersW_H
    FROM altice_experiment4_2_2_universe
    GROUP BY name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

DROP TABLE IF EXISTS altice_experiment4_2_2_usersNUM_byMunicipal;
CREATE TEMPORARY TABLE altice_experiment4_2_2_usersNUM_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersW_H_notH_notW) AS usersNUM
  FROM (
    SELECT p.name_2, "Tower Density (Km2 per Cell)", id, coalesce(count(*),0) AS intermediateTowersW_H_notH_notW
    FROM (
        SELECT *
        FROM altice_experiment4_2_2_universe
        WHERE intermediateHome_W_H = 0 AND intermediateWorkplace_W_H = 0
    ) y
    RIGHT JOIN (SELECT DISTINCT name_2 FROM (SELECT DISTINCT ON (name_2) * FROM altice_experiment4_2_2_universe)u)p
    ON p.name_2 = y.name_2

    GROUP BY p.name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

--------------------------------------------------------------------------------------
-- EXPERIMENT 4_2_3:

DROP TABLE IF EXISTS altice_experiment4_2_3_universe;
CREATE TABLE altice_experiment4_2_3_universe AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", id, intermediateHome_H_W, intermediateWorkplace_H_W, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM altice_aux_experiment4_2
    WHERE (intermediateHome_H_W IS NOT NULL AND intermediateWorkplace_H_W IS NOT NULL) OR (intermediateHome_W_H IS NOT NULL AND intermediateWorkplace_W_H IS NOT NULL)
);


DROP TABLE IF EXISTS altice_experiment4_2_3_usersDEN_byMunicipal;
CREATE TEMPORARY TABLE altice_experiment4_2_3_usersDEN_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_H) AS usersDEN
  FROM (
    SELECT name_2, "Tower Density (Km2 per Cell)", id, count(*) AS intermediateTowersH_W_H
    FROM altice_experiment4_2_3_universe
    GROUP BY name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

DROP TABLE IF EXISTS altice_experiment4_2_3_usersNUM_byMunicipal;
CREATE TEMPORARY TABLE altice_experiment4_2_3_usersNUM_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_H_notH_notW) AS usersNUM
  FROM (
    SELECT p.name_2, "Tower Density (Km2 per Cell)", id, coalesce(count(*),0) AS intermediateTowersH_W_H_notH_notW
    FROM (
        SELECT *
        FROM altice_experiment4_2_3_universe
        WHERE ((intermediateHome_H_W = 0 AND intermediateWorkplace_H_W = 0) OR (intermediateHome_W_H = 0 AND intermediateWorkplace_W_H = 0))
    ) y
    RIGHT JOIN (SELECT DISTINCT name_2 FROM (SELECT DISTINCT ON (name_2) * FROM altice_experiment4_2_3_universe)u)p
    ON p.name_2 = y.name_2
    GROUP BY p.name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);


--------------------------------------------------------------------------------------
-- EXPERIMENT 4_2_4:
DROP TABLE IF EXISTS altice_experiment4_2_4_universe;
CREATE TEMPORARY TABLE altice_experiment4_2_4_universe AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", id, intermediateHome_H_W, intermediateWorkplace_H_W, intermediateHome_W_H, intermediateWorkplace_W_H
  FROM altice_aux_experiment4_2
    WHERE (intermediateHome_H_W IS NOT NULL AND intermediateWorkplace_H_W IS NOT NULL) AND (intermediateHome_W_H IS NOT NULL AND intermediateWorkplace_W_H IS NOT NULL)
);

DROP TABLE IF EXISTS altice_experiment4_2_4_usersDEN_byMunicipal;
CREATE TEMPORARY TABLE altice_experiment4_2_4_usersDEN_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_H) AS usersDEN
  FROM (
    SELECT name_2, "Tower Density (Km2 per Cell)", id, count(*) AS intermediateTowersH_W_H
    FROM altice_experiment4_2_4_universe
    GROUP BY name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);

DROP TABLE IF EXISTS altice_experiment4_2_4_usersNUM_byMunicipal;
CREATE TEMPORARY TABLE altice_experiment4_2_4_usersNUM_byMunicipal AS (
  SELECT name_2, "Tower Density (Km2 per Cell)", sum(intermediateTowersH_W_H_notH_notW) AS usersNUM
  FROM (
    SELECT p.name_2, "Tower Density (Km2 per Cell)", id, coalesce(count(*),0) AS intermediateTowersH_W_H_notH_notW
    FROM (
        SELECT *
        FROM altice_experiment4_2_4_universe
        WHERE ((intermediateHome_H_W = 0 AND intermediateWorkplace_H_W = 0) AND (intermediateHome_W_H = 0 AND intermediateWorkplace_W_H = 0))
      ) y
    RIGHT JOIN (SELECT DISTINCT name_2 FROM (SELECT DISTINCT ON (name_2) * FROM altice_experiment4_2_4_universe)u)p
    ON p.name_2 = y.name_2
    GROUP BY p.name_2, "Tower Density (Km2 per Cell)", id
  ) o
  GROUP BY name_2, "Tower Density (Km2 per Cell)"
);


--------------------------------------------------------------------------------------
DROP TABLE IF EXISTS altice_experiment_4_2;
CREATE TABLE altice_experiment_4_2 AS (
  SELECT a.name_2, a."Tower Density (Km2 per Cell)", coalesce(CAST(b.usersNUM AS FLOAT)*100/a.usersDEN,0) AS racioH_W, coalesce(CAST(d.usersNUM AS FLOAT)*100/c.usersDEN,0) AS racioW_H, coalesce(CAST(f.usersNUM AS FLOAT)*100/e.usersDEN,0) AS racioH_W_or_W_H, coalesce(CAST(h.usersNUM AS FLOAT)*100/g.usersDEN,0) AS racioH_W_and_W_H
  FROM altice_experiment4_2_1_usersDEN_byMunicipal a

  LEFT JOIN altice_experiment4_2_1_usersNUM_byMunicipal b
  ON a.name_2 = b.name_2

  LEFT JOIN altice_experiment4_2_2_usersDEN_byMunicipal c
  ON a.name_2 = c.name_2

  LEFT JOIN altice_experiment4_2_2_usersNUM_byMunicipal d
  ON a.name_2 = d.name_2

  LEFT JOIN altice_experiment4_2_3_usersDEN_byMunicipal e
  ON a.name_2 = e.name_2

  LEFT JOIN altice_experiment4_2_3_usersNUM_byMunicipal f
  ON a.name_2 = f.name_2

  LEFT JOIN altice_experiment4_2_4_usersDEN_byMunicipal g
  ON a.name_2 = g.name_2

  LEFT JOIN altice_experiment4_2_4_usersNUM_byMunicipal h
  ON a.name_2 = h.name_2

);

-- ----------------------------------------------------------------------------------------- EXPERIMENT 3: NUMBER OF DIFFERENT DAYS -------------------------------------------------------------------- --
/*
Experiment 3.1: Universe is every user < x ActiveDays
*/
DROP TABLE IF EXISTS altice_experiment_3_universe;
CREATE TEMPORARY TABLE altice_experiment_3_universe AS (
  SELECT "Nº Active Days", user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" AS M_H, "Number of Calls Made/Received in The Workplace During the Morning" AS M_W, "Number of Calls Made/Received at Home During the Evening" AS E_H, "Number of Calls Made/Received in The Workplace During the Evening" AS E_W, "Number of Calls Made/Received During the Weekdays" AS nweekdays
  FROM altice_users_characterization
  GROUP BY "Nº Active Days", user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" , "Number of Calls Made/Received in The Workplace During the Morning", "Number of Calls Made/Received at Home During the Evening", "Number of Calls Made/Received in The Workplace During the Evening", "Number of Calls Made/Received During the Weekdays"
  ORDER BY "Nº Active Days"
);

DROP TABLE IF EXISTS altice_experiment_3_usersDEN;
CREATE TEMPORARY TABLE altice_experiment_3_usersDEN AS (
  SELECT "Nº Active Days", sum(pre_usersDEN) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersDEN  -- cumulative sum of users
  FROM (
    SELECT "Nº Active Days", count(DISTINCT user_id) AS pre_usersDEN
    FROM altice_experiment_3_universe
    GROUP BY "Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersDEN
);

-----------------------------------------------------
--EXPERIMENT 3.1.1:
DROP TABLE IF EXISTS altice_experiment_3_1_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_1_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
         SELECT *
         FROM altice_experiment_3_universe
         WHERE home_id IS NOT NULL
    ) t
     RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
      ON p."Nº Active Days" = t."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

-----------------------------------------------------
--EXPERIMENT 3.1.2:
DROP TABLE IF EXISTS altice_experiment_3_1_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_1_2_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_3_universe
      WHERE workplace_id IS NOT NULL
    ) y
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = y."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

-----------------------------------------------------
--EXPERIMENT 3.1.3:
DROP TABLE IF EXISTS altice_experiment_3_1_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_1_3_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_3_universe
      WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL
    ) i
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = i."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

-----------------------------------------------------
--Results experiment_3_1:
DROP TABLE IF EXISTS altice_experiment_3_1;
CREATE TABLE altice_experiment_3_1 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace
  FROM altice_experiment_3_usersDEN a
  LEFT JOIN altice_experiment_3_1_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
  LEFT JOIN altice_experiment_3_1_2_usersNUM c
  ON a."Nº Active Days" = c."Nº Active Days"
  LEFT JOIN altice_experiment_3_1_3_usersNUM d
  ON a."Nº Active Days" = d."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_3_1;

/*
Experiment 3.2: Universe is every user < x ActiveDays
*/

-----------------------------------------------------
--EXPERIMENT 3.2.1:
DROP TABLE IF EXISTS altice_experiment_3_2_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_2_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_3_universe
      WHERE M_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 3.2.2:
DROP TABLE IF EXISTS altice_experiment_3_2_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_2_2_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_3_universe
      WHERE M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

-----------------------------------------------------
--EXPERIMENT 3.2.3:
DROP TABLE IF EXISTS altice_experiment_3_2_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_2_3_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_3_universe
      WHERE M_H IS NOT NULL AND M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

-----------------------------------------------------
--Results:
DROP TABLE IF EXISTS altice_experiment_3_2;
CREATE TABLE altice_experiment_3_2 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Morning,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Morning,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Morning
  FROM altice_experiment_3_usersDEN a
  LEFT JOIN altice_experiment_3_2_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
  LEFT JOIN altice_experiment_3_2_2_usersNUM c
  ON a."Nº Active Days" = c."Nº Active Days"
  LEFT JOIN altice_experiment_3_2_3_usersNUM d
  ON a."Nº Active Days" = d."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_3_2;



/*
Experiment 3.3: Universe is every user < x ActiveDays
*/

-----------------------------------------------------
--EXPERIMENT 3.3.1:
DROP TABLE IF EXISTS altice_experiment_3_3_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_3_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id)) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_3_universe
      WHERE E_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 3.3.2:
DROP TABLE IF EXISTS altice_experiment_3_3_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_3_2_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_3_universe
      WHERE E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

-----------------------------------------------------
--EXPERIMENT 3.3.3:
DROP TABLE IF EXISTS altice_experiment_3_3_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_3_3_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_3_universe
      WHERE E_H IS NOT NULL AND E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

-----------------------------------------------------
--Results:
DROP TABLE IF EXISTS altice_experiment_3_3;
CREATE TABLE altice_experiment_3_3 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Evening,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Evening,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Evening
  FROM altice_experiment_3_usersDEN a
  LEFT JOIN altice_experiment_3_3_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
  LEFT JOIN altice_experiment_3_3_2_usersNUM c
  ON a."Nº Active Days" = c."Nº Active Days"
  LEFT JOIN altice_experiment_3_3_3_usersNUM d
  ON a."Nº Active Days" = d."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_3_3;

/*
Experiment 3.4: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 3.4.1:
DROP TABLE IF EXISTS altice_experiment_3_4_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_4_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f."Nº Active Days", f.user_id
      FROM altice_experiment_3_universe f
      INNER JOIN altice_intermediateTowers_H_W_u g
          ON f.user_id = g.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = u."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

-----------------------------------------------------
--EXPERIMENT 3.4.2:
DROP TABLE IF EXISTS altice_experiment_3_4_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_4_2_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f."Nº Active Days", f.user_id
      FROM altice_experiment_3_universe f
      INNER JOIN altice_intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
    ) u
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = u."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 3.4.3:
DROP TABLE IF EXISTS altice_experiment_3_4_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_4_3_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f."Nº Active Days", f.user_id
      FROM altice_experiment_3_universe f
      INNER JOIN altice_aux_experiment4_2 g
          ON f.user_id = g.id
    ) u
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = u."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 3.4.4:
DROP TABLE IF EXISTS altice_experiment_3_4_4_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_4_4_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f."Nº Active Days", f.user_id
      FROM altice_experiment_3_universe f
      INNER JOIN altice_intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
      INNER JOIN altice_intermediateTowers_H_W_u t
          ON f.user_id = t.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = u."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);
SELECT * FROM altice_experiment_3_4_3_usersNUM;


-----------------------------------------------------
-- Results:
DROP TABLE IF EXISTS altice_experiment_3_4;
CREATE TABLE altice_experiment_3_4 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioW_H,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_or_W_H,
         coalesce(CAST(e.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_and_W_H
  FROM altice_experiment_3_usersDEN a
  LEFT JOIN altice_experiment_3_4_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
  LEFT JOIN altice_experiment_3_4_2_usersNUM c
  ON a."Nº Active Days" = c."Nº Active Days"
  LEFT JOIN altice_experiment_3_4_3_usersNUM d
  ON a."Nº Active Days" = d."Nº Active Days"
  LEFT JOIN altice_experiment_3_4_4_usersNUM e
  ON a."Nº Active Days" = e."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_3_4;

/*
Experiment 3.5: Universe is every user < x averageCalls
*/
-----------------------------------------------------
--EXPERIMENT 3.5.1:
DROP TABLE IF EXISTS altice_experiment_3_5_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_3_5_1_usersNUM AS (
  SELECT "Nº Active Days", sum(pre_usersNUM) OVER (ORDER BY "Nº Active Days" ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p."Nº Active Days", coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_3_universe
      WHERE nweekdays IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT "Nº Active Days" FROM (SELECT DISTINCT ON ("Nº Active Days") * FROM altice_experiment_3_universe)u)p
    ON p."Nº Active Days" = o."Nº Active Days"
    GROUP BY p."Nº Active Days"
  ) t
  GROUP BY "Nº Active Days", pre_usersNUM
);

-----------------------------------------------------
--Results:
DROP TABLE IF EXISTS altice_experiment_3_5;
CREATE TABLE altice_experiment_3_5 AS (
  SELECT a."Nº Active Days",
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWeekdays
  FROM altice_experiment_3_usersDEN a
  LEFT JOIN altice_experiment_3_5_1_usersNUM b
  ON a."Nº Active Days" = b."Nº Active Days"
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_3_5;

-- ----------------------------------------------------------------------------------------- EXPERIMENT 2: REGULARITY -------------------------------------------------------------------- --
/*
Experiment 2.1: Universe is every user < x ActiveDays
*/
DROP TABLE IF EXISTS altice_experiment_2_universe;
CREATE TEMPORARY TABLE altice_experiment_2_universe AS (
  SELECT ROUND(CAST("Call Every x Days (on Average)" AS NUMERIC),1) AS regularity, user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" AS M_H, "Number of Calls Made/Received in The Workplace During the Morning" AS M_W, "Number of Calls Made/Received at Home During the Evening" AS E_H, "Number of Calls Made/Received in The Workplace During the Evening" AS E_W, "Number of Calls Made/Received During the Weekdays" AS nweekdays
  FROM altice_users_characterization_final
  GROUP BY "Call Every x Days (on Average)", user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" , "Number of Calls Made/Received in The Workplace During the Morning", "Number of Calls Made/Received at Home During the Evening", "Number of Calls Made/Received in The Workplace During the Evening", "Number of Calls Made/Received During the Weekdays"
  ORDER BY "Call Every x Days (on Average)"
);

-- ----------------------------------------------------
DROP TABLE IF EXISTS altice_experiment_2_usersDEN;
CREATE TEMPORARY TABLE altice_experiment_2_usersDEN AS (
  SELECT regularity, sum(pre_usersDEN) OVER (ORDER BY regularity DESC) AS cumulative_usersDEN  -- cumulative sum of users
  FROM (
    SELECT regularity, count(DISTINCT user_id) AS pre_usersDEN
    FROM altice_experiment_2_universe
    GROUP BY regularity
  ) t
  GROUP BY regularity, pre_usersDEN
);

-----------------------------------------------------
--EXPERIMENT 2.1.1:
DROP TABLE IF EXISTS altice_experiment_2_1_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_1_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT y.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
         SELECT *
         FROM altice_experiment_2_universe
         WHERE home_id IS NOT NULL
    ) t
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)y
        ON y.regularity = t.regularity
    GROUP BY y.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);



-----------------------------------------------------
--EXPERIMENT 2.1.2:
DROP TABLE IF EXISTS altice_experiment_2_1_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_1_2_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_2_universe
      WHERE workplace_id IS NOT NULL
    ) y
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = y.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 2.1.3:
DROP TABLE IF EXISTS altice_experiment_2_1_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_1_3_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_2_universe
      WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL
    ) i
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = i.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

-----------------------------------------------------
--Results experiment_2_1:
DROP TABLE IF EXISTS altice_experiment_2_1;
CREATE TABLE altice_experiment_2_1 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace
  FROM altice_experiment_2_usersDEN a
  LEFT JOIN altice_experiment_2_1_1_usersNUM b
  ON a.regularity = b.regularity
  LEFT JOIN altice_experiment_2_1_2_usersNUM c
  ON a.regularity = c.regularity
  LEFT JOIN altice_experiment_2_1_3_usersNUM d
  ON a.regularity = d.regularity
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_2_1 ORDER BY regularity DESC;


/*
Experiment 2.2: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 2.2.1:
DROP TABLE IF EXISTS altice_experiment_2_2_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_2_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_2_universe
      WHERE M_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 2.2.2:
DROP TABLE IF EXISTS altice_experiment_2_2_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_2_2_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_2_universe
      WHERE M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

-----------------------------------------------------
--EXPERIMENT 2.2.3:
DROP TABLE IF EXISTS altice_experiment_2_2_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_2_3_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_2_universe
      WHERE M_H IS NOT NULL AND M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

-----------------------------------------------------
--Results:
DROP TABLE IF EXISTS altice_experiment_2_2;
CREATE TABLE altice_experiment_2_2 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Morning,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Morning,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Morning
  FROM altice_experiment_2_usersDEN a
  LEFT JOIN altice_experiment_2_2_1_usersNUM b
  ON a.regularity = b.regularity
  LEFT JOIN altice_experiment_2_2_2_usersNUM c
  ON a.regularity = c.regularity
  LEFT JOIN altice_experiment_2_2_3_usersNUM d
  ON a.regularity = d.regularity
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_2_2;



/*
Experiment 2.3: Universe is every user < x ActiveDays
*/

-----------------------------------------------------
--EXPERIMENT 2.3.1:
DROP TABLE IF EXISTS altice_experiment_2_3_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_3_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_2_universe
      WHERE E_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 2.3.2:
DROP TABLE IF EXISTS altice_experiment_2_3_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_3_2_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_2_universe
      WHERE E_W IS NOT NULL
    ) o

    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

-----------------------------------------------------
--EXPERIMENT 2.3.3:
DROP TABLE IF EXISTS altice_experiment_2_3_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_3_3_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_2_universe
      WHERE E_H IS NOT NULL AND E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

-----------------------------------------------------
--Results:
DROP TABLE IF EXISTS altice_experiment_2_3;
CREATE TABLE altice_experiment_2_3 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Evening,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Evening,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Evening
  FROM altice_experiment_2_usersDEN a
  LEFT JOIN altice_experiment_2_3_1_usersNUM b
  ON a.regularity = b.regularity
  LEFT JOIN altice_experiment_2_3_2_usersNUM c
  ON a.regularity = c.regularity
  LEFT JOIN altice_experiment_2_3_3_usersNUM d
  ON a.regularity = d.regularity
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_2_3;

/*
Experiment 2.4: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 2.4.1:
DROP TABLE IF EXISTS altice_experiment_2_4_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_4_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.regularity, f.user_id
      FROM altice_experiment_2_universe f
      INNER JOIN altice_intermediateTowers_H_W_u g
          ON f.user_id = g.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = u.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

-----------------------------------------------------
--EXPERIMENT 2.4.2:
DROP TABLE IF EXISTS altice_experiment_2_4_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_4_2_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.regularity, f.user_id
      FROM altice_experiment_2_universe f
      INNER JOIN altice_intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
    ) u
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = u.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 2.4.3:
DROP TABLE IF EXISTS altice_experiment_2_4_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_4_3_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.regularity, f.user_id
      FROM altice_experiment_2_universe f
      INNER JOIN altice_aux_experiment4_2 g
          ON f.user_id = g.id
    ) u
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = u.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 2.4.4:
DROP TABLE IF EXISTS altice_experiment_2_4_4_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_4_4_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.regularity, f.user_id
      FROM altice_experiment_2_universe f
      INNER JOIN altice_intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
      INNER JOIN altice_intermediateTowers_H_W_u t
          ON f.user_id = t.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = u.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);
SELECT * FROM altice_experiment_2_4_4_usersNUM;



-----------------------------------------------------
-- Results:
DROP TABLE IF EXISTS altice_experiment_2_4;
CREATE TABLE altice_experiment_2_4 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioW_H,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_or_W_H,
         coalesce(CAST(e.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_and_W_H
  FROM altice_experiment_2_usersDEN a
  LEFT JOIN altice_experiment_2_4_1_usersNUM b
  ON a.regularity = b.regularity
  LEFT JOIN altice_experiment_2_4_2_usersNUM c
  ON a.regularity = c.regularity
  LEFT JOIN altice_experiment_2_4_3_usersNUM d
  ON a.regularity = d.regularity
  LEFT JOIN altice_experiment_2_4_4_usersNUM e
  ON a.regularity = e.regularity
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_2_4;

/*
Experiment 2.5: Universe is every user < x averageCalls
*/
-----------------------------------------------------
--EXPERIMENT 2.5.1:
DROP TABLE IF EXISTS altice_experiment_2_5_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_2_5_1_usersNUM AS (
  SELECT regularity, sum(pre_usersNUM) OVER (ORDER BY regularity DESC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.regularity, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_2_universe
      WHERE nweekdays IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT regularity FROM (SELECT DISTINCT ON (regularity) * FROM altice_experiment_2_universe)u)p
    ON p.regularity = o.regularity
    GROUP BY p.regularity
  ) t
  GROUP BY regularity, pre_usersNUM
);

-----------------------------------------------------
--Results:
DROP TABLE IF EXISTS altice_experiment_2_5;
CREATE TABLE altice_experiment_2_5 AS (
  SELECT a.regularity,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWeekdays
  FROM altice_experiment_2_usersDEN a
  LEFT JOIN altice_experiment_2_5_1_usersNUM b
  ON a.regularity = b.regularity
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_2_5;

-- ----------------------------------------------------------------------------------------- EXPERIMENT 1: AVERAGE CALLS PER DAY -------------------------------------------------------------------- --
/*
Experiment 1.1: Universe is every user < x ActiveDays
*/
DROP TABLE IF EXISTS altice_experiment_1_universe;
CREATE TEMPORARY TABLE altice_experiment_1_universe AS (
  SELECT ROUND(CAST("Average Calls Per Day" AS NUMERIC),1) AS averageCalls, user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" AS M_H, "Number of Calls Made/Received in The Workplace During the Morning" AS M_W, "Number of Calls Made/Received at Home During the Evening" AS E_H, "Number of Calls Made/Received in The Workplace During the Evening" AS E_W, "Number of Calls Made/Received During the Weekdays" AS nweekdays
  FROM altice_users_characterization_final
  GROUP BY "Average Calls Per Day", user_id, home_id, workplace_id, "Number of Calls Made/Received at Home During the Morning" , "Number of Calls Made/Received in The Workplace During the Morning", "Number of Calls Made/Received at Home During the Evening", "Number of Calls Made/Received in The Workplace During the Evening","Number of Calls Made/Received During the Weekdays"
  ORDER BY "Average Calls Per Day"
);

DROP TABLE IF EXISTS altice_experiment_1_usersDEN;
CREATE TEMPORARY TABLE altice_experiment_1_usersDEN AS (
  SELECT averageCalls, sum(pre_usersDEN) OVER (ORDER BY averageCalls ASC) AS cumulative_usersDEN  -- cumulative sum of users
  FROM (
    SELECT averageCalls, count(DISTINCT user_id) AS pre_usersDEN
    FROM altice_experiment_1_universe
    GROUP BY averageCalls
  ) t
  GROUP BY averageCalls, pre_usersDEN
);

-----------------------------------------------------
--EXPERIMENT 1.1.1:
DROP TABLE IF EXISTS altice_experiment_1_1_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_1_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
         SELECT *
         FROM altice_experiment_1_universe
         WHERE home_id IS NOT NULL
    ) t
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = t.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 1.1.2:
DROP TABLE IF EXISTS altice_experiment_1_1_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_1_2_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_1_universe
      WHERE workplace_id IS NOT NULL
    ) y
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = y.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 1.1.3:
DROP TABLE IF EXISTS altice_experiment_1_1_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_1_3_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_1_universe
      WHERE home_id IS NOT NULL AND workplace_id IS NOT NULL
    ) i
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = i.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

-----------------------------------------------------
--Results experiment_1_1:
DROP TABLE IF EXISTS altice_experiment_1_1;
CREATE TABLE altice_experiment_1_1 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace
  FROM altice_experiment_1_usersDEN a
  LEFT JOIN altice_experiment_1_1_1_usersNUM b
  ON a.averageCalls = b.averageCalls
  LEFT JOIN altice_experiment_1_1_2_usersNUM c
  ON a.averageCalls = c.averageCalls
  LEFT JOIN altice_experiment_1_1_3_usersNUM d
  ON a.averageCalls = d.averageCalls
);

/*
Experiment 1.2: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 1.2.1:
DROP TABLE IF EXISTS altice_experiment_1_2_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_2_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_1_universe
      WHERE M_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 1.2.2:
DROP TABLE IF EXISTS altice_experiment_1_2_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_2_2_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_1_universe
      WHERE M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls

    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);



-----------------------------------------------------
--EXPERIMENT 1.2.3:
DROP TABLE IF EXISTS altice_experiment_1_2_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_2_3_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_1_universe
      WHERE M_H IS NOT NULL AND M_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

-----------------------------------------------------
--Results:
DROP TABLE IF EXISTS altice_experiment_1_2;
CREATE TABLE altice_experiment_1_2 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Morning,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Morning,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Morning
  FROM altice_experiment_1_usersDEN a
  LEFT JOIN altice_experiment_1_2_1_usersNUM b
  ON a.averageCalls = b.averageCalls
  LEFT JOIN altice_experiment_1_2_2_usersNUM c
  ON a.averageCalls = c.averageCalls
  LEFT JOIN altice_experiment_1_2_3_usersNUM d
  ON a.averageCalls = d.averageCalls
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_1_2;



/*
Experiment 1.3: Universe is every user < x ActiveDays
*/

-----------------------------------------------------
--EXPERIMENT 1.3.1:
DROP TABLE IF EXISTS altice_experiment_1_3_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_3_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_1_universe
      WHERE E_H IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);



-----------------------------------------------------
--EXPERIMENT 1.3.2:
DROP TABLE IF EXISTS altice_experiment_1_3_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_3_2_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_1_universe
      WHERE E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

-----------------------------------------------------
--EXPERIMENT 1.3.3:
DROP TABLE IF EXISTS altice_experiment_1_3_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_3_3_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_1_universe
      WHERE E_H IS NOT NULL AND E_W IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);

-----------------------------------------------------
--Results:
DROP TABLE IF EXISTS altice_experiment_1_3;
CREATE TABLE altice_experiment_1_3 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Evening,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWorkplace_Evening,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioHome_Workplace_Evening
  FROM altice_experiment_1_usersDEN a
  LEFT JOIN altice_experiment_1_3_1_usersNUM b
  ON a.averageCalls = b.averageCalls
  LEFT JOIN altice_experiment_1_3_2_usersNUM c
  ON a.averageCalls = c.averageCalls
  LEFT JOIN altice_experiment_1_3_3_usersNUM d
  ON a.averageCalls = d.averageCalls
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_1_3;

/*
Experiment 1.4: Universe is every user < x ActiveDays
*/
-----------------------------------------------------
--EXPERIMENT 1.4.1:
DROP TABLE IF EXISTS altice_experiment_1_4_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_4_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.averageCalls, f.user_id
      FROM altice_experiment_1_universe f
      INNER JOIN altice_intermediateTowers_H_W_u g
          ON f.user_id = g.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = u.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);
SELECT * FROM altice_experiment_1_4_1_usersNUM;

-----------------------------------------------------
--EXPERIMENT 1.4.2:
DROP TABLE IF EXISTS altice_experiment_1_4_2_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_4_2_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.averageCalls, f.user_id
      FROM altice_experiment_1_universe f
      INNER JOIN altice_intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
    ) u
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = u.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 1.4.3:
DROP TABLE IF EXISTS altice_experiment_1_4_3_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_4_3_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.averageCalls, f.user_id
      FROM altice_experiment_1_universe f
      INNER JOIN altice_aux_experiment4_2 g
          ON f.user_id = g.id
    ) u
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = u.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);


-----------------------------------------------------
--EXPERIMENT 1.4.4:
DROP TABLE IF EXISTS altice_experiment_1_4_4_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_4_4_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT f.averageCalls, f.user_id
      FROM altice_experiment_1_universe f
      INNER JOIN altice_intermediateTowers_W_H_u g
          ON f.user_id = g.intermediateTowers_W_HID
      INNER JOIN altice_intermediateTowers_H_W_u t
          ON f.user_id = t.intermediateTowers_H_WID
    ) u
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = u.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);


-----------------------------------------------------
-- Results:
DROP TABLE IF EXISTS altice_experiment_1_4;
CREATE TABLE altice_experiment_1_4 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W,
         coalesce(CAST(c.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioW_H,
         coalesce(CAST(d.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_or_W_H,
         coalesce(CAST(e.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioH_W_and_W_H
  FROM altice_experiment_1_usersDEN a
  LEFT JOIN altice_experiment_1_4_1_usersNUM b
  ON a.averageCalls = b.averageCalls
  LEFT JOIN altice_experiment_1_4_2_usersNUM c
  ON a.averageCalls = c.averageCalls
  LEFT JOIN altice_experiment_1_4_3_usersNUM d
  ON a.averageCalls = d.averageCalls
  LEFT JOIN altice_experiment_1_4_4_usersNUM e
  ON a.averageCalls = e.averageCalls
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_1_4;

/*
Experiment 1.5: Universe is every user < x averageCalls
*/
-----------------------------------------------------
--EXPERIMENT 1.5.1:
DROP TABLE IF EXISTS altice_experiment_1_5_1_usersNUM;
CREATE TEMPORARY TABLE altice_experiment_1_5_1_usersNUM AS (
  SELECT averageCalls, sum(pre_usersNUM) OVER (ORDER BY averageCalls ASC) AS cumulative_usersNUM  -- cumulative sum of users
  FROM (
    SELECT p.averageCalls, coalesce(count(DISTINCT user_id),0) AS pre_usersNUM
    FROM (
      SELECT *
      FROM altice_experiment_1_universe
      WHERE nweekdays IS NOT NULL
    ) o
    RIGHT JOIN (SELECT DISTINCT averageCalls FROM (SELECT DISTINCT ON (averageCalls) * FROM altice_experiment_1_universe)u)p
    ON p.averageCalls = o.averageCalls
    GROUP BY p.averageCalls
  ) t
  GROUP BY averageCalls, pre_usersNUM
);


-----------------------------------------------------
--Results:
DROP TABLE IF EXISTS altice_experiment_1_5;
CREATE TABLE altice_experiment_1_5 AS (
  SELECT a.averageCalls,
         coalesce(CAST(b.cumulative_usersNUM AS FLOAT)*100/cumulative_usersDEN,0) AS racioWeekdays
  FROM altice_experiment_1_usersDEN a
  LEFT JOIN altice_experiment_1_5_1_usersNUM b
  ON a.averageCalls = b.averageCalls
);
-- check if the number of different days are preserved
SELECT * FROM altice_experiment_2_2;
SELECT * FROM altice_experiment_2_3;


SELECT * FROM altice_statsmunicipals;

