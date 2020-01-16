-- ----------------------------------------- REMOVING OSCILLATION SEQUENCES ---------------------------
/*These series of calculations were done in the subset of the CDR's of the specific region due to the high demand for computational power.
  Check the continuity of the same call through different records:
    -- (1) if the difference between the date_id's of two registers between the exact same users is equal to 0 and there is a difference of towers --> delete all of them!
    -- if the difference between the date_id's of two registers between the exact same users is equal to the duration of the first record, then we have a call continuity. It can be:
        -- (2) merge the cdr's: once the originating_cell_ids or the terminating_cell_ids are the same
        -- (3) user is moving and changed legitimately his/her cell tower: once at least one of the originating_cell_ids or the terminating_cell_ids changed and is not an oscillation
        -- (4) oscillation: once it is noticed the changed previously described was done at a ridiculous speed (400 km/h). In this case we assume that the first record is the true one and fuse.
                        -- there are cases where users switch cells more than once??? If so, another action needs to be done*/
----------------------------------------------------------
-- CREATE SEQUENCE serial START 1;
----------------------------------------------
DROP TABLE IF EXISTS altice_differences;
CREATE TEMPORARY TABLE altice_differences AS( -- creating a temporary table that calculates difference between date_ids of the calls between the same users and potentially identify call continuity
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
      FROM altice_unique_call_fct
    ) f
    ORDER BY originating_id, terminating_id, date_id
  )g

);
----------------------------------------------------------

-- case (1)
DROP TABLE IF EXISTS altice_case1;
CREATE TEMPORARY TABLE altice_case1 AS (
  SELECT *
  FROM altice_differences
  WHERE diffDates = 0
);

START TRANSACTION;
DELETE -- WARNING: DELETES CAN TAKE MULTIPLE DAYS OF PROCESSING, SPECIALLY IF RECORDS ARE IN THE ORDER OF THOUSANDS OF MILLIONS
FROM altice_unique_call_fct u
USING altice_case1 d
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


----------------------------------------------------------
-- case (2)
DROP TABLE IF EXISTS altice_case2;
CREATE TEMPORARY TABLE altice_case2 AS (
  SELECT *
  FROM altice_differences
  WHERE diffDates = lagduration_amt
        AND originating_cell_id = lagoriginating_cell_id
        AND terminating_cell_id = lagterminating_cell_id
);

----------------------------------------------------------
DROP TABLE IF EXISTS altice_mergecase2;
CREATE TEMPORARY TABLE altice_mergecase2 AS (
  SELECT lagoriginating_id AS originating_id,
         lagoriginating_cell_id AS originating_cell_id,
         lagterminating_id AS terminating_id,
         lagterminating_cell_id AS terminating_cell_id,
         lagdate_id AS date_id,
         soma + lagduration_amt AS soma
  FROM (
    SELECT DISTINCT ON (mySequence) *
    FROM (SELECT * FROM altice_differences ORDER BY date_id) a
    WHERE diffDates = lagduration_amt
          AND originating_cell_id = lagoriginating_cell_id
          AND terminating_cell_id = lagterminating_cell_id
  ) a
  INNER JOIN (SELECT mySequence AS seq,  sum(duration_amt) AS soma
              FROM altice_differences
              GROUP BY mySequence) r
  ON a.mySequence = seq
);

START TRANSACTION;
DELETE -- WARNING: DELETES CAN TAKE MULTIPLE DAYS OF PROCESSING, SPECIALLY IF RECORDS ARE IN THE ORDER OF THOUSANDS OF MILLIONS
FROM altice_unique_call_fct u
USING altice_case2 d
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


START TRANSACTION;
INSERT INTO altice_unique_call_fct (originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, duration_amt)
(SELECT originating_id, originating_cell_id, terminating_id, terminating_cell_id, date_id, soma
  FROM altice_mergecase2);
COMMIT;

----------------------------------------------------------
-- case (3) and (4)
DROP TABLE IF EXISTS altice_case3and4;
CREATE TEMPORARY TABLE altice_case3and4 AS (
  SELECT *, geom1 AS ori_geom_point, geom2 AS term_geom_point, geom3 AS lagori_geom_point, geom4 AS lagterm_geom_point
  FROM(
    SELECT *
    FROM altice_differences
    WHERE diffDates = lagduration_amt
          AND (originating_cell_id != lagoriginating_cell_id OR terminating_cell_id != lagterminating_cell_id)
  ) b

  INNER JOIN (SELECT cell_id AS cid, geom_point AS geom1 FROM altice_call_dim) u
  ON originating_cell_id = cid

  INNER JOIN (SELECT cell_id AS cide, geom_point AS geom2 FROM altice_call_dim) v
  ON terminating_cell_id = cide

  INNER JOIN (SELECT cell_id AS cida, geom_point AS geom3 FROM altice_call_dim) s
  ON lagoriginating_cell_id = cida

  INNER JOIN (SELECT cell_id AS cidu, geom_point AS geom4 FROM altice_call_dim) d
  ON lagterminating_cell_id = cidu
);

----------------------------------------------------------
-- case (4)
DROP TABLE IF EXISTS altice_switchspeedscase4;
CREATE TABLE altice_switchspeedscase4 AS (
  SELECT *,
             (CAST(distanciaOrig AS FLOAT)/1000)/(CAST(lagduration_amt AS FLOAT)/3600) AS "Switch Speed Origin - Km per hour",
             (CAST(distanciaTerm AS FLOAT)/1000)/(CAST(lagduration_amt AS FLOAT)/3600) AS "Switch Speed Terminating - Km per hour"
  FROM (SELECT *,
           st_distance(ST_Transform(ori_geom_point, 3857), ST_Transform(lagori_geom_point, 3857)) AS distanciaOrig,
           st_distance(ST_Transform(term_geom_point, 3857), ST_Transform(lagterm_geom_point, 3857)) AS distanciaTerm
        FROM altice_case3and4) r
);
