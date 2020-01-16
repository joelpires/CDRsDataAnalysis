---------------------------- DETERMINING THE FINAL SCORE AND EXACT ROUTES -------------------------------------------------

DROP TYPE altice_MODES;
CREATE TYPE altice_MODES AS (
  mode1 TEXT,
  mode2 TEXT,
  mode3 TEXT,
  mode4 TEXT
);

SELECT * FROM altice_finalroutes_porto;
---------------------- --- STATISTICS AND VISUALIZATIONS TO EXECUTE AFTER PYTHON EXECUTION ------------ ------------ ------------ ------------ ------------

CREATE TABLE IF NOT EXISTS altice_porto_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_porto (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));

DROP TABLE IF EXISTS altice_new_finalRoutes_porto;
CREATE TEMPORARY TABLE altice_new_finalRoutes_porto AS (
  SELECT *
  FROM altice_porto_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_porto)

);


----------------------
CREATE TABLE IF NOT EXISTS altice_lisboa_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_lisboa (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_lisboa;
CREATE TEMPORARY TABLE altice_new_finalRoutes_lisboa AS (
  SELECT *
  FROM altice_lisboa_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_lisboa)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_coimbra_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_coimbra (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_coimbra;
CREATE TEMPORARY TABLE altice_new_finalRoutes_coimbra AS (
  SELECT *
  FROM altice_coimbra_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_coimbra)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_braga_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_braga (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_braga;
CREATE TEMPORARY TABLE altice_new_finalRoutes_braga AS (
  SELECT *
  FROM altice_braga_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_braga)
);

----------------------
CREATE TABLE IF NOT EXISTS altice_setubal_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_setubal (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_setubal;
CREATE TEMPORARY TABLE altice_new_finalRoutes_setubal AS (
  SELECT *
  FROM altice_setubal_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_setubal)
);

----------------------
CREATE TABLE IF NOT EXISTS altice_aveiro_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_aveiro (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_aveiro;
CREATE TEMPORARY TABLE altice_new_finalRoutes_aveiro AS (
  SELECT *
  FROM altice_aveiro_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_aveiro)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_faro_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_faro (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_faro;
CREATE TEMPORARY TABLE altice_new_finalRoutes_faro AS (
  SELECT *
  FROM altice_faro_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_faro)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_leiria_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_leiria (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_leiria;
CREATE TEMPORARY TABLE altice_new_finalRoutes_leiria AS (
  SELECT *
  FROM altice_leiria_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_leiria)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_viana_do_castelo_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_viana_do_castelo (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_viana_do_castelo;
CREATE TEMPORARY TABLE altice_new_finalRoutes_viana_do_castelo AS (
  SELECT *
  FROM altice_viana_do_castelo_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_viana_do_castelo)
);

----------------------
CREATE TABLE IF NOT EXISTS altice_vila_real_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_vila_real (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_vila_real;
CREATE TEMPORARY TABLE altice_new_finalRoutes_vila_real AS (
  SELECT *
  FROM altice_vila_real_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_vila_real)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_viseu_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_viseu (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_viseu;
CREATE TEMPORARY TABLE altice_new_finalRoutes_viseu AS (
  SELECT *
  FROM altice_viseu_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_viseu)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_santarem_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_santarem (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_santarem;
CREATE TEMPORARY TABLE altice_new_finalRoutes_santarem AS (
  SELECT *
  FROM altice_santarem_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_santarem)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_guarda_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_guarda (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_guarda;
CREATE TEMPORARY TABLE altice_new_finalRoutes_guarda AS (
  SELECT *
  FROM altice_guarda_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_guarda)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_portalegre_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_portalegre (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_portalegre;
CREATE TEMPORARY TABLE altice_new_finalRoutes_portalegre AS (
  SELECT *
  FROM altice_portalegre_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_portalegre)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_braganca_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_braganca (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_braganca;
CREATE TEMPORARY TABLE altice_new_finalRoutes_braganca AS (
  SELECT *
  FROM altice_braganca_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_braganca)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_evora_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_evora (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_evora;
CREATE TEMPORARY TABLE altice_new_finalRoutes_evora AS (
  SELECT *
  FROM altice_evora_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_evora)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_castelo_branco_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_castelo_branco (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_castelo_branco;
CREATE TEMPORARY TABLE altice_new_finalRoutes_castelo_branco AS (
  SELECT *
  FROM altice_castelo_branco_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_castelo_branco)

);

----------------------
CREATE TABLE IF NOT EXISTS altice_beja_possible_routes (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
CREATE TABLE IF NOT EXISTS altice_finalroutes_beja (userid integer, commutingtype text, routenumber integer, duration integer, transportmodes modes, latitude numeric, longitude numeric, sequencenumber integer, geom_point_orig geometry(Point,4326));
DROP TABLE IF EXISTS altice_new_finalRoutes_beja;
CREATE TEMPORARY TABLE altice_new_finalRoutes_beja AS (
  SELECT *
  FROM altice_beja_possible_routes
  WHERE (userid, commutingtype, routenumber, duration, transportmodes) IN (SELECT DISTINCT ON (userid, commutingtype) userid, commutingtype, routenumber, duration, transportmodes FROM altice_finalroutes_beja)
);


------------------------------------------------------------------
DROP TABLE IF EXISTS altice_new_porto_possible_routes;
CREATE TEMPORARY TABLE altice_new_porto_possible_routes AS(
  SELECT *
  FROM altice_porto_possible_routes
  WHERE sequencenumber = 0
);

DROP TABLE IF EXISTS altice_new_lisboa_possible_routes;
CREATE TEMPORARY TABLE altice_new_lisboa_possible_routes AS(
  SELECT *
  FROM altice_lisboa_possible_routes
  WHERE sequencenumber = 0
);

DROP TABLE IF EXISTS altice_new_coimbra_possible_routes;
CREATE TEMPORARY TABLE altice_new_coimbra_possible_routes AS(
  SELECT *
  FROM altice_coimbra_possible_routes
  WHERE sequencenumber = 0
);

ALTER TABLE altice_new_porto_possible_routes ADD COLUMN city TEXT;
UPDATE altice_new_porto_possible_routes SET city='Porto';

ALTER TABLE altice_new_lisboa_possible_routes ADD COLUMN city TEXT;
UPDATE altice_new_lisboa_possible_routes SET city='Lisboa';

ALTER TABLE altice_new_coimbra_possible_routes ADD COLUMN city TEXT;
UPDATE altice_new_coimbra_possible_routes SET city='Coimbra';

--  do the same for the other municipals...

DROP TABLE IF EXISTS altice_all_possible_routes;
CREATE TEMPORARY TABLE altice_all_possible_routes AS (
  SELECT *
  FROM altice_new_braga_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_setubal_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_aveiro_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_faro_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_leiria_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_viana_do_castelo_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_vila_real_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_viseu_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_santarem_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_guarda_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_portalegre_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_braganca_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_evora_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_castelo_branco_possible_routes

  UNION ALL

  SELECT *
  FROM altice_new_beja_possible_routes

);


DROP TABLE IF EXISTS altice_aux_finalRoutes_lisboa;
CREATE TEMPORARY TABLE altice_aux_finalRoutes_lisboa AS(
  SELECT *
  FROM altice_new_finalRoutes_lisboa
  WHERE sequencenumber = 0
);

DROP TABLE IF EXISTS altice_aux_finalRoutes_porto;
CREATE TEMPORARY TABLE altice_aux_finalRoutes_porto AS(
  SELECT *
  FROM altice_new_finalRoutes_porto
  WHERE sequencenumber = 0
);

DROP TABLE IF EXISTS altice_aux_finalRoutes_coimbra;
CREATE TEMPORARY TABLE altice_aux_finalRoutes_coimbra AS(
  SELECT *
  FROM altice_new_finalRoutes_coimbra
  WHERE sequencenumber = 0
);

ALTER TABLE altice_aux_finalRoutes_porto ADD COLUMN city TEXT;
UPDATE altice_aux_finalRoutes_porto SET city='Porto';

ALTER TABLE altice_aux_finalRoutes_lisboa ADD COLUMN city TEXT;
UPDATE altice_aux_finalRoutes_lisboa SET city='Lisboa';

DROP TABLE IF EXISTS altice_aux_finalRoutes_beja;
CREATE TEMPORARY TABLE altice_aux_finalRoutes_beja AS(
  SELECT *
  FROM altice_new_finalRoutes_beja
  WHERE sequencenumber = 0
);

ALTER TABLE altice_aux_finalRoutes_beja ADD COLUMN city TEXT;
UPDATE altice_aux_finalRoutes_beja SET city='Beja';


DROP TABLE IF EXISTS altice_all_finalroutes;
CREATE TEMPORARY TABLE altice_all_finalroutes AS (
  SELECT *
  FROM altice_aux_finalRoutes_braga

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_setubal

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_aveiro

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_faro

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_leiria

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_viana_do_castelo

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_vila_real

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_viseu

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_santarem

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_guarda

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_portalegre

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_braganca

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_evora

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_castelo_branco

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_beja

);


DROP TABLE IF EXISTS altice_all_finalroutes;
CREATE TEMPORARY TABLE altice_all_finalroutes AS (
  SELECT *
  FROM altice_aux_finalRoutes_lisboa

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_porto

  UNION ALL

  SELECT *
  FROM altice_aux_finalRoutes_coimbra

);


-- === POSSIBLE AND FINAL ROUTES ==
  --  === HOME TO WORK; WORK TO HOME TRAJECTORIES; OVERALL === ----

  -- how many mobility combinations are possible: 15
  SELECT count(distinct transportmodes) FROM altice_all_possible_routes;
  SELECT DISTINCT ON (transportmodes) * FROM altice_all_possible_routes;


  -- how many unimodal possible routes: - 3: bus, driving, walking
  SELECT count(distinct transportmodes) FROM altice_all_possible_routes WHERE (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';
  SELECT DISTINCT ON (transportmodes) * FROM altice_all_possible_routes WHERE (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode4 = '';

  -- how many multimodal possible routes: - 12: ...
  SELECT count(distinct transportmodes) FROM altice_all_possible_routes WHERE (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 != '';
  SELECT DISTINCT ON (transportmodes) * FROM altice_all_possible_routes WHERE (transportmodes).mode1 IS NOT NULL AND (transportmodes).mode2 != '';


-- preparing all possible routes: we have to eliminate walking as a multimodal travel mode combination
UPDATE altice_all_possible_routes
SET transportmodes.mode1 = (transportmodes).mode2, transportmodes.mode2 = (transportmodes).mode3, transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 != '';

UPDATE altice_all_possible_routes
SET transportmodes.mode2 = (transportmodes).mode3, transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode2 = 'WALKING';

UPDATE altice_all_possible_routes
SET transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode3 = 'WALKING';

UPDATE altice_all_possible_routes
SET transportmodes.mode4 = ''
WHERE (transportmodes).mode4 = 'WALKING';


DROP TABLE IF EXISTS altice_prep_all_possible_routes;
CREATE TEMPORARY TABLE altice_prep_all_possible_routes AS (
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
  FROM altice_all_possible_routes
);


-- preparing all final routes: we have to eliminate walking as a multimodal travel mode combination
UPDATE altice_all_finalroutes
SET transportmodes.mode1 = (transportmodes).mode2, transportmodes.mode2 = (transportmodes).mode3, transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 != '';

UPDATE altice_all_finalroutes
SET transportmodes.mode2 = (transportmodes).mode3, transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode2 = 'WALKING';

UPDATE altice_all_finalroutes
SET transportmodes.mode3 = (transportmodes).mode4, transportmodes.mode4 = ''
WHERE (transportmodes).mode3 = 'WALKING';

UPDATE altice_all_finalroutes
SET transportmodes.mode4 = ''
WHERE (transportmodes).mode4 = 'WALKING';


DROP TABLE IF EXISTS altice_prep_all_final_routes;
CREATE TEMPORARY TABLE altice_prep_all_final_routes AS (
  SELECT *,   CASE
              WHEN (transportmodes).mode1 = 'DRIVING' AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode3 = '' THEN 1 -- and bicycling
              WHEN (transportmodes).mode1 = 'WALKING' AND (transportmodes).mode2 = '' AND (transportmodes).mode3 = '' AND (transportmodes).mode3 = '' THEN 1 -- and bicycling
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
  FROM altice_all_finalroutes
);


DROP TABLE IF EXISTS altice_possibleroutes_stats_typemode_commutingtype;
CREATE TEMPORARY TABLE altice_aux_possibleroutes_stats_typemode_commutingtype AS (
   SELECT city, commutingtype, sum(unimodal) AS freqUnimodal, count(*)-sum(unimodal) AS freqmultimodal, sum(public) AS freqPublic, sum(private) AS freqprivate, count(*) AS TOTAL
   FROM altice_prep_all_possible_routes
   GROUP BY city, commutingtype
);

DROP TABLE IF EXISTS altice_possibleroutes_stats_typemode_commutingtype;
CREATE TEMPORARY TABLE altice_possibleroutes_stats_typemode_commutingtype AS (
  SELECT city, commutingtype, CAST(sum(unimodal) AS FLOAT)*100/count(*) AS percentUnimodal, CAST(count(*)-sum(unimodal) AS FLOAT)*100/count(*) AS percentmultimodal, CAST(sum(public) AS FLOAT)*100/count(*) AS percentPublic, CAST(sum(private) AS FLOAT)*100/count(*) AS percentPrivate
  FROM altice_prep_all_possible_routes
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, 'H_W_H' AS commutingtype, CAST(sum(freqUnimodal) AS FLOAT)*100/sum(TOTAL) AS percentUnimodal, CAST(sum(freqmultimodal) AS FLOAT)*100/sum(TOTAL) AS percentMultimodal, CAST(sum(freqPublic) AS FLOAT)*100/sum(TOTAL) AS percentPublic, CAST(sum(freqprivate) AS FLOAT)*100/sum(TOTAL) AS percentPrivate
  FROM altice_aux_possibleroutes_stats_typemode_commutingtype t
  GROUP BY city
);


DROP TABLE IF EXISTS altice_aux_possibleroutes_stats_travel_modes_commutingtype;
CREATE TEMPORARY TABLE altice_aux_possibleroutes_stats_travel_modes_commutingtype AS (

  SELECT city, commutingtype, CAST(transportmodes AS TEXT), count(*) AS freq
  FROM altice_prep_all_possible_routes
  GROUP BY city, commutingtype, transportmodes

  UNION ALL

  SELECT city, commutingtype, 'BUS IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'BUS' OR (transportmodes).mode2 = 'BUS' OR (transportmodes).mode3 = 'BUS' OR (transportmodes).mode4 = 'BUS'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'COMMUTER_TRAIN IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'COMMUTER_TRAIN' OR (transportmodes).mode2 = 'COMMUTER_TRAIN' OR (transportmodes).mode3 = 'COMMUTER_TRAIN' OR (transportmodes).mode4 = 'COMMUTER_TRAIN'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'HEAVY_RAIL IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'HEAVY_RAIL' OR (transportmodes).mode2 = 'HEAVY_RAIL' OR (transportmodes).mode3 = 'HEAVY_RAIL' OR (transportmodes).mode4 = 'HEAVY_RAIL'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'TRAM IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'TRAM' OR (transportmodes).mode2 = 'TRAM' OR (transportmodes).mode3 = 'TRAM' OR (transportmodes).mode4 = 'TRAM'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'SUBWAY IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_possible_routes
  WHERE (transportmodes).mode1 = 'SUBWAY' OR (transportmodes).mode2 = 'SUBWAY' OR (transportmodes).mode3 = 'SUBWAY' OR (transportmodes).mode4 = 'SUBWAY'
  GROUP BY city, commutingtype

);

DROP TABLE IF EXISTS altice_possibleroutes_stats_travel_modes_city;
CREATE TEMPORARY TABLE altice_possibleroutes_stats_travel_modes_city AS (

  SELECT u.city, u.commutingtype, u.transportmodes, CAST(freq AS FLOAT)*100/total AS percentage
  FROM altice_aux_possibleroutes_stats_travel_modes_commutingtype u
  INNER JOIN (SELECT city,commutingtype , sum(freq) AS total
              FROM altice_aux_possibleroutes_stats_travel_modes_commutingtype
              GROUP BY city, commutingtype) p
  ON u.city = p.city
  AND u.commutingtype = p.commutingtype
  GROUP BY u.city, u.commutingtype,u.transportmodes, u.freq, total

  UNION ALL

  SELECT u.city, 'H_W_H' AS commutingtype, u.transportmodes, CAST(frequi AS FLOAT)*100/total AS percentage
  FROM altice_aux_possibleroutes_stats_travel_modes_commutingtype u
  INNER JOIN (SELECT city, sum(freq) AS total
              FROM altice_aux_possibleroutes_stats_travel_modes_commutingtype
              GROUP BY city) y
  ON u.city = y.city
  INNER JOIN (SELECT city, transportmodes, sum(freq) AS frequi
              FROM altice_aux_possibleroutes_stats_travel_modes_commutingtype
              GROUP BY city, transportmodes) p
  ON u.city = p.city
  AND u.transportmodes = p.transportmodes
  GROUP BY u.city, u.transportmodes,frequi, total

);


--- final routes ---
DROP TABLE IF EXISTS altice_aux_finalroutes_stats_typemode_commutingtype;
CREATE TEMPORARY TABLE altice_aux_finalroutes_stats_typemode_commutingtype AS (
   SELECT city, commutingtype, sum(unimodal) AS freqUnimodal, count(*)-sum(unimodal) AS freqmultimodal, sum(public) AS freqPublic, sum(private) AS freqprivate, count(*) AS TOTAL
   FROM altice_prep_all_final_routes
   GROUP BY city, commutingtype
);

DROP TABLE IF EXISTS altice_aux_finalroutes_stats_travel_modes_commutingtype;
CREATE TEMPORARY TABLE altice_aux_finalroutes_stats_travel_modes_commutingtype AS (

  SELECT city, commutingtype, CAST(transportmodes AS TEXT), count(*) AS freq
  FROM altice_prep_all_final_routes
  GROUP BY city, commutingtype, transportmodes

  UNION ALL

  SELECT city, commutingtype, 'BUS IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_final_routes
  WHERE (transportmodes).mode1 = 'BUS' OR (transportmodes).mode2 = 'BUS' OR (transportmodes).mode3 = 'BUS' OR (transportmodes).mode4 = 'BUS'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'COMMUTER_TRAIN IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_final_routes
  WHERE (transportmodes).mode1 = 'COMMUTER_TRAIN' OR (transportmodes).mode2 = 'COMMUTER_TRAIN' OR (transportmodes).mode3 = 'COMMUTER_TRAIN' OR (transportmodes).mode4 = 'COMMUTER_TRAIN'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'HEAVY_RAIL IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_final_routes
  WHERE (transportmodes).mode1 = 'HEAVY_RAIL' OR (transportmodes).mode2 = 'HEAVY_RAIL' OR (transportmodes).mode3 = 'HEAVY_RAIL' OR (transportmodes).mode4 = 'HEAVY_RAIL'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'TRAM IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_final_routes
  WHERE (transportmodes).mode1 = 'TRAM' OR (transportmodes).mode2 = 'TRAM' OR (transportmodes).mode3 = 'TRAM' OR (transportmodes).mode4 = 'TRAM'
  GROUP BY city, commutingtype

  UNION ALL

  SELECT city, commutingtype, 'SUBWAY IN GENERAL' AS transportmodes, count(*) AS freq
  FROM altice_prep_all_final_routes
  WHERE (transportmodes).mode1 = 'SUBWAY' OR (transportmodes).mode2 = 'SUBWAY' OR (transportmodes).mode3 = 'SUBWAY' OR (transportmodes).mode4 = 'SUBWAY'
  GROUP BY city, commutingtype

);


DROP TABLE IF EXISTS altice_finalroutes_stats_travel_modes_city;
CREATE TEMPORARY TABLE altice_finalroutes_stats_travel_modes_city AS (

  SELECT u.city, u.commutingtype, u.transportmodes, CAST(freq AS FLOAT)*100/total AS percentage
  FROM altice_aux_finalroutes_stats_travel_modes_commutingtype u
  INNER JOIN (SELECT city,commutingtype , sum(freq) AS total
              FROM altice_aux_finalroutes_stats_travel_modes_commutingtype
              GROUP BY city, commutingtype) p
  ON u.city = p.city
  AND u.commutingtype = p.commutingtype
  GROUP BY u.city, u.commutingtype,u.transportmodes, u.freq, total

  UNION ALL

  SELECT u.city, 'H_W_H' AS commutingtype, u.transportmodes, CAST(frequi AS FLOAT)*100/total AS percentage
  FROM altice_aux_finalroutes_stats_travel_modes_commutingtype u
  INNER JOIN (SELECT city, sum(freq) AS total
              FROM altice_aux_finalroutes_stats_travel_modes_commutingtype
              GROUP BY city) y
  ON u.city = y.city
  INNER JOIN (SELECT city, transportmodes, sum(freq) AS frequi
              FROM altice_aux_finalroutes_stats_travel_modes_commutingtype
              GROUP BY city, transportmodes) p
  ON u.city = p.city
  AND u.transportmodes = p.transportmodes
  GROUP BY u.city, u.transportmodes,frequi, total

);


DROP TABLE IF EXISTS altice_finalroutes_stats_typemode;
CREATE TABLE altice_finalroutes_stats_typemode AS (
  SELECT city, commutingtype, CAST(freqUnimodal AS FLOAT)*100/total AS percentUnimodal, CAST(freqmultimodal AS FLOAT)*100/total AS percentmultimodal, CAST(freqPublic AS FLOAT)*100/total AS percentPublic, CAST(freqprivate AS FLOAT)*100/total AS percentPrivate, '2' AS factor
  FROM altice_aux_finalroutes_stats_typemode_commutingtype
  GROUP BY city, commutingtype, freqUnimodal, freqmultimodal, freqPublic, freqprivate, total

  UNION ALL

  SELECT city, 'H_W_H' AS commutingtype, CAST(sum(freqUnimodal) AS FLOAT)*100/sum(TOTAL) AS percentUnimodal, CAST(sum(freqmultimodal) AS FLOAT)*100/sum(TOTAL) AS percentMultimodal, CAST(sum(freqPublic) AS FLOAT)*100/sum(TOTAL) AS percentPublic, CAST(sum(freqprivate) AS FLOAT)*100/sum(TOTAL) AS percentPrivate, '3' AS factor
  FROM altice_aux_finalroutes_stats_typemode_commutingtype t
  GROUP BY city

  ORDER BY city, factor DESC, commutingtype
);

DROP TABLE IF EXISTS altice_finalroutes_stats_travel_modes;
CREATE TABLE altice_finalroutes_stats_travel_modes AS (
  SELECT v.city, v.commutingtype, v.transportmodes, coalesce(percentage,0)
  FROM (SELECT * FROM altice_finalroutes_stats_travel_modes_city) y
  RIGHT JOIN (SELECT city, commutingtype, transportmodes
              FROM (SELECT DISTINCT ON (city, commutingtype) city, commutingtype FROM altice_finalroutes_stats_travel_modes_city) y
              CROSS JOIN (SELECT DISTINCT ON (transportmodes) transportmodes FROM altice_aux_finalroutes_stats_travel_modes_commutingtype) h) v
  ON y.city = v.city
  AND y.commutingtype = v.commutingtype
  AND y.transportmodes = v.transportmodes

  ORDER BY city, v.commutingtype, v.transportmodes
);


DROP TABLE IF EXISTS altice_aux_finalroutes_stats_duration_commutingtype;
CREATE TEMPORARY TABLE altice_aux_finalroutes_stats_duration_commutingtype AS (
   SELECT *
   FROM altice_all_final_routes

);

DROP TABLE IF EXISTS altice_prep_all_final_routes;
CREATE TEMPORARY TABLE altice_prep_all_final_routes AS (
  SELECT userid, city, commutingtype, duration,   CASE
                                                      WHEN duration < 960 THEN 1 -- até 15 mins
                                                      ELSE 0
                                                  END AS "less than 15mins", CASE
                                                                                  WHEN duration >= 960 AND duration < 1800 THEN 1 -- de 16 a 30 mins
                                                                                  ELSE 0
                                                                              END AS "16minsto30mins",
                                                                                                        CASE
                                                                                                            WHEN duration >= 1800 AND duration < 3600 THEN 1  -- de 31 a 60 mins
                                                                                                            ELSE 0
                                                                                                        END AS "31minsto60mins", CASE
                                                                                                                                        WHEN duration >= 3600 AND duration < 5400 THEN 1  -- de 61 a 90 mins
                                                                                                                                        ELSE 0
                                                                                                                                    END AS "61minsto90mins",
                                                                                                                                                              CASE
                                                                                                                                                                  WHEN duration >= 5400 THEN 1  -- de mais de 90 mins
                                                                                                                                                                  ELSE 0
                                                                                                                                                              END AS "morethan90"
  FROM altice_all_finalroutes
);

DROP TABLE IF EXISTS altice_aux_finalroutes_stats_durations_commutingtype;
CREATE TEMPORARY TABLE altice_aux_finalroutes_stats_durations_commutingtype AS (
   SELECT city, commutingtype, sum("less than 15mins") AS "freq less than 15mins", sum("16minsto30mins") AS "freq 16minsto30mins", sum("31minsto60mins") AS "freq 31minsto60mins", sum("61minsto90mins") AS "freq 61minsto90mins", sum("morethan90") AS "freq morethan90", count(*) AS TOTAL
   FROM altice_prep_all_final_routes
   GROUP BY city, commutingtype
);

DROP TABLE IF EXISTS altice_finalroutes_stats_durations;
CREATE TABLE altice_finalroutes_stats_durations AS (
  SELECT city, commutingtype, CAST("freq less than 15mins" AS FLOAT)*100/total AS "percent less than 15mins", CAST("freq 16minsto30mins" AS FLOAT)*100/total AS "percent 16minsto30mins", CAST("freq 31minsto60mins" AS FLOAT)*100/total AS "percent 31minsto60mins", CAST("freq 61minsto90mins" AS FLOAT)*100/total AS "percent 61minsto90mins", CAST("freq morethan90" AS FLOAT)*100/total AS "percent morethan90", '2' AS factor
  FROM altice_aux_finalroutes_stats_durations_commutingtype
  GROUP BY city, commutingtype, "percent less than 15mins", "percent 16minsto30mins", "percent 31minsto60mins", "percent 61minsto90mins", "percent morethan90", total

  UNION ALL

  SELECT city, 'H_W_H' AS commutingtype, CAST(sum("freq less than 15mins") AS FLOAT)*100/sum(TOTAL) AS "percent less than 15mins", CAST(sum("freq 16minsto30mins") AS FLOAT)*100/sum(TOTAL) AS "percent 16minsto30mins", CAST(sum("freq 31minsto60mins") AS FLOAT)*100/sum(TOTAL) AS "percent 31minsto60mins", CAST(sum("freq 61minsto90mins") AS FLOAT)*100/sum(TOTAL) AS "percent 61minsto90mins", CAST(sum("freq morethan90") AS FLOAT)*100/sum(TOTAL) AS "percent morethan90", '3' AS factor
  FROM altice_aux_finalroutes_stats_durations_commutingtype t
  GROUP BY city

  ORDER BY city, factor DESC, commutingtype
);


SELECT count(*) FROM (SELECT DISTINCT ON (transportmodes) * FROM altice_all_finalroutes WHERE commutingtype = 'W_H')r;
SELECT count(*) FROM (SELECT DISTINCT ON (transportmodes) * FROM altice_finalroutes_coimbra) r;


SELECT * FROM altice_finalroutes_stats_typemode;


SELECT * FROM altice_final_eligibleUsers_byMunicipal

