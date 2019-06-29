--start off with some exploratory analysis

--question: is vehicle_tag, report_time an alternate key for the table?
SELECT vehicle_tag, report_time, count(*)
FROM sfmta_avl
GROUP BY vehicle_tag, report_time
HAVING count(*) > 1
--answer: found many, many rows with count=2
--need to have it unique so we can join on this pair

--delete dups:
DELETE FROM sfmta_avl a
USING sfmta_avl b
WHERE a.reading_id < b.reading_id
  AND a.vehicle_tag = b.vehicle_tag
  AND a.report_time = b.report_time;

--question: bounds on speed
SELECT min(speed), max(speed) FROM sfmta_avl;
-- min |  max
-------+--------
--   0 | 99.722
--not sure what the units are, but seems reasonable

-- first idea for stopped vehicles: window function query
SELECT reading_id, max(report_time) OVER (PARTITION BY vehicle_tag WHERE speed > 0)
FROM sfmta_avl
WHERE speed = 0
--except it doesn't look like a "WHERE" is supported inside the OVER, which is what I need

-- pseudocode:
-- for a given row with vehicle_tag vt and report_time rt and speed = 0
-- select max(report_time) from sfmta_avl where report_time < rt and vehicle_tag = vt and speed > 0

SELECT stpd.vt, stpd.rt, max(mvng.report_time)
FROM (
 SELECT vehicle_tag vt, report_time rt
 FROM sfmta_avl
 WHERE speed = 0
) stpd 
JOIN sfmta_avl mvng ON stpd.vt = mvng.vehicle_tag AND stpd.rt > mvng.report_time
WHERE mvng.speed > 0
GROUP BY stpd.vt, stpd.rt

--try the above for a single vehicle_tag
CREATE TABLE veh_j2836
AS
SELECT stpd.rt AS stopped_at, max(mvng.report_time) AS stopped_since
FROM (
 SELECT report_time rt
 FROM sfmta_avl
 WHERE speed = 0
 AND vehicle_tag = 'J2836'
) stpd 
JOIN sfmta_avl mvng ON stpd.rt > mvng.report_time
WHERE mvng.speed > 0
AND vehicle_tag = 'J2836'
GROUP BY stpd.rt
ORDER BY stpd.rt;

--function to calculate the difference in seconds
CREATE OR REPLACE FUNCTION TimeDiff (start_t timestamp, end_t timestamp) 
RETURNS integer AS $diffrnc$

DECLARE ts_diff interval := end_t - start_t;
DECLARE diffrnc integer;

BEGIN
    SELECT ((DATE_PART('day', ts_diff) * 24 + 
           DATE_PART('hour', ts_diff)) * 60 +
           DATE_PART('minute', ts_diff)) * 60 +
           DATE_PART('second', ts_diff) INTO diffrnc;
    RETURN diffrnc;
END; 
$diffrnc$ LANGUAGE plpgsql;

SELECT stopped_at, stopped_since, TimeDiff(stopped_since, stopped_at)
FROM veh_j2836

--add calculated elapsed time
ALTER TABLE veh_j2836
ADD COLUMN stopped_for integer;

UPDATE veh_j2836
SET stopped_for = TimeDiff(stopped_since, stopped_at);

WITH stop_ends AS (
  SELECT report_time, speed, lead(speed, 1) OVER (ORDER BY report_time) AS next_speed 
  FROM sfmta_avl
  WHERE vehicle_tag = 'J2836')
SELECT report_time
FROM stop_ends
WHERE speed = 0
AND ;

--new query to identify the stops (max length)
CREATE TABLE veh_j2836
AS
SELECT stpd.rt AS stopped_at, max(mvng.report_time) AS stopped_since
FROM (
 SELECT report_time rt
 FROM (SELECT report_time, speed, lead(speed, 1) OVER (ORDER BY report_time) AS next_speed 
  FROM sfmta_avl
  WHERE vehicle_tag = 'J2836') AS stop_ends
 WHERE speed = 0
 AND next_speed > 0
) AS stpd 
JOIN sfmta_avl mvng ON stpd.rt > mvng.report_time
WHERE mvng.speed > 0
AND vehicle_tag = 'J2836'
GROUP BY stpd.rt
ORDER BY stpd.rt;

--add calculated elapsed time
ALTER TABLE veh_j2836
ADD COLUMN stopped_for integer;

UPDATE veh_j2836
SET stopped_for = TimeDiff(stopped_since, stopped_at);

--now join with main table to get lat,lon
SELECT sa.report_time, sa.latitude, sa.longitude, vj.stopped_for
FROM sfmta_avl AS sa
JOIN veh_j2836 AS vj
ON sa.report_time = vj.stopped_at
WHERE sa.vehicle_tag = 'J2836'
ORDER BY vj.stopped_for DESC;

--results:
     report_time     |  latitude  | longitude | stopped_for
---------------------+------------+-----------+-------------
 2014-06-20 08:27:02 | -122.40491 |  37.70932 |        1080
 2014-06-20 06:54:02 | -122.44704 |  37.72112 |         848
 2014-06-20 05:22:32 | -122.40486 |  37.70935 |         540
 2014-06-20 05:55:32 |  -122.3942 |  37.77654 |         362
 2014-06-20 05:12:02 | -122.40548 |  37.70822 |         192
 2014-06-20 09:01:32 | -122.39357 |  37.77607 |         180
 2014-06-20 07:15:07 | -122.46558 |  37.74116 |         172
 2014-06-20 09:43:32 | -122.45453 |  37.72364 |          90
 2014-06-20 09:34:32 | -122.47134 |  37.73538 |          87
 2014-06-20 07:39:41 | -122.39381 |  37.77653 |          82
 2014-06-20 07:34:32 | -122.38865 |  37.78984 |          80
 2014-06-20 08:45:02 | -122.38784 |  37.74303 |          71
 2014-06-20 04:57:02 | -122.38695 |  37.75041 |          66
 2014-06-20 07:03:02 | -122.46205 |  37.72524 |          59
 2014-06-20 06:36:02 | -122.45841 |  37.72436 |          51
 2014-06-20 07:46:32 |  -122.3889 |  37.76451 |          51
 2014-06-20 06:30:02 | -122.47152 |  37.73524 |          50
 2014-06-20 05:27:02 | -122.39548 |  37.72284 |          49
 2014-06-20 08:03:02 | -122.39559 |   37.7227 |          48
 2014-06-20 08:43:32 | -122.38892 |  37.73987 |          44
 2014-06-20 09:07:32 | -122.38948 |  37.79045 |          44
 2014-06-20 07:51:02 |  -122.3876 |   37.7507 |          44
 2014-06-20 05:58:32 | -122.39234 |  37.77812 |          43
 2014-06-20 07:57:02 | -122.38986 |  37.73746 |          41
 2014-06-20 08:40:32 | -122.39063 |  37.73502 |          40
 2014-06-20 08:07:32 | -122.40496 |  37.70936 |          40
 2014-06-20 08:28:32 | -122.40213 |  37.71228 |          40
 2014-06-20 06:27:02 | -122.46602 |  37.74147 |          38
 2014-06-20 09:30:02 | -122.46559 |   37.7414 |          38
 2014-06-20 06:04:32 | -122.39085 |  37.79199 |          37
 2014-06-20 09:39:02 | -122.46465 |  37.72621 |          36
 2014-06-20 05:31:32 | -122.39167 |  37.73217 |          34
 2014-06-20 07:37:32 | -122.39019 |  37.77946 |          34
 2014-06-20 08:51:02 | -122.38867 |  37.76084 |          32
 2014-06-20 05:28:32 | -122.39411 |  37.72594 |          31
 2014-06-20 08:55:32 | -122.38935 |  37.77027 |          25
 2014-06-20 09:45:02 | -122.45127 |  37.72307 |          24
 2014-06-20 08:30:02 | -122.39931 |  37.71472 |          24
 2014-06-20 08:42:02 | -122.38982 |  37.73743 |          24
 2014-06-20 09:36:02 | -122.47178 |  37.73148 |          22
 2014-06-20 07:06:02 | -122.46924 |  37.72995 |          21
 2014-06-20 08:49:32 | -122.38807 |  37.75582 |          20
 2014-06-20 08:57:02 | -122.38969 |  37.77316 |          18
 2014-06-20 07:09:02 | -122.47157 |  37.73452 |          17
 2014-06-20 07:36:19 | -122.38819 |  37.78365 |          17
 2014-06-20 06:28:32 | -122.46913 |  37.73823 |          15
 2014-06-20 05:43:32 |  -122.3887 |  37.76099 |          15
 2014-06-20 07:43:32 | -122.38972 |  37.77257 |          13
 2014-06-20 07:58:32 | -122.39156 |   37.7326 |          13
 2014-06-20 08:46:32 | -122.38744 |  37.74835 |          11
 2014-06-20 05:48:02 | -122.38987 |  37.77435 |          11
(51 rows)

--could I plot these on a map?
--remove the ones that are near stops (this will require the GIS features)

select count(*) from stops;

--create a helper table for all vehicles
CREATE TABLE stop_times
AS
SELECT stpd.vehicle_tag, stpd.rt AS stopped_at, max(mvng.report_time) AS stopped_since
FROM (
 SELECT vehicle_tag, report_time rt
 FROM (SELECT vehicle_tag, report_time, speed, lead(speed, 1) OVER (PARTITION BY vehicle_tag ORDER BY report_time) AS next_speed 
  FROM sfmta_avl) AS stop_ends
 WHERE speed = 0
 AND next_speed > 0
) AS stpd 
JOIN sfmta_avl mvng ON stpd.vehicle_tag = mvng.vehicle_tag AND stpd.rt > mvng.report_time
WHERE mvng.speed > 0
GROUP BY stpd.vehicle_tag, stpd.rt;

ALTER TABLE stop_times
ADD COLUMN stopped_for integer;

UPDATE stop_times
SET stopped_for = TimeDiff(stopped_since, stopped_at);

-- now do queries on this, stop_times:
-- vehicle tag | time it was stopped | last time it was moving | stopped for

--this is the query I need to parametrize for the webapp:
SELECT st.vehicle_tag, st.stopped_at, st.stopped_for, sa.latitude, sa.longitude
FROM stop_times st
JOIN sfmta_avl sa ON st.vehicle_tag = sa.vehicle_tag AND st.stopped_at = sa.report_time
WHERE st.stopped_at BETWEEN '2014-06-20 06:50:00' AND '2014-06-20 06:55:00'
ORDER BY st.stopped_for DESC

--
--function sorts the vehicles by length of time stopped, over the last few minutes
--
CREATE OR REPLACE FUNCTION list_stopped_vehicles(query_time timestamp, look_back integer)
RETURNS TABLE (veh_tag varchar(50), train_asng varchar(50), stopped_at timestamp, stopped_for integer, lat double precision, lon double precision)
AS $$

DECLARE begin_query_time timestamp := (query_time - make_interval(mins => look_back));

BEGIN

DROP TABLE IF EXISTS temp_stopped;
DROP TABLE IF EXISTS temp_moving;
DROP TABLE IF EXISTS temp_stop_times;
    
RAISE NOTICE 'querying for stopped vehicles within % minutes of %', look_back, query_time;

CREATE TEMPORARY TABLE IF NOT EXISTS temp_stopped
AS
SELECT vehicle_tag, report_time
FROM
( SELECT vehicle_tag, report_time, speed, lead(speed, 1) OVER (PARTITION BY vehicle_tag ORDER BY report_time) AS next_speed 
  FROM sfmta_avl
  WHERE report_time BETWEEN begin_query_time AND query_time
) AS startup_info
WHERE speed = 0
AND next_speed > 0;

RAISE NOTICE 'found records of ends of vehicle stops';

CREATE TEMPORARY TABLE IF NOT EXISTS temp_moving
AS
SELECT vehicle_tag, report_time
FROM sfmta_avl
WHERE report_time BETWEEN begin_query_time AND query_time
AND speed > 0;

RAISE NOTICE 'found records of moving vehicles';

CREATE TEMPORARY TABLE IF NOT EXISTS temp_stop_times
AS
SELECT stpd.vehicle_tag, stpd.report_time AS stopped_at, max(mvng.report_time) AS stopped_since
FROM temp_stopped stpd
JOIN temp_moving mvng ON stpd.vehicle_tag = mvng.vehicle_tag 
WHERE stpd.report_time > mvng.report_time
GROUP BY stpd.vehicle_tag, stpd.report_time;

RAISE NOTICE 'calculating length of stops';

ALTER TABLE temp_stop_times ADD COLUMN stopped_for integer;

UPDATE temp_stop_times SET stopped_for = TimeDiff(temp_stop_times.stopped_since, temp_stop_times.stopped_at);

--DROP TABLE temp_stopped;
--DROP TABLE temp_moving;
--DROP TABLE temp_stop_times;

RAISE NOTICE 'joining with main table to obtain other columns';

RETURN QUERY
SELECT st.vehicle_tag AS veh_tag, avl.train_assignment AS train_asng, st.stopped_at, st.stopped_for, avl.latitude, avl.longitude
FROM temp_stop_times st
JOIN sfmta_avl avl ON st.vehicle_tag = avl.vehicle_tag AND st.stopped_at = avl.report_time
WHERE avl.report_time BETWEEN begin_query_time AND query_time
ORDER BY st.stopped_for DESC;

END; 
$$ LANGUAGE plpgsql;
--
--END list_stopped_vehicles
--

SELECT list_stopped_vehicles('2014-01-04 10:05:00', 5);

--Spark creates this table after its processing:
sfmta_stops(report_time, vehicle_tag, stopped_for)

--how many stops per day on average?
ALTER TABLE sfmta_stops ADD COLUMN hour_of_day integer;

UPDATE sfmta_stops SET hour_of_day = date_part('hour', report_time);

SELECT hour_of_day, count(*), avg(stopped_for)
FROM sfmta_stops_10 --changer
GROUP BY hour_of_day
ORDER BY hour_of_day;

--to speed up joins
CREATE INDEX sfmta_stops_hour_idx ON sfmta_stops(hour_of_day);
CREATE INDEX sfmta_stops_time_idx ON sfmta_stops(report_time);
CREATE INDEX sfmta_stops_vehicle_idx ON sfmta_stops(vehicle_tag);
CREATE INDEX sfmta_stops_length_idx ON sfmta_stops(stopped_for);

CREATE TABLE sfmta_stops_det AS
SELECT sfs.report_time, sfs.vehicle_tag, sfs.stopped_for, sfs.hour_of_day, avl.latitude, avl.longitude
FROM sfmta_stops sfs
JOIN sfmta_avl avl ON sfs.vehicle_tag = avl.vehicle_tag AND sfs.report_time = avl.report_time;

--to speed up webapp queries
CREATE INDEX sfmta_stops_det_hour_idx ON sfmta_stops_det(hour_of_day);
CREATE INDEX sfmta_stops_det_time_idx ON sfmta_stops_det(report_time);
CREATE INDEX sfmta_stops_det_vehicle_idx ON sfmta_stops_det(vehicle_tag);
CREATE INDEX sfmta_stops_det_length_idx ON sfmta_stops_det(stopped_for);

--get data for 4pm
SELECT vehicle_tag, report_time, stopped_for, latitude, longitude
FROM sfmta_stops_det
WHERE hour_of_day = 16
AND stopped_for > 600;

SELECT st.vehicle_tag AS veh_tag, avl.train_assignment AS train_asng, st.stopped_at, st.stopped_for, avl.latitude, avl.longitude