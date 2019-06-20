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

--question: bounds on lat/lon and are they way outside the maxes and mins for the scheduled stops
--37.705764 | 37.836443 | -122.53867 | -122.36633
DELETE FROM sfmta_avl
WHERE latitude < 37.7
OR latitude > 37.9
OR longitude < -122.6
OR longitude > -122.3;

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
CREATE OR REPLACE FUNCTION TimeDiff (start_t TIMESTAMP, end_t TIMESTAMP) 
RETURNS integer AS $diffrnc$
declare
    diffrnc integer;
BEGIN
    SELECT ((DATE_PART('day', end_t - start_t) * 24 + 
           DATE_PART('hour', end_t - start_t)) * 60 +
           DATE_PART('minute', end_t - start_t)) * 60 +
           DATE_PART('second', end_t - start_t) INTO diffrnc;
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



--so what we have, for each time a vehicle is stopped, is the last time it was moving (LMT)
--so for any time a vehicle is stopped, we can calculate the elapsed time between the LMT and the current time. this is the length of time stopped.

--- put length of time stopped in a sep. column or table?
