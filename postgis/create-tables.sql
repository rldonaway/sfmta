CREATE TABLE sfmta_avl_stg
( rev              VARCHAR(12)
, report_time      VARCHAR(20)
, vehicle_tag      VARCHAR(20)
, longitude        FLOAT
, latitude         FLOAT
, speed            FLOAT
, heading          FLOAT
, train_assignment VARCHAR(12)
, predictable      VARCHAR(5)
);



--check this in

CREATE ROLE sfmta;
GRANT sfmta TO insight;
GRANT ALL PRIVILEGES ON sfmta_avl TO sfmta
GRANT ALL PRIVILEGES ON sfmta_avl_stg TO sfmta

--  first try
\copy sfmta_avl(vehicle_tag,report_time,longitude,latitude,train_assignment,is_predictable,last_timepoint) FROM 'sfmtaAVLRawData01012013.csv' DELIMITER ',' CSV HEADER

\copy sfmta_avl_stg(rev,report_time,vehicle_tag,longitude,latitude,speed,heading,train_assignment,predictable) FROM 'sfmtaAVLRawData01012013.csv' DELIMITER ',' CSV HEADER


-- clean files
sed -i '/^$/d' sfmtaAVLRawData01012013.csv
sed -i 's/REV,REPORT_TIME,VEHICLE_TAG,LONGITUDE,LATITUDE,SPEED,HEADING,TRAIN_ASSIGNMENT,PREDICTABLE//g' sfmtaAVLRawData01012013.csv

--didn't work... read data from Spark and then saved in postgres

--describe table
\d sfmta_avl_stg

-- converting text to data types...
SELECT CAST(speed AS FLOAT) FROM sfmta_avl_stg LIMIT 10;
SELECT to_timestamp(report_time, 'MM/DD/YYYY HH24:MI:SS')::timestamp without time zone FROM sfmta_avl_stg LIMIT 10;
SELECT ST_POINT(latitude::double precision, longitude::double precision) FROM sfmta_avl_stg LIMIT 10;
SELECT POINT(latitude::double precision, longitude::double precision) FROM sfmta_avl_stg LIMIT 10;

---no...
--quote: If your data is geographically compact (contained within a state, county or city), 
---use the geometry type with a Cartesian projection that makes sense with your data. 
---See the http://spatialreference.org site and type in the name of your region for a selection of possible reference systems.

--example of creating a table with geography from one with geometry
CREATE TABLE nyc_subway_stations_geog AS
SELECT
  Geography(ST_Transform(geom,4326)) AS geog,
  name,
  routes
FROM nyc_subway_stations;

--create an index on the geog column
CREATE INDEX nyc_subway_stations_geog_gix
ON nyc_subway_stations_geog USING GIST (geog);

sfmta=> select min(latitude), max(latitude), min(longitude), max(longitude) from sfmta_avl_stg;
   min    |   max    | min |    max
----------+----------+-----+-----------
 24.03971 | 85.58866 | 0.0 | -99.99997
(1 row)

--whoops need to take care of those out of bounds values
-- but first let's get the data into the regular table

CREATE TABLE sfmta_avl
( reading_id       bigserial PRIMARY KEY
, rev              varchar(50)
, report_time      timestamp
, vehicle_tag      varchar(50)
, speed            float
, heading          float
, train_assignment varchar(50)
, predictable      varchar(50)
, latitude         double precision
, longitude        double precision
, located_at       geography(point)
);
-- after data is inserted, SET located_at = ST_POINT(latitude, longitude);

INSERT INTO sfmta_avl(rev, report_time, vehicle_tag, latitude, longitude, speed, heading, train_assignment, predictable)
SELECT 
SUBSTRING (rev FROM 0 FOR 50),
to_timestamp(report_time, 'MM/DD/YYYY HH24:MI:SS')::timestamp without time zone,
SUBSTRING (vehicle_tag FROM 0 FOR 50),
CASE WHEN longitude = '' THEN null
    ELSE longitude::double precision
    END,
CASE WHEN latitude = '' THEN null
    ELSE latitude::double precision
    END,
CASE WHEN speed = '' THEN null
    ELSE speed::float
    END,
CASE WHEN heading = '' THEN null
    ELSE heading::float
    END,
SUBSTRING (train_assignment FROM 0 FOR 50),
SUBSTRING (predictable FROM 0 FOR 50)
FROM sfmta_avl_stg;

--INSERT 0 1456337028

CREATE INDEX sfmta_avl_report_time_idx ON sfmta_avl(report_time);
CREATE INDEX sfmta_avl_vehicle_tag_idx ON sfmta_avl(vehicle_tag);
CREATE INDEX sfmta_avl_speed_idx ON sfmta_avl(speed);
CREATE INDEX sfmta_avl_train_assignment_idx ON sfmta_avl(train_assignment);

can do now --import data into other tables that are related to sfmta_avl
after indexes --create materialized view for list of vehicles
--create materialized view for list of train assignments 
CREATE MATERIALIZED VIEW view_name
AS
query
WITH DATA;

--window function query

select reading_id, max(report_time) over (partition by vehicle_tag where speed > 0)
from sfmta_avl
where speed = 0

--doesn't look like a where is supported

for a given row with vehicle_tag vt and report_time rt and speed = 0
select max(report_time) from sfmta_avl where report_time < rt and vehicle_tag = vt and speed > 0
 --subquery?

select stpd.vt, stpd.rt, max(mvng.report_time)
from (
 select vehicle_tag vt, report_time rt
 from sfmta_avl
 where speed = 0
) stpd 
join sfmta_avl mvng on stpd.vt = mvng.vehicle_tag and stpd.rt > mvng.report_time and mvng.speed > 0

--question: is vehicle_tag, report_time an alternate key for the table?

--so what we have, for each time a vehicle is stopped, is the last time it was moving (LMT)
--so for any time a vehicle is stopped, we can calculate the elapsed time between the LMT and the current time. this is the length of time stopped.

--- put length of time stopped in a sep. column or table?
