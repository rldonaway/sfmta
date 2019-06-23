-- created from Spark dataframe...
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

--whoops need to take care of those out of bounds values... use the bus stop locations as a guide
select min(stop_lat), max(stop_lat), min(stop_lon), max(stop_lon) from sfmta_stops;
    min    |    max    |    min     |    max
-----------+-----------+------------+------------
 37.705764 | 37.836443 | -122.53867 | -122.36633

--put data into perm table
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
-- if needed, add column located_at       geography(point) [or geometrty?]
-- then SET located_at = ST_POINT(latitude, longitude);

CREATE INDEX sfmta_avl_report_time_idx ON sfmta_avl(report_time);
CREATE INDEX sfmta_avl_vehicle_tag_idx ON sfmta_avl(vehicle_tag);
CREATE INDEX sfmta_avl_speed_idx ON sfmta_avl(speed);
CREATE INDEX sfmta_avl_train_assignment_idx ON sfmta_avl(train_assignment);

CREATE MATERIALIZED VIEW vehicles
AS
SELECT DISTINCT vehicle_tag FROM sfmta_avl;

CREATE MATERIALIZED VIEW train_assignments
AS
SELECT DISTINCT train_assignment FROM sfmta_avl;

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

REFRESH MATERIALIZED VIEW vehicles WITH DATA;
REFRESH MATERIALIZED VIEW train_assignments WITH DATA;
