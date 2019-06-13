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

CREATE TABLE sfmta_avl
( reading_id       BIGSERIAL PRIMARY KEY
, rev              VARCHAR(12)
, report_time      TIMESTAMP
, vehicle_tag      VARCHAR(12)
, speed            FLOAT
, heading          FLOAT
, train_assignment VARCHAR(12)
, predictable      VARCHAR(5)
, located_at       GEOGRAPHY
);

-- when creating main table, SET located_at = ST_POINT(latitude, longitude);


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




