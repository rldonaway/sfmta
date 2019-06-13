#!/bin/sh
# inserts data from a CSV file into postgres

# steps...
# 1. get filename
# 2. download file from S3
# 3. clean file as above
# 4. copy into postgres
# 5. append filename to log
# 6. delete file

insert_file_into_db()
{
  FILENAME=$1
  echo "Processing file $FILENAME..."
  aws s3 cp s3://insight-donaway-sfmta-data/$FILENAME myfile.csv
  sed -i '/^$/d' myfile.csv
  sed -i 's/REV,REPORT_TIME,VEHICLE_TAG,LONGITUDE,LATITUDE,SPEED,HEADING,TRAIN_ASSIGNMENT,PREDICTABLE//g' myfile.csv
  /usr/pgsql-9.5/bin/psql -h 127.0.0.1 -d sfmta -U insight -p 5217 -c "\copy sfmta_avl_stg(rev,report_time,vehicle_tag,longitude,latitude,speed,heading,train_assignment,predictable) FROM 'myfile.csv' WITH CSV" 
  echo "Processed file $FILENAME" >> file-log.txt
}

# maybe there is a problem with the line endings? take the real file and remove all but the first two lines
#when I copied them there was no problem
# try putting into pandas and seeing if that has a problem

rm -f file-log.txt

# List of files to process
echo "Start of script..."

insert_file_into_db sfmtaAVLRawData01012013.csv

echo "End of script..."
