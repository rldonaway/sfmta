/*
This script was used to read location data CSV files from S3, put them into a dataframe, clean the data, and insert
into PostgreSQL. These are the detailed steps:
     1. Set up database connection properties
     2. Declare a cleaning function for header rows
     3. Read entire collection of CSV files from S3 into a dataframe with one column containing a String
     4. Remove blank lines
     5. Remove the header information from the beginning of affected rows
     6. Convert the one column containing comma separated values into 9 separate columns
     7. Drop the temporary columns
     8. Copy the dataframe into a new table in Postgres
 */

// set up properties for connecting to database
import java.util.Properties
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

val spark: SparkSession = SparkSession.active // no need for this when running in the Spark shell
import spark.implicits._ // this is how $ can work when calling select

val connectionProperties = new Properties()
connectionProperties.setProperty("Driver", "org.postgresql.Driver")
connectionProperties.setProperty("user", "insight")
connectionProperties.setProperty("password", "xxxx") // fill in password at runtime only

/**
  * Processes a column to remove the CSV header.
  * Each imported file has a first row that starts off with the header and is followed by the first row of actual data
  * for the file. We don't need the header at this point, but we do want the first row of data. Uses a regular
  * expression to find and remove this, leaving the actual data.
  * @param col the column to be processed
  * @return a column without the header
  */
def removeHeader(col: Column): Column = {
  val header = "REV,REPORT_TIME,VEHICLE_TAG,LONGITUDE,LATITUDE,SPEED,HEADING,TRAIN_ASSIGNMENT,PREDICTABLE"
  regexp_replace(col, header, "")
}

val rawfile = spark.read.text("s3a://insight-donaway-sfmta-data/sfmtaAVLRawData*.csv")
val noblanks = rawfile.filter("value != ''")
val cleaned = noblanks.withColumn("clean_value", removeHeader(col("value")))
val expanded = cleaned.withColumn("_list", split($"clean_value", ",")).select(
  $"_list".getItem(0).as("rev"),
  $"_list".getItem(1).as("report_time"),
  $"_list".getItem(2).as("vehicle_tag"),
  $"_list".getItem(3).as("longitude"),
  $"_list".getItem(4).as("latitude"),
  $"_list".getItem(5).as("speed"),
  $"_list".getItem(6).as("heading"),
  $"_list".getItem(7).as("train_assignment"),
  $"_list".getItem(8).as("predictable")
).drop("_list")
val tosave = expanded.drop("value")

val db_url = "jdbc:postgresql://10.0.0.24:5217/sfmta"
tosave.write.mode(SaveMode.Overwrite).jdbc(db_url,"sfmta_avl_stg", connectionProperties)
// the table sfmta_avl_stg is now populated in the database

// now read the tables with relationships to sfmta_avl
import org.apache.spark.sql.types._

val cols = Seq("stop_lat", "stop_code", "stop_lon", "stop_url", "stop_id", "stop_desc", "stop_name", "location_type",
  "zone_id")
val doubleCols = Set("stop_lat", "stop_lon")

val schema = StructType(cols.map(
  c => StructField(c, if (doubleCols contains c) DoubleType else StringType)
))

val stops = spark.read.schema(schema).csv("s3a://insight-donaway-sfmta-join-data/stops.txt")
stops.write.mode(SaveMode.Overwrite).jdbc(db_url,"sfmta_stops", connectionProperties)

val cols_t = Seq("block_id", "route_id", "original_trip_id", "direction_id", "trip_headsign", "shape_id", "service_id",
  "trip_id")
val schema_t = StructType(cols_t.map(c => StructField(c, StringType)))
val trips = spark.read.schema(schema_t).csv("s3a://insight-donaway-sfmta-join-data/stops.txt")
trips.write.mode(SaveMode.Overwrite).jdbc(db_url,"sfmta_trips", connectionProperties)

