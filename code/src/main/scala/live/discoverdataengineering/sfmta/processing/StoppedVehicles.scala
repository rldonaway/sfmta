package live.discoverdataengineering.sfmta.processing

import java.sql.Timestamp
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.log4j.LogManager
import org.apache.spark.storage.StorageLevel

/**
  * Calculates the times any vehicle was stopped and for how long. Writes results to the database in table sfmta_stops.
  */
object StoppedVehicles {

  @transient lazy val log = LogManager.getLogger("live.discoverdataengineering.sfmta.processing.StoppedVehicles")

  /*
  running...
  (sh /usr/local/spark/sbin/start-all.sh; cd ~)
  /usr/local/spark/bin/spark-submit --class "live.discoverdataengineering.sfmta.processing.StoppedVehicles" --properties-file insight.conf --master spark://ip-10-0-0-20.us-west-2.compute.internal:7077 --deploy-mode cluster --conf "spark.network.timeout=10000000" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/home/ubuntu/log4j.properties" --jars /usr/local/spark/lib/postgresql-42.2.5.jar sfmta.jar
  /usr/local/spark/bin/spark-submit --class "live.discoverdataengineering.sfmta.processing.StoppedVehicles" --properties-file insight.conf --master spark://ip-10-0-0-20.us-west-2.compute.internal:7077 --deploy-mode cluster --conf "spark.network.timeout=10000000" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/home/ubuntu/log4j.properties" --jars /usr/local/spark/lib/postgresql-42.2.5.jar sfmta.jar "2013-01-01 00:00:00" "2013-01-16 00:00:00"
  settings to try:
  --driver-memory 30G --executor-memory 14G --num-executors 7 --executor-cores 8 --conf spark.driver.maxResultSize=4g --conf spark.executor.heartbeatInterval=10000000
  --executor-cores 5 means that each executor can run a maximum of five tasks at the same time

Assume there are 6 nodes available on a cluster with 25 core nodes and 125 GB memory per node
A recommended approach when using YARN would be to use - -num-executors 30 - -executor-cores 4 - -executor-memory 24G. Which would result in YARN allocating 30 containers with executors, 5 containers per node using up 4 executor cores each. The RAM per container on a node 124/5= 24GB (roughly).

1/1000 of the data: 1.5 days (to 2013-01-02 12:00:00) 19 seconds
1/100 of the data: 15 days (to 2013-01-16 00:00:00) 2.5 minutes
1/10 of the data: 144 days (to 2013-05-25 00:00:00) 4.8 hours
all of the data: 1435 days

   */
  def main(args: Array[String]) {
    val sc: SparkContext = SparkContext.getOrCreate
    val spark = SparkSession.builder.appName("Spark Stopped Vehicles").getOrCreate
    val lowerDateBound = if (args.length > 0) args(0) else "2013-01-01 00:00:00"
    val upperDateBound = if (args.length > 1) args(1) else "2016-12-06 00:00:00"

    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user", sc.getConf.get("spark.database.user"))
    connectionProperties.setProperty("password", sc.getConf.get("spark.database.password"))

    log.info("Reading DataFrame data from database")
    val query = "(SELECT vehicle_tag, report_time, speed " +
      s"FROM sfmta_avl WHERE report_time BETWEEN '$lowerDateBound' AND '$upperDateBound') as subq"
    /*
    -- partition boundaries queried from vehicles table using the following to split into 6 parts:
    select percentile_disc(0.17) within group (order by vehicle_tag),
      percentile_disc(0.33) within group (order by vehicle_tag),
      percentile_disc(0.50) within group (order by vehicle_tag),
      percentile_disc(0.67) within group (order by vehicle_tag),
      percentile_disc(0.83) within group (order by vehicle_tag)
      from vehicles_t;
     */
//    val predicates = Array("vehicle_tag < '6224'",
//      "vehicle_tag BETWEEN '6224' AND '8445'",
//      "vehicle_tag > '8445' AND vehicle_tag < 'C15417'",
//      "vehicle_tag BETWEEN 'C15417' AND 'C7769'",
//      "vehicle_tag > 'C7769' AND vehicle_tag < 'J2572'",
//      "vehicle_tag >= 'J2572'")
//    val all_readings_df: DataFrame = spark.read.jdbc(sc.getConf.get("spark.database.host"),
//      query, predicates, connectionProperties)
    val all_readings_df: DataFrame = spark.read.format("jdbc")
      .option("url", sc.getConf.get("spark.database.host"))
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", query)
      .option("user", sc.getConf.get("spark.database.user"))
      .option("password", sc.getConf.get("spark.database.password"))
      .option("numPartitions", 24)
      .option("partitionColumn", "report_time")
      .option("lowerBound", lowerDateBound)
      .option("upperBound", upperDateBound)
      .load()
    all_readings_df.repartitionByRange(24, col("vehicle_tag"))
    all_readings_df.cache()

    log.info("Filtering DataFrame for speed")
    val moving_df: DataFrame = filterMoving(all_readings_df)
    moving_df.persist(StorageLevel.DISK_ONLY)

    log.info("Finding the last time each vehicle was stopped, so we get the maximum stop time only")
    val last_stopped_df = findLastStoppedTime(all_readings_df)
    last_stopped_df.persist(StorageLevel.DISK_ONLY)

    log.info("Calculating when vehicles were last moving")
    val last_moving_time_df = calculateLastMovingTime(last_stopped_df, moving_df)

    log.info("Calculating how long vehicles were stopped")
    val last_moving_info_df = calculateStopLength(last_moving_time_df)

    log.info("Selecting desired columns and filtering for long stops")
    val top_stops_df = prepareForDatabaseTable(last_moving_info_df)

    log.info("Writing results to the database")
    top_stops_df.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", sc.getConf.get("spark.database.host"))
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "sfmta_stops")
      .option("user", sc.getConf.get("spark.database.user"))
      .option("password", sc.getConf.get("spark.database.password"))
      .save()

    spark.stop()

  }

  /** find the rows where the vehicle is stopped, but the next reading it is moving */
  def findLastStoppedTime(all_readings_df: DataFrame) = {
    val timeWindow = Window.partitionBy(col("vehicle_tag")).orderBy(col("report_time"))
    all_readings_df
      .withColumn("next_speed", lead(col("speed"), 1).over(timeWindow))
      .filter("speed = 0 and next_speed > 0")
  }

  def filterMoving(all_readings_df: DataFrame) = {
    all_readings_df.filter(col("speed") > 0)
  }

  def calculateLastMovingTime(stopped_df: DataFrame, moving_df: DataFrame) = {
    // find when a vehicle is stopped, what time was it last moving... this will help calculate how long it was stopped
    val sameVehicle = col("stopped.vehicle_tag") === col("moving.vehicle_tag")
    val stoppedAfterMoving = col("stopped.report_time") > col("moving.report_time")
    val joined_df = stopped_df.as("stopped").join(moving_df.as("moving"),
      sameVehicle && stoppedAfterMoving,"inner")
    joined_df
      .groupBy(col("stopped.vehicle_tag"), col("stopped.report_time"))
      .agg(max("moving.report_time"))
      .withColumnRenamed("max(moving.report_time)", "last_moving_time")
  }

  /** calculate how long the vehicle is stopped */
  def calculateStopLength(last_moving_time_df: DataFrame) = {
    val timediff = (startTime: Timestamp, endTime: Timestamp) => (endTime.getTime - startTime.getTime) / 1000
    val timediff_udf = udf(timediff)
    last_moving_time_df.withColumn("stopped_for",
      timediff_udf(col("last_moving_time"), col("report_time")))
  }

  /** get relevant columns; only vehicles stopped for a long time */
  def prepareForDatabaseTable(all_stops_df: DataFrame) = {
    all_stops_df
      .select("stopped.report_time", "stopped.vehicle_tag", "stopped_for")
      .filter(col("stopped_for") > 300 && col("stopped_for") < 7200)
  }

}
