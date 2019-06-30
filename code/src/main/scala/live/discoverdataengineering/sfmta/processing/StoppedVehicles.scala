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
  /usr/local/spark/bin/spark-submit --class "live.discoverdataengineering.sfmta.processing.StoppedVehicles" --deploy-mode cluster --properties-file insight.conf --master spark://ip-10-0-0-20.us-west-2.compute.internal:7077 --conf "spark.network.timeout=10000000" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/home/ubuntu/log4j.properties" --jars /usr/local/spark/lib/postgresql-42.2.5.jar sfmta.jar
  /usr/local/spark/bin/spark-submit --class "live.discoverdataengineering.sfmta.processing.StoppedVehicles" --deploy-mode cluster --properties-file insight.conf --master spark://ip-10-0-0-20.us-west-2.compute.internal:7077 --conf "spark.network.timeout=10000000" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/home/ubuntu/log4j.properties" --jars /usr/local/spark/lib/postgresql-42.2.5.jar sfmta.jar "2013-01-01 00:00:00" "2013-01-16 00:00:00"

  testing with different loads:
  1/1000 of the data: 1.5 days (to 2013-01-02 12:00:00) 19 seconds
  1/100 of the data: 15 days (to 2013-01-16 00:00:00) 1 minute
  1/10 of the data: 144 days (to 2013-05-25 00:00:00) 52 min
  all of 2015: 365 days, 2.2 hours
  all of the data: 1435 days, ? (out of heap space... need more workers)
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
    val query = "(SELECT reading_id, vehicle_tag, report_time, speed " +
      s"FROM sfmta_avl WHERE report_time BETWEEN '$lowerDateBound' AND '$upperDateBound') as subq"
    val all_readings_df: DataFrame = spark.read.format("jdbc")
      .option("url", sc.getConf.get("spark.database.host"))
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", query)
      .option("user", sc.getConf.get("spark.database.user"))
      .option("password", sc.getConf.get("spark.database.password"))
      .option("numPartitions", 8)
      .option("partitionColumn", "report_time")
      .option("lowerBound", lowerDateBound)
      .option("upperBound", upperDateBound)
      .load()
    all_readings_df.repartitionByRange(8, col("vehicle_tag"))
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
    val closeInTime = col("stopped.reading_id") - col("moving.reading_id") < 300
    val joined_df = stopped_df.as("stopped").join(moving_df.as("moving"),
      closeInTime && sameVehicle && stoppedAfterMoving,"inner")
    joined_df
      .groupBy(col("stopped.vehicle_tag"), col("stopped.report_time"))
      .agg(max("moving.report_time"))
      .withColumnRenamed("max(moving.report_time)", "last_moving_time")
  }

  /** calculate how long the vehicle is stopped */
  def calculateStopLength(last_moving_time_df: DataFrame) = {
    last_moving_time_df.withColumn("stopped_for",
      unix_timestamp(col("report_time")) - unix_timestamp(col("last_moving_time")))
  }

  /** get relevant columns; only vehicles stopped for a long time */
  def prepareForDatabaseTable(all_stops_df: DataFrame) = {
    all_stops_df
      .select("report_time", "vehicle_tag", "stopped_for")
      .filter(col("stopped_for") > 300L && col("stopped_for") < 7200L)
  }

}
