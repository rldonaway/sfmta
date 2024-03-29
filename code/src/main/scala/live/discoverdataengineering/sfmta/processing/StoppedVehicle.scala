package live.discoverdataengineering.sfmta.processing

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Shows the times a single vehicle was stopped. This is just a proof of concept.
  */
object StoppedVehicle {

  /*
  running...
  (sh /usr/local/spark/sbin/start-all.sh; cd ~)
  /usr/local/spark/bin/spark-submit --class "live.discoverdataengineering.sfmta.processing.StoppedVehicle" --properties-file insight.conf --master spark://ip-10-0-0-20.us-west-2.compute.internal:7077 --deploy-mode cluster --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/home/ubuntu/log4j.properties" --jars /usr/local/spark/lib/postgresql-42.2.5.jar sfmta.jar
   */
  def main(args: Array[String]) {
    val sc: SparkContext = SparkContext.getOrCreate
    val spark = SparkSession.builder.appName("Spark Stopped J2836").getOrCreate

    def readDataFrame(query: String): DataFrame = {
      spark.read.format("jdbc")
        .option("url", sc.getConf.get("spark.database.host"))
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", query)
        .option("user", sc.getConf.get("spark.database.user"))
        .option("password", sc.getConf.get("spark.database.password"))
        .load()
    }

    val columns = "(SELECT reading_id, report_time, vehicle_tag, speed, train_assignment, latitude, longitude FROM sfmta_avl "
    val stopped_df: DataFrame = readDataFrame(columns + "WHERE speed = 0 AND vehicle_tag = 'J2836') as subq")
    val moving_df: DataFrame = readDataFrame(columns + "WHERE speed > 0 AND vehicle_tag = 'J2836') as subq")
    val all_readings_df: DataFrame = readDataFrame(columns + "WHERE vehicle_tag = 'J2836') as subq")

    // find when a vehicle is stopped, what time was it last moving... this will help calculate how long it was stopped
    val sameVehicle = col("stopped.vehicle_tag") === col("moving.vehicle_tag")
    val stoppedAfterMoving = col("stopped.report_time") > col("moving.report_time")
    val joined_df = stopped_df.as("stopped")
      .join(moving_df.as("moving"), sameVehicle && stoppedAfterMoving,"inner")
    val last_moving_time_df = joined_df
      .groupBy(col("stopped.report_time"))
      .agg(max("moving.report_time"))
      .withColumnRenamed("max(moving.report_time)","last_moving_time")

    val timediff: ((Timestamp, Timestamp) => Long) = (startTime: Timestamp, endTime: Timestamp) => (endTime.getTime - startTime.getTime) / 1000
    val timediff_udf = udf(timediff)

    // calculate how long the vehicle is stopped
    val last_moving_info_df = last_moving_time_df.withColumn("stopped_for",
      timediff_udf(col("last_moving_time"), col("report_time")))

    // find the rows where the vehicle is stopped, but the next reading it is moving
    val timeWindow = Window.orderBy(col("report_time"))
    val last_stopped_df = all_readings_df
      .withColumn("next_speed", lead(col("speed"), 1).over(timeWindow))
      .filter("speed = 0 and next_speed > 0")

    // join to add the last moving info columns
    val sameTime = col("stopped.report_time") === col("last_moving.report_time")
    val all_stops_df = last_stopped_df.as("stopped")
      .join(last_moving_info_df.as("last_moving"), sameTime,"inner")

    // get relevant columns; put longest stops first
    val top_stops_df = all_stops_df.select("reading_id", "stopped.report_time", "vehicle_tag", "speed",
      "train_assignment", "latitude", "longitude", "last_moving_time", "stopped_for")

    top_stops_df.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", sc.getConf.get("spark.database.host"))
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "sfmta_stops_j2836")
      .option("user", sc.getConf.get("spark.database.user"))
      .option("password", sc.getConf.get("spark.database.password"))
      .save()

    spark.stop()

  }

}
