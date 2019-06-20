package live.discoverdataengineering.sfmta.processing

import java.sql.Timestamp
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object StoppedVehicle {

  def main(args: Array[String]) {
    val sc: SparkContext = SparkContext.getOrCreate
    val spark = SparkSession.builder.appName("Spark Stopped J2836").getOrCreate

    // need to run with spark-submit --properties-file insight.conf
    val db_url = sc.getConf.get("spark.database.host") // "jdbc:postgresql://10.0.0.24:5217/sfmta"
    val db_user = sc.getConf.get("spark.database.user") // "insight"
    val db_pass = sc.getConf.get("spark.database.password")

    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user", db_user)
    connectionProperties.setProperty("password", db_pass)

    val stopped_df: DataFrame = spark.read.jdbc(db_url, "(SELECT * FROM sfmta_avl WHERE speed = 0 AND vehicle_tag = 'J2836') as stopped", connectionProperties)
    val moving_df: DataFrame = spark.read.jdbc(db_url, "(SELECT * FROM sfmta_avl WHERE speed > 0 AND vehicle_tag = 'J2836') as moving", connectionProperties)
    val all_readings_df: DataFrame = spark.read.jdbc(db_url, "(SELECT * FROM sfmta_avl WHERE vehicle_tag = 'J2836') as all_readings", connectionProperties)

    val joined_df = stopped_df.as("stopped").join(
      moving_df.as("moving")
      , col("stopped.vehicle_tag") === col("moving.vehicle_tag")
        && col("stopped.report_time") > col("moving.report_time")
      , "inner")
    val last_moving_time_df = joined_df.groupBy(col("stopped.report_time")).agg(max("moving.report_time")).withColumnRenamed("max(moving.report_time)", "last_moving_time")

    val timediff: ((Timestamp, Timestamp) => Long) = (startTime: Timestamp, endTime: Timestamp) => (endTime.getTime - startTime.getTime) / 1000
    val timediff_udf = udf(timediff)

    val last_moving_info_df = last_moving_time_df.withColumn("stopped_for",
      timediff_udf(col("last_moving_time"), col("report_time")))

    val last_stopped_df = all_readings_df.withColumn("next_speed",
      lead(col("speed"), 1).over(Window.orderBy(col("report_time")))).filter("speed = 0 and next_speed > 0")

    val all_stops_df = last_stopped_df.as("stopped").join(last_moving_info_df.as("last_moving"),
      col("stopped.report_time") === col("last_moving.report_time"), "inner")

    val top_stops_df = all_stops_df.orderBy(col("stopped_for").desc)

    // try the same thing in Spark SQL    println("...")
    spark.stop()
  }

}
