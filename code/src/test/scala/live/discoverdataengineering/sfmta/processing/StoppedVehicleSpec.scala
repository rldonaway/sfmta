package live.discoverdataengineering.sfmta.processing

import live.discoverdataengineering.sfmta.SparkSessionTestWrapper
import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DataFramePrettyPrint}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import java.sql.Timestamp

import StoppedVehicles._

class StoppedVehicleSpec
  extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  val all_readings = Seq(
    (1, "B456", Timestamp.valueOf("2013-01-10 11:10:20"), 5),
    (2, "B456", Timestamp.valueOf("2013-01-10 11:16:00"), 0),
    (3, "B456", Timestamp.valueOf("2013-01-10 11:16:20"), 0),
    (4, "B456", Timestamp.valueOf("2013-01-10 11:16:50"), 12),
    (5, "C789", Timestamp.valueOf("2013-01-10 11:15:20"), 7),
    (6, "C789", Timestamp.valueOf("2013-01-10 11:16:00"), 10),
    (7, "C789", Timestamp.valueOf("2013-01-10 11:16:20"), 0),
    (8, "C789", Timestamp.valueOf("2013-01-10 11:16:50"), 9)
  ).toDF("reading_id", "vehicle_tag", "report_time", "speed")

  val stoppedSchema = List(
    StructField("reading_id", IntegerType, false),
    StructField("vehicle_tag", StringType),
    StructField("report_time", TimestampType),
    StructField("speed", IntegerType, false),
    StructField("next_speed", IntegerType)
  )

  val stopped = Seq(
    Row(3, "B456", Timestamp.valueOf("2013-01-10 11:16:20"), 0, 12),
    Row(7, "C789", Timestamp.valueOf("2013-01-10 11:16:20"), 0, 9)
  )

  it("extracts the last stopped record") {

    val actual_df = findLastStoppedTime(all_readings)

    val stopped_df = spark.createDataFrame(
      spark.sparkContext.parallelize(stopped),
      StructType(stoppedSchema)
    )

    assertSmallDataFrameEquality(actual_df, stopped_df)

  }

  val moving = Seq(
    Row(1, "B456", Timestamp.valueOf("2013-01-10 11:10:20"), 5),
    Row(4, "B456", Timestamp.valueOf("2013-01-10 11:16:50"), 12),
    Row(5, "C789", Timestamp.valueOf("2013-01-10 11:15:20"), 7),
    Row(6, "C789", Timestamp.valueOf("2013-01-10 11:16:00"), 10),
    Row(8, "C789", Timestamp.valueOf("2013-01-10 11:16:50"), 9)
  )

  val movingSchema = List(
    StructField("reading_id", IntegerType, false),
    StructField("vehicle_tag", StringType),
    StructField("report_time", TimestampType),
    StructField("speed", IntegerType, false)
  )

  it("includes readings where vehicles are moving") {

    val actual_df = filterMoving(all_readings)

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(moving),
      StructType(movingSchema)
    )

    assertSmallDataFrameEquality(actual_df, expectedDF)

  }

  val lastMovingSchema = List(
    StructField("vehicle_tag", StringType),
    StructField("report_time", TimestampType),
    StructField("last_moving_time", TimestampType)
  )

  val last_moving = Seq(
    Row("B456", Timestamp.valueOf("2013-01-10 11:16:20"), Timestamp.valueOf("2013-01-10 11:10:20")),
    Row("C789", Timestamp.valueOf("2013-01-10 11:16:20"), Timestamp.valueOf("2013-01-10 11:16:00"))
  )

  it("calculates the last moving time") {

    val stopped_df = spark.createDataFrame(
      spark.sparkContext.parallelize(stopped),
      StructType(stoppedSchema)
    )

    val moving_df = spark.createDataFrame(
      spark.sparkContext.parallelize(moving),
      StructType(movingSchema)
    )

    val actualDF = calculateLastMovingTime(stopped_df, moving_df)

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(last_moving),
      StructType(lastMovingSchema)
    )

    assertSmallDataFrameEquality(actualDF, expectedDF)

  }

  val stopLengthSchema = List(
    StructField("vehicle_tag", StringType),
    StructField("report_time", TimestampType),
    StructField("last_moving_time", TimestampType),
    StructField("stopped_for", LongType)
  )

  val stop_length = Seq(
    Row("B456", Timestamp.valueOf("2013-01-10 11:16:20"), Timestamp.valueOf("2013-01-10 11:10:20"), 360L),
    Row("C789", Timestamp.valueOf("2013-01-10 11:16:20"), Timestamp.valueOf("2013-01-10 11:16:00"), 20L)
  )

  it("calculates the stop lengths") {
    val last_moving_df = spark.createDataFrame(
      spark.sparkContext.parallelize(last_moving),
      StructType(lastMovingSchema))

    val actualDF = calculateStopLength(last_moving_df)

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(stop_length),
      StructType(stopLengthSchema)
    )

    assertSmallDataFrameEquality(actualDF, expectedDF)
  }

  it("prepares data for the database table") {
    val stop_length_df = spark.createDataFrame(
      spark.sparkContext.parallelize(stop_length),
      StructType(stopLengthSchema)
    )

    val actualDF = prepareForDatabaseTable(stop_length_df)

    val dbReadySchema = List(
      StructField("report_time", TimestampType),
      StructField("vehicle_tag", StringType),
      StructField("stopped_for", LongType)
    )

    val db_ready = Seq(
      Row(Timestamp.valueOf("2013-01-10 11:16:20"), "B456", 360L)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(db_ready),
      StructType(dbReadySchema)
    )

    assertSmallDataFrameEquality(actualDF, expectedDF)

  }

}
