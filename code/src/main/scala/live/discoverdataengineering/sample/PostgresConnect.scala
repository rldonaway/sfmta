package live.discoverdataengineering.sample

import org.apache.log4j.LogManager
import org.apache.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}

/** Just for testing database connectivity. */
object PostgresConnect {

  @transient lazy val log = LogManager.getLogger("live.discoverdataengineering.sample.PostgresConnect")

  /*
    /usr/local/spark/bin/spark-submit --class "live.discoverdataengineering.sample.PostgresConnect" --properties-file insight.conf --master spark://ip-10-0-0-20.us-west-2.compute.internal:7077 --deploy-mode cluster --conf "spark.network.timeout=10000000" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/home/ubuntu/log4j.properties" --jars /usr/local/spark/lib/postgresql-42.2.5.jar sfmta.jar
   */
  def main(args: Array[String]) {
    val sc: SparkContext = SparkContext.getOrCreate
    val spark = SparkSession.builder.appName("Spark Postgres Connect").getOrCreate

    val df = spark.read.format("jdbc")
      .option("url", sc.getConf.get("spark.database.host"))
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "vehicles_t")
      .option("user", sc.getConf.get("spark.database.user"))
      .option("password", sc.getConf.get("spark.database.password"))
      .load()

    log.info("Read dataframe from database")

    df.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", sc.getConf.get("spark.database.host"))
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", "vehicles_t2")
      .option("user", sc.getConf.get("spark.database.user"))
      .option("password", sc.getConf.get("spark.database.password"))
      .save()

    log.info("Wrote dataframe to database")

    spark.stop()
  }
}

