package org.test.app

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object ReadEBS_Lake {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("testIO")
      .master("local[*]")
      .getOrCreate()

    val config = ConfigFactory.load()

    val prestoHost = config.getString("app.presto.host")
    val prestoPort = config.getString("app.presto.port")
    val prestoUser = config.getString("app.presto.user")
    val prestoPassword = config.getString("app.presto.password")

    val ebsCatalog = config.getString("app.ebs.catalog")
    val ebsSchema = config.getString("app.ebs.schema")
    val ebsTable = config.getString("app.ebs.table")

    val run_date = sys.env.getOrElse("RUN_DATE", "null")
    val run_hour = sys.env.getOrElse("RUN_HOUR", "null")
    val dateHour = s"""$run_date-$run_hour"""

    val subquery = s"""(
      SELECT event_id, msisdn, event_time, date_hour
      FROM $ebsCatalog.$ebsSchema.$ebsTable
      WHERE date_hour = '$dateHour'
    ) as T """

    val prestoUrl = s"jdbc:presto://$prestoHost:$prestoPort/$ebsCatalog/$ebsSchema"
    val df = spark.read.format("jdbc")
      .option("driver", "io.prestosql.jdbc.PrestoDriver")
      .option("url", prestoUrl)
      .option("dbtable", subquery)
      .option("user", prestoUser)
      .option("password", prestoPassword)
      .load()

//    println(run_date)
//    println(run_hour)
    df.show()
    spark.stop()
  }
}
