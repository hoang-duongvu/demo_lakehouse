package org.test.app
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object ReadSIP_Hudi {
    def main(args: Array[String]): Unit = {
      val config = ConfigFactory.load()

      val spark = SparkSession.builder()
        .appName("Read Hudi Data via Trino")
        .master("local[*]")
        .getOrCreate()

      val trinoHost = config.getString("app.trino.host")
      val trinoPort = config.getString("app.trino.port")
      val trinoUser = config.getString("app.trino.user")
      val trinoPassword = config.getString("app.trino.password")

      val sipCatalog = config.getString("app.sip.catalog")
      val sipSchema = config.getString("app.sip.schema")
      val sipTable =  config.getString("app.sip.table")
      val partDate = "2023-10-01"
      val partHour = "5"

      val jdbcUrl = "jdbc:trino://{trinoHost}:{trinoPort}/{sipCatalog}/{sipSchema}"
        .replace("{trinoHost}", trinoHost)
        .replace("{trinoPort}", trinoPort)
        .replace("{sipCatalog}", sipCatalog)
        .replace("{sipSchema}", sipSchema)

      val sipCollectQuery = s"""(
        SELECT call_id, start_ts, end_call_ts, call_type, to_phone_enc, from_phone_enc
        FROM $sipCatalog.$sipSchema.$sipTable
        WHERE part_date = '$partDate' AND cast(part_hour as varchar) = '$partHour'
        ) AS t"""

      val df = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", sipCollectQuery)
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .option("user", trinoUser)  // Bỏ comment nếu cần
        .option("password", trinoPassword)  // Bỏ comment nếu cần
        .load()

      // Hiển thị dữ liệu hoặc xử lý tiếp
      df.printSchema()

      spark.stop()
    }
}
