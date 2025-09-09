package org.test.app

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.io.StdIn

object Pipeline_02_opt {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MyApp")
//      .master("local[*]")
      .config("spark.jars.packages", "com.typesafe:config:1.4.3,io.trino:trino-jdbc:422,io.prestosql:presto-jdbc:350")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      //      .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
      //      .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", -1) // disable broadcast join
      .getOrCreate()

    val config = ConfigFactory.load()
    val numParts = 4 // dung cho repartition()

    // Doc Sip Invite tu Lakehouse Trino
    val trinoHost = config.getString("app.trino.host")
    val trinoPort = config.getString("app.trino.port")
    val trinoUser = config.getString("app.trino.user")
    val trinoPassword = config.getString("app.trino.password")

    val sipCatalog = config.getString("app.sip.catalog")
    val sipSchema = config.getString("app.sip.schema")
    val sipTable =  config.getString("app.sip.table")
    val partDate = "2023-10-01"
    val partHour = "5"

    val sipUrl = "jdbc:trino://{trinoHost}:{trinoPort}/{sipCatalog}/{sipSchema}"
      .replace("{trinoHost}", trinoHost)
      .replace("{trinoPort}", trinoPort)
      .replace("{sipCatalog}", sipCatalog)
      .replace("{sipSchema}", sipSchema)

    val sipCollectQuery = s"""(
        SELECT call_id, start_ts, end_call_ts, call_type, to_phone_enc, from_phone_enc
        FROM $sipCatalog.$sipSchema.$sipTable
        WHERE part_date = '$partDate' AND cast(part_hour as varchar) = '$partHour'
        ) AS t"""

    val sipInvite = spark.read
      .format("jdbc")
      .option("url", sipUrl)
      .option("dbtable", sipCollectQuery)
      .option("driver", "io.trino.jdbc.TrinoDriver")
      .option("user", trinoUser)
      .option("password", trinoPassword)
      .load()

    // Doc EBS tu Lake Presto
    val prestoHost = config.getString("app.presto.host")
    val prestoPort = config.getString("app.presto.port")
    val prestoUser = config.getString("app.presto.user")
    val prestoPassword = config.getString("app.presto.password")
    val prestoTrustStorePass = config.getString("app.presto.trustStorePass")
    val prestoKeyStorePath = config.getString("app.presto.keyStorePath")

    val ebsCatalog = config.getString("app.ebs.catalog")
    val ebsSchema = config.getString("app.ebs.schema")
    val ebsTable = config.getString("app.ebs.table")

    val dateHour = "2023-10-01-5"

    val ebsQuery = s"""(
      SELECT event_id, msisdn, eci, event_time, date_hour
      FROM $ebsCatalog.$ebsSchema.$ebsTable
      WHERE date_hour = '$dateHour'
    ) as T """

    //    val ebsUrl = "jdbc:presto://{presto_host}:{presto_port}/hive?SSL=true&SSLTrustStorePassword={truststore_password}&SSLTrustStorePath={keystore_path}"
    //      .replace("{presto_host}", prestoHost)
    //      .replace("{presto_port}", prestoPort)
    //      .replace("{truststore_password}", prestoTrustStorePass)
    //      .replace("{keystore_path}", prestoKeyStorePath)

    val ebsUrl = s"jdbc:presto://$prestoHost:$prestoPort/$ebsCatalog/$ebsSchema"
    // Duong link tren may ca nhan

    val ebs = spark.read.format("jdbc")
      .option("driver", "io.prestosql.jdbc.PrestoDriver")
      .option("url", ebsUrl)
      .option("dbtable", ebsQuery)
      .option("user", prestoUser)
      .option("password", prestoPassword)
      .load()
      .repartition(numParts, col("msisdn"))
      .sortWithinPartitions(col("msisdn"))


    ebs.persist(StorageLevel.MEMORY_AND_DISK).count()
    // Persist vi EBS dung de join 2 lan

    val sipWithKey = sipInvite
      .withColumn("join_key",
        when(col("call_type") === "MO", col("from_phone_enc"))
        .when(col("call_type") === "MT", col("to_phone_enc"))
        .otherwise(col("from_phone_enc"))
      )
      .select("call_id", "start_ts", "end_call_ts", "join_key")
      .repartition(4, col("join_key"))
      .sortWithinPartitions(col("join_key"))
    // repartition de cac row cung join_key ve cung 1 partition, phuc vu Join
    sipWithKey.persist(StorageLevel.MEMORY_AND_DISK).count()

    val ebsTempDF = ebs
      .filter(col("event_id") === "l_bearer_activate" || col("event_id") === "l_bearer_deactivate")
      .sortWithinPartitions("msisdn")

    ebsTempDF.persist(StorageLevel.MEMORY_AND_DISK).count()

    val bearerTimeDF = sipWithKey.hint("MERGE").join(ebsTempDF.hint("MERGE"),
        sipWithKey("join_key") === ebsTempDF("msisdn") &&
          (ebsTempDF("event_id") === "l_bearer_activate" &&
            ebsTempDF("event_time").between(sipWithKey("start_ts") - 3, sipWithKey("start_ts") + 3)
          )
      )
      .withColumnRenamed("event_time", "activate_time")
      .select("call_id", "start_ts", "end_call_ts", "join_key", "activate_time")
      .hint("MERGE").join(ebsTempDF.hint("MERGE"),
        sipWithKey("join_key") === ebsTempDF("msisdn") &&
          (ebsTempDF("event_id") === "l_bearer_deactivate" &&
            ebsTempDF("event_time").between(sipWithKey("end_call_ts") - 3, sipWithKey("end_call_ts") + 3)
            )
      )
      .withColumnRenamed("event_time", "deactivate_time")
      .select("call_id", "start_ts", "end_call_ts", "join_key", "activate_time", "deactivate_time")

    bearerTimeDF.show(5)

    val cellDetectDF = bearerTimeDF.hint("MERGE").join(ebs.hint("MERGE"),
      bearerTimeDF("join_key") === ebs("msisdn") &&
        col("event_time").between(col("activate_time"), col("deactivate_time"))
    )
    .select("call_id", "start_ts", "end_call_ts", "activate_time", "deactivate_time", "event_id", "eci", "msisdn", "event_time", "date_hour")
    cellDetectDF.show(5)

    println("------------------------------ START WRITE HUDI! -----------------------------")
    val tableName = config.getString("app.cell_detect.table2")
    val basePath = config.getString("app.cell_detect.base_path")
    val hiveSyncDB = config.getString("app.cell_detect.hive_sync_db")
    val hiveSyncMetastoreUris = config.getString("app.cell_detect.hive_sync_metastore_uris")

    cellDetectDF.write.format("hudi")
      .option("hoodie.datasource.write.recordkey.field", "call_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "date_hour")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
      //      HMS sync
      .option("hoodie.datasource.meta.sync.enable", "true")
      .option("hoodie.datasource.hive_sync.mode", "hms")
      .option("hoodie.datasource.hive_sync.metastore.uris", hiveSyncMetastoreUris)
      .option("hoodie.datasource.hive_sync.database", hiveSyncDB)
      .option("hoodie.datasource.hive_sync.partition_fields", "date_hour")
      .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .option("hoodie.metadata.enable", "false")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    println("------------------------------ FINISHED WRITE HUDI! -----------------------------")
    spark.stop()
  }
}
