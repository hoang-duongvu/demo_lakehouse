package org.test.app

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object WriteHudi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WriteHudi")
//      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()

    val sipDF = spark.read
      .option("header", "true")
      .parquet("hdfs://namenode:9000/data/sip_invite/sip_invite.parquet")

//    sipDF.show()
    sipDF.printSchema()

    println("start write to hudi-----------------")
    val tableName = "sip_invite"
    val basePath = "hdfs://namenode:9000/data/hudi"
    val hiveSyncDB = "default"

    sipDF.write.format("hudi")
      .option("hoodie.datasource.write.recordkey.field", "call_id")
      .option("hoodie.datasource.write.precombine.field", "ts")
      .option("hoodie.datasource.write.partitionpath.field", "part_date,part_hour")
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", "insert")
      .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE")
//      HMS sync
      .option("hoodie.datasource.meta.sync.enable", "true")
      .option("hoodie.datasource.hive_sync.mode", "hms")
      .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://hive-metastore:9083")
      .option("hoodie.datasource.hive_sync.database", hiveSyncDB)
      .option("hoodie.datasource.hive_sync.partition_fields", "part_date,part_hour")
      .option("hoodie.datasource.hive_sync.partition_extractor_class", "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .option("hoodie.metadata.enable", "false")
      .mode(SaveMode.Overwrite)
      .save(basePath)
    println("end write hudi---------------------------")
    spark.stop()
  }
}
