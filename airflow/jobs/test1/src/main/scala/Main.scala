import org.apache.hudi.DataSourceWriteOptions.TABLE_NAME
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


object Main {
  def main(args: Array[String]): Unit = {
    val basePath = "hdfs://namenode:9000/data/hudi"
    val tableName = "test_users_hudi"

    val spark = SparkSession.builder()
      .appName("WriteTestHudi")
//      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()

    import spark.implicits._

    // 1) Định nghĩa schema
    val schema = StructType(Seq(
      StructField("user_id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("email", StringType, nullable = true),
      StructField("ts", LongType, nullable = true)
    ))

    // 2) Tạo sample data
    val data = Seq(
      Row(1, "Alice", "alice@example.com", System.currentTimeMillis()),
      Row(2, "Bob", "bob@example.com", System.currentTimeMillis()),
      Row(3, "Carol", "carol@example.com", System.currentTimeMillis())
    )

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema)

    //    df.show(false)
    //    df.printSchema()

    // 3) Hudi write options (simple example, COPY_ON_WRITE)
    val hudiOptions = Map[String, String](
      "hoodie.table.name" -> tableName,
      "hoodie.datasource.write.recordkey.field" -> "user_id",
      "hoodie.datasource.write.precombine.field" -> "ts",
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.datasource.hive_sync.enable" -> "true", // bật nếu bạn muốn sync Hive (thêm config)
      // nếu muốn dùng hive sync:
       "hoodie.datasource.hive_sync.mode" -> "hms",
       "hoodie.datasource.hive_sync.database" -> "default",
       "hoodie.datasource.hive_sync.table" -> tableName
      // và đảm bảo hive-site.xml có trên classpath
    )

    //    // 4) Viết sang Hudi trên HDFS
    df.write
      .format("hudi")
      .options(hudiOptions)
      .mode(SaveMode.Append)
      .save(basePath)

    spark.stop()
  }
}
