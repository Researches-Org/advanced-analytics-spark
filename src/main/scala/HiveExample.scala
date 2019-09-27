import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object HiveExample {

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH '/Users/mmenezes/Documents/apps/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/kv1.txt' INTO TABLE src")

    sql("SELECT * FROM src").show()

    sql("SELECT count(*) FROM src").show()

    val sqlDf = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    val stringDf = sqlDf.map {
      case Row(key: Int, value: String) => s"key: $key, value: $value"
    }

    stringDf.show()

    val recordsDf = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDf.createOrReplaceTempView("records")

    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

    sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")

    val df = spark.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")

    sql("SELECT * FROM hive_records").show()


    // Prepare a Parquet data directory
    val dataDir = "/tmp/parquet_data"
    spark.range(10).write.parquet(dataDir)
    // Create a Hive external Parquet table
    sql(s"CREATE EXTERNAL TABLE hive_bigints(id bigint) STORED AS PARQUET LOCATION '$dataDir'")
    // The Hive external table should already have data
    sql("SELECT * FROM hive_bigints").show()
    // +---+
    // | id|
    // +---+
    // |  0|
    // |  1|
    // |  2|
    // ... Order may vary, as spark processes the partitions in parallel.

    // Turn on flag for Hive Dynamic Partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // Create a Hive partitioned table using DataFrame API
    df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
    // Partitioned column `key` will be moved to the end of the schema.
    sql("SELECT * FROM hive_part_tbl").show()
    // +-------+---+
    // |  value|key|
    // +-------+---+
    // |val_238|238|
    // | val_86| 86|
    // |val_311|311|
    // ...

    spark.stop()
  }

}
