import org.apache.spark.sql.SparkSession

object SimpleApp {

	def main(args: Array[String]) {
		val logFile = "/Users/mmenezes/Documents/apps/spark-2.4.4-bin-hadoop2.7/README.md"
		val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
		import spark.implicits._

		val logData = spark.read.textFile(logFile).cache()
		val numAs = logData.filter(line => line.contains("a")).count()
		val numBs = logData.filter(line => line.contains("b")).count()
		println(s"Lines with a: $numAs, Lines with b: $numBs")

		val usersDf = spark.read.load("/Users/mmenezes/Documents/apps/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/users.parquet")
		usersDf.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

		usersDf.write.format("orc")
  		.option("orc.bloom.filter.columns", "favorite_color")
  		.option("orc.dictionary.key.threshold", "1.0")
  		.save("users_with_options.orc")

		val peopleDf = spark.read.format("json")
			.load("/Users/mmenezes/Documents/apps/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/people.json")
		peopleDf.select("name", "age").write.format("parquet")
  			.save("namesAndAges.parquet")
		peopleDf.write.parquet("people.parquet")
		val parquetFileDf = spark.read.parquet("people.parquet")
		parquetFileDf.createOrReplaceTempView("parquetFile")
		val namesDf = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
		namesDf.map(attributes => "Name: " + attributes(0)).show()

		val squaresDf = spark.sparkContext.makeRDD(1 to 5)
			.map(i => (i, i * i))
			.toDF("value", "square")
		squaresDf.write.parquet("data/test_table/key=1")

		val cubesDf = spark.sparkContext.makeRDD(6 to 10)
			.map(i => (i, i * i *i ))
			.toDF("value", "cube")
		cubesDf.write.parquet("data/test_table/key=2")

		val mergedDf = spark.read
  			.option("mergeSchema", "true")
  			.parquet("data/test_table")
		mergedDf.printSchema()


		spark.stop()
	}
}
