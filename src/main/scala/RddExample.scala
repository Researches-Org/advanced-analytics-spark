import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RddExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Rdd Example")
      .setMaster("spark://127.0.0.1:7077")

    val sparkContext = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sparkContext.parallelize(data)
    val sum = distData.reduce((a, b) => a + b)
    println(s"sum: $sum")

    val lines = sparkContext.textFile("/Users/mmenezes/Documents/workspace/spark/SimpleProject/src/main/resources/data.txt")
    val lineLengths = lines.map(s => s.length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    println(s"Total Length: $totalLength")

    val pairs = lines.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)
    println(s"line counts: $counts")

  }

}
