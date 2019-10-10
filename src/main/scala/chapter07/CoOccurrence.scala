package chapter07

import edu.umd.cloud9.collection.XMLInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.xml.{Elem, XML}

class CoOccurrence(spark: SparkSession, filePrefix: String) extends java.io.Serializable {

  import spark.implicits._

  val path = filePrefix + "/medsamp2016a.xml"

  var medlineRaw: Dataset[String] = null

  var medline: Dataset[Seq[String]] = null

  var topics: DataFrame = null

  var topicDist: DataFrame = null

  def loadMedline(): Dataset[String] = {
    if (medlineRaw == null) {
      val conf = new Configuration()
      conf.set(XMLInputFormat.START_TAG_KEY, "<MedlineCitation ")
      conf.set(XMLInputFormat.END_TAG_KEY, "</MedlineCitation>")

      val sc = spark.sparkContext

      val input = sc.newAPIHadoopFile(path, classOf[XMLInputFormat],
        classOf[LongWritable], classOf[Text], conf)

      medlineRaw = input.map(line => line._2.toString).toDS()
    }

    medlineRaw
  }

  private def majorTopics(record: String): Seq[String] = {
    val elem = XML.loadString(record)
    val dn = elem \\ "DescriptorName"
    val mt = dn.filter(n => (n \ "@MajorTopicYN").text == "Y")

    mt.map(n => n.text)
  }

  def getMedline(): Dataset[Seq[String]] = {
    if (medline == null) {
      medline = loadMedline().map(majorTopics)
      medline.cache()
    }

    medline
  }

  def getTopics(): DataFrame = {
    if (topics == null) {
      topics = getMedline()
        .flatMap(mesh => mesh)
        .toDF("topic")

      topics.createOrReplaceTempView("topics")
    }

    topics
  }

  def getTopicDist(): DataFrame = {
    if (topicDist == null) {
      getTopics()
        topicDist = spark.sql(
        """
        SELECT topic, COUNT(*) cnt
        FROM topics
        GROUP BY topic
        ORDER BY cnt DESC
      """)
    }

    topicDist
  }

  def getXml(position: Int): Elem = {
    val xmlStr = loadMedline().take(position)(position - 1)
    XML.loadString(xmlStr)
  }


}
