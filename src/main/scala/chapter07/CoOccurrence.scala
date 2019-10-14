package chapter07

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import edu.umd.cloud9.collection.XMLInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.util.StatCounter

import scala.xml.{Elem, XML}

class CoOccurrence(spark: SparkSession, filePrefix: String) extends java.io.Serializable {

  import spark.implicits._

  val path = filePrefix + "/src/main/resources/chapter07/medsamp2016a.xml"

  private var medlineRaw: Dataset[String] = null

  private var medline: Dataset[Seq[String]] = null

  private var topics: DataFrame = null

  private var topicDist: DataFrame = null

  private var topicPairs: DataFrame = null

  private var cooccurs: DataFrame = null

  private var vertices: DataFrame = null

  private var edges: Dataset[Edge[Long]] = null

  private var vertexRdd: RDD[(Long, String)] = null

  private var topicGraph: Graph[String, Long] = null

  private var connectedComponents: Graph[VertexId, Long] = null

  private var componentsDf: DataFrame = null

  private var topicComponentDf: DataFrame = null

  private var degrees: VertexRDD[Int] = null

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

      topicDist.createOrReplaceTempView("topic_dist")
    }

    topicDist
  }

  def getTopicDistCountDist(): DataFrame = {

    getTopicDist()

    spark.sql(
      """
         SELECT cnt, COUNT(*) dist
         FROM topic_dist
         GROUP BY cnt
         ORDER BY dist DESC
      """)
  }

  def getTopicPairs(): DataFrame = {
    if (topicPairs == null) {
      topicPairs = getMedline().flatMap(t => {
        t.sorted.combinations(2)
      }).toDF("pairs")

      topicPairs.createOrReplaceTempView("topic_pairs")
    }

    topicPairs
  }

  def getCoOccurs(): DataFrame = {
    if (cooccurs == null) {
      getTopicPairs()

      cooccurs = spark.sql(
        """
         SELECT pairs, COUNT(*) cnt
         FROM topic_pairs
         GROUP BY pairs
      """)

      cooccurs.createOrReplaceTempView("cooccurs")

      cooccurs.cache()
    }

    cooccurs
  }

  def getTop10Pairs(): DataFrame = {
    getCoOccurs()

    spark.sql(
      """
        SELECT pairs, cnt
        FROM cooccurs
        ORDER BY cnt DESC
        LIMIT 10
      """)
  }

  private def hashId(str: String): Long = {
    val bytes = MessageDigest.getInstance("MD5")
      .digest(str.getBytes(StandardCharsets.UTF_8))

    (bytes(0) & 0xFFL) |
      ((bytes(1) & 0xFFL) << 8) |
      ((bytes(2) & 0xFFL) << 16) |
      ((bytes(3) & 0xFFL) << 24) |
      ((bytes(4) & 0xFFL) << 32) |
      ((bytes(5) & 0xFFL) << 40) |
      ((bytes(6) & 0xFFL) << 48) |
      ((bytes(7) & 0xFFL) << 56)
  }

  def getVertices(): DataFrame = {
    if (vertices == null) {
      vertices = getTopics().map {
        case Row(topic: String) => (hashId(topic), topic)
      }.toDF("hash", "topic")
    }

    vertices
  }

  def countDistinctHash(): Array[Row] = {
    getVertices().agg(functions.countDistinct("hash")).take(1)
  }

  def getEdges(): Dataset[Edge[Long]] = {
    if (edges == null) {
      edges = getCoOccurs().map {
        case Row(topics: Seq[_], cnt: Long) =>
          val ids = topics.map(_.toString).map(hashId).sorted
          Edge(ids(0), ids(1), cnt)
      }
    }

    edges
  }

  def getVertexRdd(): RDD[(Long, String)] = {
    val vertexRdd = getVertices().rdd.map {
      case Row(hash: Long, topic: String) => (hash, topic)
    }

    vertexRdd
  }

  def getTopicGraph(): Graph[String, Long] = {
    if (topicGraph == null) {
      topicGraph = Graph(getVertexRdd(), getEdges().rdd)
      topicGraph.cache()
    }

    topicGraph
  }

  def getConnectedComponents(): Graph[VertexId, Long] = {
    if (connectedComponents == null) {
      connectedComponents = getTopicGraph().connectedComponents()
    }

    connectedComponents
  }

  def getComponentsDf(): DataFrame = {
    if (componentsDf == null) {
      componentsDf = getConnectedComponents().vertices.toDF("vid", "cid")
    }

    componentsDf
  }

  def getTopicComponentDf(): DataFrame = {
    if (topicComponentDf == null) {
      topicComponentDf = getTopicGraph().vertices.innerJoin(getConnectedComponents().vertices) {
        (topicId, name, componentId) => (name, componentId)
      }.toDF("topic", "name_cid").select("name_cid._1", "name_cid._2").toDF("topic", "cid")
    }

    topicComponentDf
  }

  def getDegrees(): VertexRDD[Int] = {
    if (degrees == null) {
      degrees = getTopicGraph().degrees
      degrees.cache()
    }

    degrees
  }

  def getDegreesStats(): StatCounter = {
    getDegrees().map(_._2).stats()
  }

  def getSingleTopics(): Dataset[Seq[String]] = {
    getMedline().filter(x => x.size == 1)
  }

  def getDistinctSingleTopics(): Dataset[String] = {
    getSingleTopics().flatMap(topic => topic).distinct
  }

  def getNamesAndDegrees(): DataFrame = {
    getDegrees().innerJoin(getTopicGraph().vertices) {
      (topicId, degree, name) => (name, degree.toInt)
    }.toDF("topic", "name_degree")
      .select("name_degree._1", "name_degree._2")
      .toDF("topic", "degree")
  }

  def getTopicDistRdd(): RDD[(Long, Long)] = {
    val topicDistRdd = getTopicDist().map{
      case Row(topic: String, cnt: Long) => (hashId(topic), cnt)
    }.rdd

    topicDistRdd
  }

  def getTopicDistGraph(): Graph[Long, Long] = {
    Graph(getTopicDistRdd(), getTopicGraph().edges)
  }

  def chiSq(YY: Long, YB: Long, YA: Long, T: Long): Double = {
    val NB = T - YB
    val NA = T - YA
    val YN = YA - YY
    val NY = YB - YY
    val NN = T - NY - YN - YY
    val inner = math.abs(YY * NN - YN * NY) - T / 2.0
    T * math.pow(inner, 2) / (YA * NA * YB * NB)
  }

  def getChiSquaredGraph(): Graph[Long, Double] = {
    val T = getMedline().count()
    val chiSquaredGraph = getTopicDistGraph().mapTriplets(triplet => {
      chiSq(triplet.attr, triplet.srcAttr, triplet.dstAttr, T)
    })

    chiSquaredGraph
  }

  def getChiSquaredGraphStats(): StatCounter = {
    getChiSquaredGraph().edges.map(x => x.attr).stats()
  }

  def getInterestingGraph(): Graph[Long, Double] = {
    val interesting = getChiSquaredGraph().subgraph(
      triplet => triplet.attr > 19.5)

    interesting
  }

  def getTriCountGraph(): Graph[Int, Double] = {
    val triCountGraph = getInterestingGraph().triangleCount()

    triCountGraph
  }

  def getTriCountGraphStats(): StatCounter = {
    getTriCountGraph().vertices.map(x => x._2).stats()
  }

  def getMaxTrisGraph(): VertexRDD[Double] = {
     getInterestingGraph().degrees.mapValues(d => d * (d - 1) / 2.0)
  }

  def getClusterCoeficient() = {
    getTriCountGraph().vertices.
      innerJoin(getMaxTrisGraph()) {
        (vertexId, triCount, maxTris) => {
          if (maxTris == 0) 0 else triCount / maxTris
        }
      }
  }

  def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {

    def minThatExists(k: VertexId): Int = {
      math.min(
        m1.getOrElse(k, Int.MaxValue),
        m2.getOrElse(k, Int.MaxValue))
    }

    (m1.keySet ++ m2.keySet).map {
      k => (k, minThatExists(k))
    }.toMap

  }

  def update(id: VertexId,
             state: Map[VertexId, Int],
             msg: Map[VertexId, Int]) = {
    mergeMaps(state, msg)
  }

  def checkIncrement(a: Map[VertexId, Int],
                     b: Map[VertexId, Int],
                     bid: VertexId) = {

    val aplus = a.map { case (v, d) => v -> (d + 1) }

    if (b != mergeMaps(aplus, b)) {
      Iterator((bid, aplus))
    } else {
      Iterator.empty
    }

  }

  def iterate(e: EdgeTriplet[Map[VertexId, Int], _]) = {
    checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
      checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
  }

  def getXml(position: Int): Elem = {
    val xmlStr = loadMedline().take(position)(position - 1)
    XML.loadString(xmlStr)
  }

}
