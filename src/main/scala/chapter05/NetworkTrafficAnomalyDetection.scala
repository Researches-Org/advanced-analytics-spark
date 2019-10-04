package chapter05

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.Random

class NetworkTrafficAnomalyDetection(private val spark: SparkSession) extends java.io.Serializable {

  import spark.implicits._

  val FILE = "/home/cy64/Downloads/kddcup.data"

  private var dataWithoutHeader: DataFrame = null

  private var data: DataFrame = null

  private var dataGroupedByLabel: DataFrame = null

  private var numericOnlyData: DataFrame = null

  private var assembler: VectorAssembler = null

  private var kmeans: KMeans = null

  private var pipeline: Pipeline = null

  private var pipelineModel: PipelineModel = null

  private var kmeansModel: KMeansModel = null

  private var clusterCenters: Array[Vector] = null

  private var withCluster: DataFrame = null

  private var withClusterGroupedByLabelAndCluster: DataFrame = null

  def getDataWithoutHeader: DataFrame = {
    if (dataWithoutHeader == null) {
      dataWithoutHeader = spark.read
        .option("inferSchema", true)
        .option("header", false)
        .csv(FILE)
    }

    dataWithoutHeader
  }

  def getData: DataFrame = {
    if (data == null) {
      data = getDataWithoutHeader.toDF(
        "duration", "protocol_type", "service", "flag",
        "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
        "hot", "num_failed_logins", "logged_in", "num_compromised",
        "root_shell", "su_attempted", "num_root", "num_file_creations",
        "num_shells", "num_access_files", "num_outbound_cmds",
        "is_host_login", "is_guest_login", "count", "srv_count",
        "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
        "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
        "dst_host_count", "dst_host_srv_count",
        "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
        "dst_host_serror_rate", "dst_host_srv_serror_rate",
        "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
        "label").cache()
    }

    data
  }

  def getDataGroupedByLabel: DataFrame = {
    if (dataGroupedByLabel == null) {
      dataGroupedByLabel = getData.select("label")
        .groupBy("label")
        .count()
        .orderBy($"count".desc)
    }
    dataGroupedByLabel
  }

  def getNumericOnlyData: DataFrame = {
    if (numericOnlyData == null) {
      numericOnlyData = getData.drop("protocol_type", "service", "flag").cache()
    }

    numericOnlyData
  }

  def getAssembler: VectorAssembler = {
    if (assembler == null) {
      assembler = new VectorAssembler()
          .setInputCols(getNumericOnlyData.columns.filter(_ != "label"))
          .setOutputCol("featureVector")
    }

    assembler
  }

  def getKMeans: KMeans = {
    if (kmeans == null) {
      kmeans = new KMeans()
          .setPredictionCol("cluster")
          .setFeaturesCol("featureVector")
    }

    kmeans
  }

  def getPipeline: Pipeline = {
    if (pipeline == null) {
      pipeline = new Pipeline().setStages(Array(getAssembler, getKMeans))
    }

    pipeline
  }

  def createPipeline(assembler: VectorAssembler, kmeans: KMeans): Pipeline = {
    new Pipeline().setStages(Array(assembler, kmeans))
  }

  def getPipelineModel: PipelineModel = {
    if (pipelineModel == null) {
      pipelineModel = getPipeline.fit(getNumericOnlyData)
    }

    pipelineModel
  }

  def getKmeansModel: KMeansModel = {
    if (kmeansModel == null) {
      kmeansModel = getPipelineModel.stages.last.asInstanceOf[KMeansModel]
    }

    kmeansModel
  }

  def getClusterCenters: Array[Vector] = {
    if (clusterCenters == null) {
      clusterCenters = getKmeansModel.clusterCenters
    }

    clusterCenters
  }

  def getWithCluster: DataFrame = {
    if (withCluster == null) {
      withCluster = getPipelineModel.transform(getNumericOnlyData)
    }

    withCluster
  }

  def getWithClusterGroupedByLabelAndCluster: DataFrame = {
    if (withClusterGroupedByLabelAndCluster == null) {
      withClusterGroupedByLabelAndCluster = getWithCluster.select("cluster", "label")
          .groupBy("cluster", "label").count()
          .orderBy($"cluster", $"count".desc)
    }

    withClusterGroupedByLabelAndCluster
  }

  def fitData(pipeline: Pipeline, data: DataFrame): KMeansModel = {
    pipeline.fit(data).stages.last.asInstanceOf[KMeansModel]
  }

  def clusteringScore0(data: DataFrame, k: Int): Double = {
    val assembler = getAssembler

    val kmeans = getKMeans.setSeed(Random.nextLong()).setK(k)

    val pipeline = createPipeline(assembler, kmeans)

    val kmeansModel = fitData(pipeline, data)

    kmeansModel.computeCost(assembler.transform(data)) / data.count()
  }

  def clusteringScore1(data: DataFrame, k: Int): Double = {
    val assembler = getAssembler

    val kmeans = getKMeans.setSeed(Random.nextLong()).setK(k).setMaxIter(40).setTol(1.0e-5)

    val pipeline = createPipeline(assembler, kmeans)

    val kmeansModel = fitData(pipeline, data)

    kmeansModel.computeCost(assembler.transform(data)) / data.count()
  }

  def clusteringScore2(data: DataFrame, k: Int): Double = {
    val pipeline = getPipelineWithCategoricalFeatures(data, k)

    val pipelineModel = pipeline.fit(data)

    val kMeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]

    kMeansModel.computeCost(pipelineModel.transform(data)) / data.count()
  }

  def getPipelineWithCategoricalFeatures(data: DataFrame, k: Int): Pipeline = {
    val (protoTypeEncoder, protoTypeVecCol) = oneHotPipeline("protocol_type")
    val (serviceEncoder, serviceVecCol) = oneHotPipeline("service")
    val (flagEncoder, flagVecCol) = oneHotPipeline("flag")

    // Original columns, without label / string columns, but with new vector encoded cols
    val assembleCols = Set(data.columns: _*) --
      Seq("label", "protocol_type", "service", "flag") ++
      Seq(protoTypeVecCol, serviceVecCol, flagVecCol)
    val assembler = new VectorAssembler().
      setInputCols(assembleCols.toArray).
      setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("scaledFeatureVector").
      setMaxIter(40).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(
      Array(protoTypeEncoder, serviceEncoder, flagEncoder, assembler, scaler, kmeans))

    pipeline
  }

  def computeCostOf0(range: Range): Seq[(Int, Double)] = {
    computeCostOf(range, clusteringScore0)
  }

  def computeCostOf1(range: Range): Seq[(Int, Double)] = {
    computeCostOf(range, clusteringScore1)
  }

  def computeCostOf2(range: Range): Seq[(Int, Double)] = {
    computeCostOfWithData(range, getData, clusteringScore2)
  }

  def computeCostOf(range: Range,
                    clusteringScore: (DataFrame, Int) => Double): Seq[(Int, Double)] = {
    val numericOnlyData = getNumericOnlyData
    range.map(k => (k, clusteringScore(numericOnlyData, k)))
  }

  def computeCostOfWithData(range: Range,
                            data: DataFrame,
                            clusteringScore: (DataFrame, Int) => Double): Seq[(Int, Double)] = {
    range.map(k => (k, clusteringScore(data, k)))
  }

  def oneHotPipeline(inputCol: String): (Pipeline, String) = {
    val indexer = new StringIndexer().
      setInputCol(inputCol).
      setOutputCol(inputCol + "_indexed")

    val encoder = new OneHotEncoder().
      setInputCol(inputCol + "_indexed").
      setOutputCol(inputCol + "_vec")

    val pipeline = new Pipeline().setStages(Array(indexer, encoder))

    (pipeline, inputCol + "_vec")
  }

  def entropy(counts: Iterable[Int]): Double = {
    val values = counts.filter(_ > 0)
    val n = values.map(_.toDouble).sum
    values.map { v =>
      val p = v / n
      -p * math.log(p)
    }.sum
  }

  def getClusterLabel(pipelineModel: PipelineModel): Dataset[(Int, String)] = {
    pipelineModel.transform(getData)
      .select("cluster", "label").as[(Int, String)]
  }

  def getAverageEntropyWeight(k: Int): Double = {
    val data = getData

    val pipelineModel = getPipelineWithCategoricalFeatures(data, k).fit(data)

    val weightedClusterEntropy = getClusterLabel(pipelineModel)
      .groupByKey { case (cluster, _) => cluster } // group by cluster name
      .mapGroups { case (_, clusterLabels) => // get all labels per cluster

        // labels of a specific cluster
        val labels = clusterLabels
          .map { case (_, label) => label }
          .toSeq

        // counts number of elements in each label of the cluster
        val labelCounts = labels.groupBy(identity).values.map(_.size)

        // weight cluster entropy : number of labels in cluster multiplied by entropy
        labels.size * entropy(labelCounts)
      }.collect()

    weightedClusterEntropy.sum / data.count()
  }

  def fitModel(k: Int): KMeansModel = {
    val data = getData

    val pipeline = getPipelineWithCategoricalFeatures(data, k)

    val kMeansModel = fitData(pipeline, data)

    kMeansModel
  }

  def getCountByClusterLabel(k: Int): Dataset[Row]= {
    val data = getData

    getPipelineWithCategoricalFeatures(data, k).fit(data).transform(data).
      select("cluster", "label").
      groupBy("cluster", "label").count().
      orderBy("cluster", "label")
  }

  def getAnomaliesFromData(k: Int): DataFrame = {
    val data = getData
    val pipelineModel = getPipelineWithCategoricalFeatures(data, k).fit(data)
    val kMeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    val originalCols = data.columns
    val clustered = pipelineModel.transform(data)
    val centroids = kMeansModel.clusterCenters

    val threshold = clustered.
      select("cluster", "scaledFeatureVector").as[(Int, Vector)]
      .map { case (cluster, vec) => Vectors.sqdist(centroids(cluster), vec) }
      .orderBy($"value".desc).take(100).last

    val anomalies = clustered.filter { row =>
      val cluster = row.getAs[Int]("cluster")
      val vec = row.getAs[Vector]("scaledFeatureVector")
      Vectors.sqdist(centroids(cluster), vec) >= threshold
    }.select(originalCols.head, originalCols.tail:_*)

    anomalies
  }
}
