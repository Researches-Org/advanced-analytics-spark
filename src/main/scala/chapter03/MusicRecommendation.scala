package chapter03

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{coalesce, count, lit, max, mean, min, sum}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MusicRecommendation extends java.io.Serializable {

  val artistAliasFile =  "/Users/mmenezes/Documents/workspace/spark/spark-music-recommender/data/artist_alias.txt"

  val artistFile = "/Users/mmenezes/Documents/workspace/spark/spark-music-recommender/data/artist_data.txt"

  val userArtistFile = "/Users/mmenezes/Documents/workspace/spark/spark-music-recommender/data/user_artist_data.txt"

  var userArtistData: Dataset[String] = null

  var userArtistDF: DataFrame = null

  var userArtistStatistics: DataFrame = null

  var artistData: Dataset[String] = null

  var artistByIdDF: DataFrame = null

  var artistAlias: Dataset[String] = null

  var artistAliasMap: Map[Int, Int] = null

  var counts: DataFrame = null

  var trainDataAndCVData: (DataFrame, DataFrame) = null

  def predictMostListened(train: DataFrame)(allData: DataFrame): DataFrame = {
    val listenCounts = train
      .groupBy("artist")
      .agg(sum("count").as("prediction"))
      .select("artist", "prediction")

    allData
      .join(listenCounts, Seq("artist"), "left_outer")
      .select("user", "artist", "prediction")
  }

  def evalHyperParam(spark: SparkSession,
                     bArtistAlias: Broadcast[Map[Int, Int]],
                     bAllArtistIds: Broadcast[Array[Int]]): Seq[(Double, (Int, Double, Double))] = {
    for (rank <- Seq(5, 30);
         regParam <- Seq(4.0, 0.0001);
         alpha <- Seq(1.0, 40.0))
      yield {
        val model = new ALS()
          .setSeed(Random.nextLong())
          .setImplicitPrefs(true)
          .setRank(rank)
          .setRegParam(regParam)
          .setAlpha(alpha)
          .setMaxIter(20)
          .setUserCol("user")
          .setItemCol("artist")
          .setRatingCol("count")
          .setPredictionCol("prediction")
          .fit(getTrainDataAndCVData(spark, bArtistAlias)._1)

        val auc = areaUnderCurve(spark, MusicRecommendation.getTrainDataAndCVData(spark, bArtistAlias)._2, bAllArtistIds, model.transform)

        model.userFactors.unpersist()
        model.itemFactors.unpersist()

        (auc, (rank, regParam, alpha))
      }
  }

  def areaUnderCurve(spark: SparkSession,
                     positiveData: DataFrame,
                     bAllArtistIDs: Broadcast[Array[Int]],
                     predictFunction: (DataFrame => DataFrame)): Double = {

    import spark.implicits._

    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive".
    // Make predictions for each of them, including a numeric score
    val positivePredictions = predictFunction(positiveData.select("user", "artist")).
      withColumnRenamed("prediction", "positivePrediction")

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other artists, excluding those that are "positive" for the user.
    val negativeData = positiveData.select("user", "artist").as[(Int,Int)].
      groupByKey { case (user, _) => user }.
      flatMapGroups { case (userID, userIDAndPosArtistIDs) =>
        val random = new Random()
        val posItemIDSet = userIDAndPosArtistIDs.map { case (_, artist) => artist }.toSet
        val negative = new ArrayBuffer[Int]()
        val allArtistIDs = bAllArtistIDs.value
        var i = 0
        // Make at most one pass over all artists to avoid an infinite loop.
        // Also stop when number of negative equals positive set size
        while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
          val artistID = allArtistIDs(random.nextInt(allArtistIDs.length))
          // Only add new distinct IDs
          if (!posItemIDSet.contains(artistID)) {
            negative += artistID
          }
          i += 1
        }
        // Return the set with user ID added back
        negative.map(artistID => (userID, artistID))
      }.toDF("user", "artist")

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeData).
      withColumnRenamed("prediction", "negativePrediction")

    // Join positive predictions to negative predictions by user, only.
    // This will result in a row for every possible pairing of positive and negative
    // predictions within each user.
    val joinedPredictions = positivePredictions.join(negativePredictions, "user").
      select("user", "positivePrediction", "negativePrediction").cache()

    // Count the number of pairs per user
    val allCounts = joinedPredictions.
      groupBy("user").agg(count(lit("1")).as("total")).
      select("user", "total")
    // Count the number of correctly ordered pairs per user
    val correctCounts = joinedPredictions.
      filter($"positivePrediction" > $"negativePrediction").
      groupBy("user").agg(count("user").as("correct")).
      select("user", "correct")

    // Combine these, compute their ratio, and average over all users
    val meanAUC = allCounts.join(correctCounts, Seq("user"), "left_outer").
      select($"user", (coalesce($"correct", lit(0)) / $"total").as("auc")).
      agg(mean("auc")).
      as[Double].first()

    joinedPredictions.unpersist()

    meanAUC
  }

  def getArtistsFromRecommendations(spark: SparkSession, recommendations: DataFrame): DataFrame = {

    import spark.implicits._

    val recommendedArtistIds = recommendations.select("artist").as[Int].collect()

    filterArtistById(spark, recommendedArtistIds)

  }

  def makeRecommendations(spark: SparkSession, model: ALSModel, userId: Int, howMany: Int): DataFrame = {
    import spark.implicits._

    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    val toRecommend = model.itemFactors
      .select($"id".as("artist"))
      .withColumn("user", functions.lit(userId))

    model.transform(toRecommend)
      .select("artist", "prediction")
      .orderBy($"prediction".desc)
      .limit(howMany)
  }

  def getArtistsByUser(spark: SparkSession, userId: Int, bArtistAlias: Broadcast[Map[Int, Int]]): Dataset[Row] = {
    import spark.implicits._

    val existingArtistIds = buildCounts(spark, bArtistAlias)
      .filter($"user" === userId)
      .select("artist").as[Int].collect()

    getArtistByIdDF(spark).filter($"id" isin(existingArtistIds:_*))
  }

  def buildModel(spark: SparkSession, bArtistAlias: Broadcast[Map[Int, Int]]): ALSModel = {
    new ALS()
      .setSeed(Random.nextLong())
      .setImplicitPrefs(true)
      .setRank(10)
      .setRegParam(0.01)
      .setAlpha(1.0)
      .setMaxIter(5)
      .setUserCol("user")
      .setItemCol("artist")
      .setRatingCol("count")
      .setPredictionCol("prediction")
      .fit(getTrainDataAndCVData(spark, bArtistAlias)._1)
  }

  def broadcastArtistAlias(spark: SparkSession): Broadcast[Map[Int, Int]] = {
    spark.sparkContext.broadcast(MusicRecommendation.getArtistAliasMap(spark))
  }

  def getTrainDataAndCVData(spark: SparkSession, bArtistAlias: Broadcast[Map[Int, Int]]): (DataFrame, DataFrame) = {
    val counts = buildCounts(spark, bArtistAlias)

    if (trainDataAndCVData == null) {
      val Array(trainData, cvData) = counts.randomSplit(Array(0.9, 0.1))
      trainDataAndCVData = (trainData, cvData)
      trainDataAndCVData._1.cache()
      trainDataAndCVData._2.cache()
    }

    trainDataAndCVData
  }

  def getAllArtistIds(spark: SparkSession, bArtistAlias: Broadcast[Map[Int, Int]]): Array[Int] = {
    import spark.implicits._

    val counts = buildCounts(spark, bArtistAlias)

    counts.select("artist").as[Int].distinct().collect()
  }

  def buildCounts(spark: SparkSession, bArtistAlias: Broadcast[Map[Int, Int]]): DataFrame = {
    if (counts == null) {
      import spark.implicits._

      counts = getRawUserArtistData(spark).map { line =>
        val Array(userId, artistId, count) = line.split(' ').map(_.toInt)

        val finalArtistId = bArtistAlias.value.getOrElse(artistId, artistId)

        (userId, finalArtistId, count)
      }.toDF("user", "artist", "count").cache()
    }

    counts
  }

  def getRawArtistData(spark: SparkSession): Dataset[String] = {
    if (artistData == null) {
      artistData = spark
        .read
        .textFile(artistFile)
        .cache()
    }

    artistData
  }

  def getRawArtistAlias(spark: SparkSession): Dataset[String] = {
    if (artistAlias == null) {
      artistAlias = spark
        .read
        .textFile(artistAliasFile)
        .cache()
    }

    artistAlias
  }

  def getArtistAliasMap(spark: SparkSession): Map[Int, Int] = {

    import spark.implicits._

    if (artistAliasMap == null) {
      artistAliasMap = getRawArtistAlias(spark).flatMap { line =>
        val Array(artist, alias) = line.split('\t')
        if (artist.isEmpty) {
          None
        } else {
          Some((artist.toInt, alias.toInt))
        }

      }.cache().collect().toMap
    }

    artistAliasMap
  }

  def filterArtistById(spark: SparkSession, ids: Array[Int]): DataFrame = {
    import spark.implicits._

    getArtistByIdDF(spark).filter($"id".isInCollection(ids))
  }

  def getArtistByIdDF(spark: SparkSession): DataFrame = {

    import spark.implicits._

    if (artistByIdDF == null) {
      artistByIdDF = getRawArtistData(spark)
        .flatMap {
          line =>
            val (id, name) = line.span(_ != '\t')
            if (name.isEmpty) {
              None
            } else {
              try {
                Some((id.toInt, name.trim))
              } catch {
                case _: NumberFormatException => None
              }
            }
        }.toDF("id", "name").cache()
    }

    artistByIdDF
  }

  def getRawUserArtistData(spark: SparkSession): Dataset[String] = {
    if (userArtistData == null) {
      userArtistData = spark
        .read
        .textFile(userArtistFile)
        .cache()
    }

    userArtistData
  }

  def getUserArtistDF(spark: SparkSession): DataFrame = {

    import spark.implicits._

    if (userArtistDF == null) {
      userArtistDF = getRawUserArtistData(spark).map {
        line =>
          val Array(user, artist, _*) = line.split(" ")
          (user.toInt, artist.toInt)
      }.toDF("user", "artist").cache()
    }

    userArtistDF
  }

  def getUserArtistStatistics(spark: SparkSession): DataFrame = {

    if (userArtistStatistics == null) {
      userArtistStatistics = getUserArtistDF(spark)
        .agg(min("user"), max("user"), min("artist"), max("artist"))
        .cache()
    }

    userArtistStatistics
  }

}
