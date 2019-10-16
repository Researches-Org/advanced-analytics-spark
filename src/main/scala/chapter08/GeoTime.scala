package chapter08

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.TimeUnit

import chapter08.GeoJsonProtocol._
import com.esri.core.geometry.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import spray.json._

import scala.collection.Map

class GeoTime(val spark: SparkSession, val filePrefix: String) extends Serializable {

  import spark.implicits._

  spark.udf.register("hours", hours)

  val file = filePrefix + "/target/taxidata/trip_data_1.csv"

  val geojson =
    scala.io.Source.fromFile(filePrefix + "/target/taxidata/nyc-boroughs.geojson")
      .mkString

  val features = geojson.parseJson.convertTo[FeatureCollection]

  val randomPoint: Point = new Point(-73.994499, 40.75066)

  val randomBorough = features.find(f => f.geometry.contains(randomPoint))

  val areaSortedFeatures =
    features.sortBy(f => {
      val borough = f("boroughCode").convertTo[Int]
      (borough, -f.geometry.area2D())
  })

  val bLookup = (x: Double, y: Double) => {
    val feature: Option[Feature] = bFeatures.value.find(f => {
      f.geometry.contains(new Point(x, y))
    })

    feature.map(f => {
      f("borough").convertTo[String]
    }).getOrElse("NA")
  }

  val boroughUDF = functions.udf(bLookup)

  val bFeatures = spark.sparkContext.broadcast(areaSortedFeatures)

  val sessions = getTaxiDone()
    .repartition($"license")
    .sortWithinPartitions($"license", $"pickupTime")
    .cache()

  var taxiRaw: DataFrame = null

  var taxiParsed: RDD[Either[Trip, (Row, Exception)]] = null

  var taxiGoodDs: Dataset[Trip] = null

  var cleanTaxiData: Dataset[Trip] = null

  var taxiDone: Dataset[Trip] = null

  val boroughDurations: DataFrame = sessions.mapPartitions(trips => {
    val iter: Iterator[Seq[Trip]] = trips.sliding(2) val viter = iter.
      filter(_.size == 2).
      filter(p => p(0).license == p(1).license)
    viter.map(p => boroughDuration(p(0), p(1)))
  }).toDF("borough", "seconds")

  val boroughDurationsStats = boroughDurations
    .where("seconds > 0 AND seconds < 60*60*4")
    .groupBy("borough")
    .agg(functions.avg("seconds"), functions.stddev("seconds"))

  def boroughDuration(t1: Trip, t2: Trip): (String, Long) = {
    val b = bLookup(t1.dropoffX, t1.dropoffY)
    val d = (t2.pickupTime - t1.dropoffTime) / 1000
    (b, d)
  }

  def getTaxiRaw(): DataFrame = {
    if (taxiRaw == null) {
      taxiRaw = spark.read.option("header", "true").csv(file)
      taxiRaw.cache()
    }

    taxiRaw
  }

  def getTaxiParsed(): RDD[Either[Trip, (Row, Exception)]]  = {
    if (taxiParsed == null) {
      taxiParsed = getTaxiRaw().rdd.map(safeParse)
    }

    taxiParsed
  }

  def getTaxiParsedCountByLeft(): Map[Boolean, Long] = {
    getTaxiParsed().map(_.isLeft).countByValue()
  }

  def getTaxiGoodDs(): Dataset[Trip] = {
    if (taxiGoodDs == null) {
      taxiGoodDs = getTaxiParsed().filter(_.isLeft).map(_.left.get).toDS()
      taxiGoodDs.cache()
    }

    taxiGoodDs
  }

  def getHistogramByHours() = {
    getTaxiGoodDs()
      .groupBy(hoursUdf($"pickupTime", $"dropoffTime").as("h"))
      .count()
      .sort("h")
  }

  def getTripsWithNegativeDuration(): Array[Trip] = {
    getTaxiGoodDs()
      .where(hoursUdf($"picupTime", $"dropoffTime") < 0)
      .collect()
  }

  def getCleanTaxiData(): Dataset[Trip] = {
    if (cleanTaxiData == null) {
      cleanTaxiData = getTaxiGoodDs().where(
        "hours(pickupTime, dropoffTime) BETWEEN 0 AND 3"
      )
    }

    cleanTaxiData
  }

  def getTaxiDataByBorough(): DataFrame = {
    getCleanTaxiData()
      .groupBy(boroughUDF($"dropoffX", $"dropoffY"))
      .count()
  }

  def getTaxiDone(): Dataset[Trip] = {
    if (taxiDone == null) {
      taxiDone = getCleanTaxiData().where(
      "dropoffX != 0 and dropoffY != 0 and pickupX != 0 and pickupY != 0"
      ).cache()
    }

    taxiDone
  }

  def parseTaxiTime(rr: RichRow, timeField: String): Long = {
    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    val optDt = rr.getAs[String](timeField)

    optDt.map(dt => formatter.parse(dt).getTime).getOrElse(0L)
  }

  def parseTaxiLoc(rr: RichRow, locField: String): Double = {
    rr.getAs[String](locField).map(_.toDouble).getOrElse(0.0)
  }

  def parse(row: Row): Trip = {
    val rr = new RichRow(row)

    Trip(
      license = rr.getAs[String]("hack_license").orNull,
      pickupTime = parseTaxiTime(rr, "pickup_datetime"),
      dropoffTime = parseTaxiTime(rr, "dropoff_datetime"),
      pickupX = parseTaxiLoc(rr, "pickup_longitude"),
      pickupY = parseTaxiLoc(rr, "pickup_latitude"),
      dropoffX = parseTaxiLoc(rr, "dropoff_longitude"),
      dropoffY = parseTaxiLoc(rr, "dropoff_latitude")
    )

  }

  def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    new Function[S, Either[T, (S, Exception)]] with Serializable {

      override def apply(s: S): Either[T, (S, Exception)] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right((s, e))
        }
      }

    }
  }

  def safeParse: Row => Either[Trip, (Row, Exception)] = safe(parse)

  private def hours: (Long, Long) => Long = (pickup: Long, dropoff: Long) => {
    TimeUnit.HOURS.convert(dropoff - pickup, TimeUnit.MILLISECONDS)
  }

  def hoursUdf: UserDefinedFunction = functions.udf(hours)

}
