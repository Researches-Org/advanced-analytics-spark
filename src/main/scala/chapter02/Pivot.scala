package chapter02

package chapter02

import org.apache.spark.sql.{DataFrame, functions}

object Pivot {

  /**
    * Pivot the summary DataFrame resulted from the describe() method.
    * Each row in the summary represents a metric and each column
    * represents a field.
    *
    * The result will be a new DataFrame where each row represents a field
    * and each column represents a metric.
    *
    * @param desc - DataFrame resulted from the describe method.
    * @return Pivot DataFrame
    */
  def pivotSummary(desc: DataFrame): DataFrame = {

    val schema = desc.schema

    import desc.sparkSession.implicits._

    // The first column in the summary is the metric.
    // The schema contains all the columns in the summary and the index is zero-based.
    // The 0 position contains the summary column and the other positions contain
    // the fields.
    val lf = desc.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => {
        (metric, schema(i).name, row.getString(i).toDouble)
      })
    }).toDF("metric", "field", "value")

    lf.groupBy("field")
      .pivot("metric", Seq("count", "mean", "stddev", "min", "max"))
      .agg(functions.first("value"))
  }

}

