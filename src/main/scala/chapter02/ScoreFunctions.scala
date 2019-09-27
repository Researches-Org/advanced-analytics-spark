package chapter02

import org.apache.spark.sql.DataFrame

object ScoreFunctions extends java.io.Serializable {

  def scoreMatchData(md: MatchData): Double = {

    var v = 0.0
    if (md.cmp_lname_c2.nonEmpty
      && md.cmp_fname_c1.nonEmpty
      && md.cmp_lname_c2.get > 0.8
      && md.cmp_fname_c1.get > 0.2) {
      v = md.cmp_lname_c2.get + md.cmp_fname_c1.get
    }

    (Score(v + md.cmp_fname_c1.getOrElse(0.0) + md.cmp_lname_c1.getOrElse(0.0))
      + md.cmp_plz
      + md.cmp_by
      + md.cmp_bd
      + md.cmp_bm
      ).value
  }

  def crossTabs(score: DataFrame, threshold: Double): DataFrame = {
    score
      .selectExpr(s"score >= $threshold as above", "is_match")
      .groupBy("above")
      .pivot("is_match", Seq("true", "false"))
      .count()
  }

}
