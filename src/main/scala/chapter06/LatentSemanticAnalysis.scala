package chapter06

import org.apache.spark.sql.SparkSession

class LatentSemanticAnalysis(private val spark: SparkSession) {

  import spark.implicits._

  /**
   * Calculate the Term Frequency versus the Inverse Document Frequency,
   * also known as TF-IDF.
   *
   * @param termFrequencyInDoc - term frequency in document.
   * @param totalTermsInDoc - total of terms in document.
   * @param termFrequencyInCorpus - term frequency in corpus.
   * @param totalDocs - total of documents.
   * @return TF-IDF.
   */
  def termDocWeight(termFrequencyInDoc: Int,
                    totalTermsInDoc:Int,
                    termFrequencyInCorpus: Int,
                    totalDocs: Int): Double = {

    val tf = termFrequencyInDoc.toDouble / totalTermsInDoc
    val docFreq = totalDocs.toDouble / termFrequencyInCorpus
    val idf = math.log(docFreq)

    tf * idf
  }

}
