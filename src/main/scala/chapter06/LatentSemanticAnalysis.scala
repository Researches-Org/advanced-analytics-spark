package chapter06

import edu.umd.cloud9.collection.XMLInputFormat
import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.sql.{Dataset, SparkSession}

class LatentSemanticAnalysis(private val spark: SparkSession) {

  import spark.implicits._

  val path = "/chapter06/wikidump.xml"

  @transient var conf: Configuration = null

  private var rawXmls: Dataset[String] = null

  private var docText: Dataset[(String, String)] = null

  def getConfiguration: Configuration = {
    if (conf == null) {
      conf = new Configuration()
      conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
      conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
    }

    conf
  }

  def getRawXmls(filePrefix: String): Dataset[String] = {
    if (rawXmls == null) {
      val kvs = spark.sparkContext
        .newAPIHadoopFile(filePrefix + "/" + path,
          classOf[XMLInputFormat],
          classOf[LongWritable],
          classOf[Text],
          getConfiguration)

      rawXmls = kvs.map(_._2.toString).toDS()
    }

    rawXmls
  }

  def getDocText(filePrefix: String): Dataset[(String, String)] = {
    if (docText == null) {
      docText = getRawXmls(filePrefix)
        .filter(_ != null)
        .flatMap(wikiXmlToPlainText)
    }

    docText
  }

  def wikiXmlToPlainText(pageXml: String): Option[(String, String)] = {
    val hackedPageXml = pageXml.replaceFirst("<text xml:space=\"preserve\" bytes=\"\\d+\">",
      "<text xml:space=\"preserve\">")

    val page = new EnglishWikipediaPage()

    WikipediaPage.readPage(page, hackedPageXml)

    if (page.isEmpty) {
      None
    } else {
      Some((page.getTitle, page.getContent))
    }
  }

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
