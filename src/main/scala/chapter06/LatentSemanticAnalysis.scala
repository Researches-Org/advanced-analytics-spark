package chapter06

import java.util.Properties
import java.util.function.Consumer

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.umd.cloud9.collection.XMLInputFormat
import edu.umd.cloud9.collection.wikipedia.WikipediaPage
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

class LatentSemanticAnalysis(private val spark: SparkSession,
                             private val filePrefix: String) extends java.io.Serializable {

  import spark.implicits._

  val path = "/chapter06/wikidump.xml"

  val stopWordsFile = "/chapter06/stopwords.txt"

  @transient var conf: Configuration = null

  private var rawXmls: Dataset[String] = null

  private var docText: Dataset[(String, String)] = null

  private var stopWords: Set[String] = null

  private var terms: Dataset[(String, Seq[String])] = null

  def getStopWords(): Set[String] = {
    if (stopWords == null) {
      stopWords = scala.io.Source.fromFile(filePrefix + "/" + stopWordsFile).getLines().toSet
    }

    stopWords
  }

  def broadcastStopWords(): Broadcast[Set[String]] = {
    spark.sparkContext.broadcast(getStopWords())
  }

  def getTerms(): Dataset[(String, Seq[String])] = {

    if (terms == null) {
      val bStopWords = broadcastStopWords()

      terms = getDocText().mapPartitions {
        iter =>
          val pipeline = createNLPPipeline()
          iter.map {
            case (title, contents) =>
              (title, plainTextToLemmas(contents, bStopWords.value, pipeline))
          }
      }
    }

    terms
  }

  def getConfiguration(): Configuration = {
    if (conf == null) {
      conf = new Configuration()
      conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
      conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
    }

    conf
  }

  def getRawXmls(): Dataset[String] = {
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

  def getDocText(): Dataset[(String, String)] = {
    if (docText == null) {
      docText = getRawXmls()
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

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")

    new StanfordCoreNLP(props)
  }

  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }

  def plainTextToLemmas(text: String,
                        stopWords: Set[String],
                        pipeline: StanfordCoreNLP): Seq[String] = {

    val doc = new Annotation(text)
    pipeline.annotate(doc)

    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])


    for (sentence <- convertToScala(sentences);
         token <- convertToScala(sentence.get(classOf[TokensAnnotation]))) {

      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }

    lemmas
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

  def convertToScala[T](list: java.util.List[T]): Seq[T] = {
    val buffer: ArrayBuffer[T] = new ArrayBuffer[T]()

    list.forEach(new Consumer[T] {
      override def accept(t: T): Unit = buffer += t
    })

    buffer
  }

}

