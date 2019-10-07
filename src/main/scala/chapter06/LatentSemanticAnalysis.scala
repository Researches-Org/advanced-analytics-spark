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
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, IDFModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

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

  private var termsDf: DataFrame = null

  private var filteredTermsDf: DataFrame = null

  private var model: CountVectorizerModel = null

  private var docTermFreqs: DataFrame = null

  private var idfModel: IDFModel = null

  private var docTermMatrix: DataFrame = null

  private var vecRdd: RDD[MLLibVector] = null

  private var svd: SingularValueDecomposition[RowMatrix, Matrix] = null

  val VS: breeze.linalg.DenseMatrix[Double] = multiplyByDiagonalMatrix(getSvd().V, getSvd().s)

  val normalizedVS: breeze.linalg.DenseMatrix[Double] = rowsNormalized(VS)

  val US: RowMatrix = multiplyByDiagonalRowMatrix(getSvd().U, getSvd().s)

  val normalizedUS: RowMatrix = distributedRowsNormalized(US)

  var idTerms: Map[String, Int] = null

  var idDocs: Map[String, Long] = null

  def getIdTerms(): Map[String, Int] = {
    if (idTerms == null) {
      idTerms = getTermIds().zipWithIndex.toMap
    }

    idTerms
  }

  def getIdDocs(): Map[String, Long] = {
    if (idDocs == null) {
      idDocs = getDocIds().map(_.swap)
    }

    idDocs
  }

  def topTermsForTerm(termId: Int): Seq[(Double, Int)] = {
    val rowVec = normalizedVS(termId, ::).t
    val termScores = (normalizedVS * rowVec).toArray.zipWithIndex

    termScores.sortBy(-_._1).take(10)
  }

  def printTopTermsForTerm(term: String): Unit = {
    val idWeights = topTermsForTerm(getIdTerms()(term))
    println(idWeights.map { case (score, id) =>
      (getTermIds()(id), score)
    }.mkString(", "))
  }

  def topDocsForDoc(docId: Long): Seq[(Double, Long)] = {
    val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap)
      .lookup(docId).head.toArray
    val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
    val docScores = normalizedUS.multiply(docRowVec)
    val allDocWeights = docScores.rows.map(_.toArray(0)).
      zipWithUniqueId()
    allDocWeights.filter(!_._1.isNaN).top(10)
  }

  def printTopDocsForDoc(doc: String): Unit = {
    val idWeights = topDocsForDoc(getIdDocs()(doc))
    println(idWeights.map { case (score, id) =>
      (getDocIds()(id), score)
    }.mkString(", "))
  }

  def topDocsForTerm(termId: Int): Seq[(Double, Long)] = {
    val rowArr = (0 until getSvd().V.numCols).
      map(i => getSvd().V(termId, i)).toArray
    val rowVec = Matrices.dense(rowArr.length, 1, rowArr)
    val docScores = US.multiply(rowVec)
    val allDocWeights = docScores.rows.map(_.toArray(0)).
      zipWithUniqueId()
    allDocWeights.top(10)
  }

  def printTopDocsForTerm(term: String): Unit = {
    val idWeights = topDocsForTerm(getIdTerms()(term))
    println(idWeights.map { case (score, id) =>
      (getDocIds()(id), score)
    }.mkString(", "))
  }

  /**
   * Returns a distributed matrix where each row is divided by its length.
   */
  def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map { vec =>
      val array = vec.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      Vectors.dense(array.map(_ / length))
    })
  }

  /**
   * Returns a matrix where each row is divided by its length.
   */
  def rowsNormalized(mat: breeze.linalg.DenseMatrix[Double]): breeze.linalg.DenseMatrix[Double] = {
    val newMat = new breeze.linalg.DenseMatrix[Double](mat.rows, mat.cols)
    for (r <- 0 until mat.rows) {
      val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).foreach(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }

  /**
   * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
   */
  def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    })
  }

  /**
   * Finds the product of a dense matrix and a diagonal matrix represented by a vector.
   * Breeze doesn't support efficient diagonal representations, so multiply manually.
   */
  def multiplyByDiagonalMatrix(mat: Matrix, diag: MLLibVector): breeze.linalg.DenseMatrix[Double] = {
    val sArr = diag.toArray
    new breeze.linalg.DenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs { case ((r, c), v) => v * sArr(c) }
  }

  def getTopTermsInTopConcepts(
                              svd: SingularValueDecomposition[RowMatrix, Matrix],
                              numConcepts: Int,
                              numTerms: Int,
                              termIds: Array[String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray

    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map {
        case (score, id) => (getTermIds()(id), score)
      }
    }

    topTerms
  }

  def getTopDocsInTopConcepts(
                               svd: SingularValueDecomposition[RowMatrix, Matrix],
                               numConcepts: Int,
                               numDocs: Int,
                               docIds: Map[Long, String]
                             ): Seq[Seq[(String, Double)]] = {
    val u = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()

    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId()
      topDocs += docWeights.top(numDocs).map {
        case (score, id) => (getDocIds()(id), score)
      }
    }

    topDocs
  }

  def printTopTermsAndDocs(numConcepts: Int, numTerms: Int, numDocs: Int) = {
    val svd = getSvd()
    val topConceptTerms = getTopTermsInTopConcepts(svd, numConcepts, numTerms, getTermIds())
    val topConceptDocs = getTopDocsInTopConcepts(svd, numConcepts, numDocs, getDocIds())

    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
    }
  }

  def getVecRdd(): RDD[MLLibVector] = {
    if (vecRdd == null) {
      vecRdd = getDocTermMatrix()
        .select("tfidfVec")
        .rdd.map {
          row => Vectors.fromML(row.getAs[org.apache.spark.ml.linalg.Vector]("tfidfVec"))
      }
      vecRdd.cache()
    }

    vecRdd
  }

  def getSvd(): SingularValueDecomposition[RowMatrix, Matrix] = {
    if (svd == null) {
      val mat = new RowMatrix(getVecRdd())

      svd = mat.computeSVD(1000, computeU = true)
    }

    svd
  }

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

  def getTermsDf(): DataFrame = {
    if (termsDf == null) {
      termsDf = getTerms().toDF("title", "terms")
    }
    termsDf
  }

  def getConfiguration(): Configuration = {
    if (conf == null) {
      conf = new Configuration()
      conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
      conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
    }

    conf
  }

  def getFilteredTermsDf(): DataFrame = {
    if (filteredTermsDf == null) {
      filteredTermsDf = getTermsDf().where(functions.size($"terms") > 1)
    }

    filteredTermsDf
  }

  def createCountVectorizer(): CountVectorizer = {
    val numTerms = 20000

    new CountVectorizer()
      .setInputCol("terms")
      .setOutputCol("termFreqs")
      .setVocabSize(numTerms)
  }

  def createIdf(): IDF = {
    new IDF().setInputCol("termFreqs").setOutputCol("tfidfVec")
  }

  def fitIdfModel(): IDFModel = {
    if (idfModel == null) {
      idfModel = createIdf().fit(getDocTermFreqs())
    }

    idfModel
  }

  def getDocTermMatrix(): DataFrame = {
    if (docTermMatrix == null) {
      docTermMatrix = fitIdfModel()
        .transform(getDocTermFreqs())
        .select("title", "tfidfVec")
    }

    docTermMatrix
  }

  def fitModel(): CountVectorizerModel = {
    if (model == null) {
      val countVectorizer = createCountVectorizer()

      model = countVectorizer.fit(getFilteredTermsDf())
    }

    model
  }

  def getTermIds(): Array[String] = {
    fitModel().vocabulary
  }

  def getDocIds(): Map[Long, String] = {
    getDocTermFreqs().rdd
      .map(_.getString(0))
      .zipWithUniqueId()
      .map(_.swap)
      .collect()
      .toMap
  }

  def getDocTermFreqs(): DataFrame = {
    if (docTermFreqs == null) {
      docTermFreqs = fitModel().transform(getFilteredTermsDf())
      docTermFreqs.cache()
    }

    docTermFreqs
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

    if (page.isEmpty || !page.isArticle || page.isRedirect || page.getTitle.contains("(disambiguation)")) {
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
