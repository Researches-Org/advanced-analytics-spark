name := "Sample Project"

version := "1.0"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "Cloudera" at "ttps://repository.cloudera.com/artifactory/cloudera-repos",
  "Cludera-local" at "https://repository.cloudera.com/artifactory/libs-release-local"
)

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "org.apache.spark" %% "spark-sql" % "2.4.4" ,
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
  "org.apache.hadoop" % "hadoop-client" % "2.7.7",
  "edu.umd" % "cloud9" % "1.5.0",
  "info.bliki.wiki" % "bliki-core" % "3.0.19",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1" classifier "models",
  "com.esri.geometry" % "esri-geometry-api" % "1.2.1",
  "com.cloudera.science" % "geojson" % "0.1.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.mockito" % "mockito-core" % "3.1.0" % "test"
)