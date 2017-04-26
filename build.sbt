name := "gallup_importer"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies  ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.0.0",
  "com.databricks" %% "spark-csv" % "1.5.0",
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)