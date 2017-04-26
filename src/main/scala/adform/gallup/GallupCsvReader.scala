package adform.gallup

import adform.gallup.Constants._
import org.apache.spark.sql.{SQLImplicits, Dataset, Column, SparkSession}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
import Segment._
/**
  * Created by atomkevich on 2/17/17.
  */
trait GallupCsvReader {
  val mediaList = List(
    "Ekstra Bladet Print (H)",
    "Ekstra Bladet Print (S)",
    "JyllandsPosten Print (H)",
    "JyllandsPosten Print (S)",
    "Politiken Print (H)",
    "Politiken Print (S)",
    "Politikens Lokalaviser Jylland midtuge",
    "Politikens Lokalaviser Sjælland midtuge",
    "Politikens Lokalaviser Sjælland weekend",
    "Politikens Lokalaviser Total"
  )

  def mediaAffinity(implicit sqlContext: SparkSession): Column = {
    import sqlContext.implicits._
    coalesce(
      mediaList.map(c => when($"media" === c, col(c)).otherwise(lit(null))): _*)
  }


  def parseGallupSegments(implicit sqlContext: SparkSession) = {

    import sqlContext.implicits._

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(segmentsFile)


    val dfWithMediaName = df.withColumn(media, explode(array(mediaList.map(lit):_*)))
    val dfWithAffinity = dfWithMediaName.withColumn(affinity, mediaAffinity)
    val dfWithCategoryName = dfWithAffinity.withColumn(categoryName, categoryNameUDF(col("Sub Category")))

    val segments = dfWithCategoryName.select(
      col(categoryName),
      col("Sub Category").alias(subCategoryName),
      col("Segment").alias(segmentName),
      col(media).alias(media),
      col(affinity).alias(affinity))
      .where($"Value" === "StrukDæk% lodret")

    segments.rdd.foreach(println)
    segments.map(toSegment)
  }


  def parseGallupSegmentIntersections(implicit sqlContext: SparkSession) = {

    import sqlContext.implicits._
    import IntersectionSegment._

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(segmentIntersectionsFile)

    df.printSchema()
    val total = df.select(
      "Total",
      "Ekstra Bladet Print (H)",
      "Ekstra Bladet Print (S)",
      "JyllandsPosten Print (H)",
      "JyllandsPosten Print (S)",
      "Politiken Print (H)",
      "Politiken Print (S)",
      "Politikens Lokalaviser Jylland midtuge",
      "Politikens Lokalaviser Sjælland midtuge",
      "Politikens Lokalaviser Sjælland weekend",
      "Politikens Lokalaviser Total"

    )
      .where($"Køn" === "Total")
      .where($"Region" === "Total")
      .where($"Alder" === "Total")
      .where($"Erhverv" === "Total")
      .where($"Ejer-/lejerbolig" === "Total")
      .where($"Husstandsindkomst" === "Total")
      .where($"Hjemmeboende børns alder" === "Total").first()


    val dfWithMediaName = df.withColumn(media, explode(array(mediaList.map(lit):_*)))
    val dfWithAffinity = dfWithMediaName.withColumn(affinity, mediaAffinity)

    dfWithAffinity.map(toIntersectionSegment(_, total))

  }



  val categoryNameUDF = udf((subCategory: String) => categoryNameF(subCategory))
}
