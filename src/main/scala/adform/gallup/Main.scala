package adform.gallup

import adform.gallup.Constants.segmentUri
import adform.gallup.Constants.segmentIntersectionsUri

import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, Dataset, SparkSession}
import org.apache.spark.{SparkContext, SparkConf}
import com.mongodb.spark.config._
import org.bson.Document
/**
  * Created by atomkevich on 2/17/17.
  */
object Main extends App with GallupCsvReader  {
  val conf = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext: SparkSession = SparkSession.builder()
    .config(conf)
    .getOrCreate()

 // val segments: Dataset[Segment] = parseGallupSegments(sqlContext)
 // MongoSpark.save(segments.toDF(), WriteConfig(Map("uri" -> segmentUri)))



  // save segnent intersection affinity to MongoDB

  val segmentIntersections = parseGallupSegmentIntersections(sqlContext)
  MongoSpark.save(segmentIntersections.toDF(), WriteConfig(Map("uri" -> segmentIntersectionsUri)))
}
