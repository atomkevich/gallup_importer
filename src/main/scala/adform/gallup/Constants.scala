package adform.gallup

import com.typesafe.config.ConfigFactory

/**
  * Created by atomkevich on 2/17/17.
  */
object Constants {
  val conf = ConfigFactory.load

  val segmentsFile = conf.getString("segments.file")
  val segmentIntersectionsFile = conf.getString("segment_intersections.file")

  val segmentUri = conf.getString("mongo.segment_uri")
  val segmentIntersectionsUri = conf.getString("mongo.segment_intersections_uri")
}
