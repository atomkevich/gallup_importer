package adform

import org.apache.spark.sql.Row

import scala.language.implicitConversions
import scala.math.BigDecimal.RoundingMode

/**
  * Created by atomkevich on 2/19/17.
  */
package object gallup {
  case class Segment(categoryName: String, subCategoryName: String,
                     segmentName: String, media: String, affinity: Double = 0.0, source: String = "Gallup")
  case object Segment {
    val categoryName = "categoryName"
    val subCategoryName = "subCategoryName"
    val segmentName = "segmentName"
    val media = "media"
    val affinity = "affinity"

    def toSegment(row: Row): Segment =
      Segment(
        row.getAs[String](categoryName),
        row.getAs[String](subCategoryName),
        row.getAs[String](segmentName),
        row.getAs[String](media),
        row.getAs[String](affinity).toDouble
      )
  }




  case class IntersectionSegment(segments: List[Segment], depth: Int, affinity: Double,
                                 media: String)

  case object IntersectionSegment {

    val existedSerments= List(
      "Køn",
      "Region",
      "Alder",
      "Erhverv",
      "Ejer-/lejerbolig",
      "Husstandsindkomst",
      "Hjemmeboende børns alder"
    )
    val segmentPrefix = ".svar"
    val media = "media"
    val affinity = "affinity"

    def toIntersectionSegment(row: Row, total: Row): IntersectionSegment = {
      val mediaName = row.getAs[String](media)
      val segments = existedSerments.flatMap(sName => {
        row.getAs[String](sName) match {
          case "Total" => None
          case subCategoryName => {
            val segmentName = row.getAs[String](s"$sName$segmentPrefix")
            Some(Segment(categoryNameF(subCategoryName), subCategoryName, segmentName, mediaName))
          }
        }
      })
      val totalMediaAffinity = total.getAs[String](mediaName).toDouble
      val mediaAffinity = row.getAs[String](affinity).toDouble

      IntersectionSegment(
        segments = segments,
        depth = segments.size,
        affinity = round((mediaAffinity/ totalMediaAffinity) * 100),
        media = mediaName)
    }

  }

  def round(value: Double, scale:Int = 2) = {
   BigDecimal(value).setScale(scale, RoundingMode.HALF_UP).toDouble
  }
  def categoryNameF(subCategoryName: String) = subCategoryName match {
    case "Region - kommunalreform" => "Geo"
    case ee => "Demographics"
  }
}
