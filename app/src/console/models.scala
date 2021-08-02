package console

import scala.util.Try

import io.circe.Decoder
import io.circe.generic.auto._

import reactivemongo.api.bson._



case class Item(id: Int)

// sealed trait ContentType
// case object Bundle extends ContentType
// case object Individual extends ContentType

sealed trait Package
object Package {
  implicit val decoder: Decoder[Package] = Decoder.decodeJson.emapTry { js =>
    js.as[BundlePackage].toTry.fold(_ => js.as[IndividualPackage].toTry, x => Try(x))
  }
}

final case class BundlePackage(bundleItems: Seq[Item], title: String) extends Package

final case class IndividualPackage(id: Int) extends Package

final case class CategoryResponse(
  category: String,
  nested: String,
  packages: Seq[Package],
  canRequestMore: Boolean,
  level: String
)

final case class DeviceToken(
  id: BSONObjectID,
  deviceToken: String,
  appId: String,
  stale: Option[Boolean] = None
)

object DeviceToken extends {

  implicit object DeviceTokenReader extends BSONDocumentReader[DeviceToken] {
    def readDocument(doc: BSONDocument) = for {
      id <- doc.getAsTry[BSONObjectID]("_id")
      deviceToken <- doc.getAsTry[String]("deviceToken")
      appId <- doc.getAsTry[String]("appId")
    } yield DeviceToken(id, deviceToken, appId, doc.getAsOpt[Boolean]("stale"))
  }
}