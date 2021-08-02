package console

import scala.util.Try
import scala.concurrent.duration.{Duration => ScDuration, _}
import java.time.temporal.ChronoUnit
import java.io.File

import zio._
import zio.console._
import zio.clock._
import zio.stream.{ZStream, ZSink}
import zio.config.magnolia.DeriveConfigDescriptor
import zio.config.ZConfig
import zio.duration.Duration
import zio.logging._
import zio.config.syntax._
import zio.config.typesafe.TypesafeConfig

import sttp.client3.SttpBackend
import sttp.capabilities.zio._
import sttp.capabilities._
import sttp.client3.asynchttpclient.zio.AsyncHttpClientZioBackend

import io.circe._
import io.circe.syntax._
import io.circe.optics.JsonPath._
import io.circe.bson._
import io.circe.parser._

import reactivemongo.api._
import reactivemongo.api.bson._
import reactivemongo.api.bson.collection._

import aliases._



object aliases {
  type SttpClient = Has[SttpBackend[Task, ZioStreams with WebSockets]]
  type MongoConf = Has[MongoConfig]
}

object constants {

  val categories = Seq(
    "Meditation", "Music", "Sleep Stories", "Sounds"
  )

  val ageTag = Seq("Adults")

  val timeOfDays = Seq("Day","Night")

  val deviceId = "B5D3F300-3A93-480A-B7BB-B056100A8659"

}


object stream {
  def categoryPackages(xs: Seq[(String, String, String)]) = {
    ZStream.fromIterable(xs)
      .mapConcatM {
        case triple @ (category, ageTag, timeOfDay) => api.getDataOfCategory(category, ageTag, timeOfDay).map(_.map(triple -> _))
      }
      .mapConcatM {
        case (_, categoryResponse) if !categoryResponse.canRequestMore => ZIO.succeed(categoryResponse.packages)
        case ((category, ageTag, timeOfDay), categoryResponse) =>
          api.getDataOfSubCategory(category, categoryResponse.nested, ageTag, timeOfDay)
            .map(_.packages)
      }
  }




  def items(pkg: Package) = {

    pkg match {
      case BundlePackage(bundleItems, title) =>
        ZStream.fromIterable(bundleItems)
          .mapMPar(5)(item => api.getData(item.id))
          .map { obj =>

            val titleWithParentTitle = root.metaData.title.string.modify(title + " - " + _)
            titleWithParentTitle(obj)
          }
      case IndividualPackage(id) => ZStream(id).mapM(id => api.getData(id))
    }
  }

  def deviceTokens() = {
    val initState: Option[BSONObjectID] = None
    val pageSize = 1000
    ZStream.unfoldM(initState) { maybeLastId =>
      db.fetch(maybeLastId, pageSize)
        .map { xs =>
          if (xs.isEmpty) None else Some(xs -> xs.lastOption.map(_.id))
        }
    }
  }

}

object jsonutil {
  def renameField(json: Json, fieldToRename: String, newName: String): Json =
    (for {
      value <- json.hcursor.downField(fieldToRename).focus
      newJson <- json.mapObject(_.add(newName, value)).hcursor.downField(fieldToRename).delete.top
    } yield newJson).getOrElse(json)
}

object mongo {
  import reactivemongo.api.{ AsyncDriver, MongoConnection }

  lazy val layer = {
    ZLayer.fromAcquireRelease(
      for {
        config <- ZIO.access[MongoConf](_.get)
        driver = new AsyncDriver
        conStr = s"mongodb://${config.user}:${config.pwd}@${config.host}:${config.port}/?readPreference=primary&ssl=false&authSource=${config.database}"
        conn <- ZIO.fromFuture(implicit ec => driver.connect(conStr))
      } yield conn
    )(driver => ZIO.fromFuture(implicit ec => driver.close()(1.minute)).ignore)
  }
}

object db {


  import reactivemongo.api.bson._
  import reactivemongo.api.bson.collection._


  trait Service {
    def upsert(json: Json): ZIO[Logging, Throwable, Unit]
    def fetch(maybeLastId: Option[BSONObjectID], pageSize: Int): ZIO[Logging, Throwable, Array[DeviceToken]]
  }

  object Service {

    lazy val live = {
      ZLayer.fromService { (pair: (DB, MongoConfig)) =>
        val (db, conf) = pair
        lazy val collection = db.collection(conf.collection)
        lazy val deviceTokens = db.collection("deviceTokens")
        new Service {
          private def json2BSONDoc(json: Json) = ZIO.fromEither(jsonToBson(json))
            .collect(new IllegalArgumentException("Inserting Json  must be an Object")) { case doc: BSONDocument => doc }
          override def upsert(json: Json) = {
            val action = for {
              bdoc <- json2BSONDoc(json)
              id <- ZIO.fromTry(Try(root.id.int.getOption(json).get))
              _ <- log.info(s"Loading ${id}")
              result <- ZIO.fromFuture { implicit ec =>
                collection.findAndUpdate(
                  BSONDocument("id" -> id),
                  BSONDocument(
                    "$set" -> bdoc,
                    "$setOnInsert" -> BSONDocument(
                      "views" -> 0,
                      "isVipContent" -> true
                    )
                  ),
                  upsert = true
                )
              }
              _ <- log.info(result.lastError.map(e => s"""|err: ${e.err}|updatedExisting: ${e.updatedExisting}|n: ${e.n}|upserted: ${e.upserted}""").toString())
            } yield ()

            action
          }


          override def fetch(maybeLastId: Option[BSONObjectID], pageSize: Int): ZIO[Logging, Throwable, Array[DeviceToken]] = {
            ZIO.fromFuture { implicit ec =>
              val filter = maybeLastId
                .map(lastId =>
                  BSONDocument("_id" -> BSONDocument("$lt" -> lastId))
                )
                .getOrElse(BSONDocument())

              deviceTokens.find(filter)
                .sort(BSONDocument("_id" -> -1))
                .cursor[DeviceToken]()
                .collect(pageSize, Cursor.ContOnError { (a: Array[DeviceToken], throwable) =>
                  throwable.printStackTrace()
                })
            }
          }

        }
      }
    }
  }

  def upsert(json: Json) = {
     ZIO.access[Has[db.Service]](_.get)
      .flatMap(_.upsert(json))
  }

  def fetch(maybeLastId: Option[BSONObjectID], pageSize: Int) = {
     ZIO.access[Has[db.Service]](_.get)
      .flatMap(_.fetch(maybeLastId, pageSize))
  }
}
object main extends App {

  def standardize(obj: Json) = {
    for {
      metaData <- root.metaData.obj.getOption(obj)
      mediaFiles <- root.mediaFiles.arr.getOption(obj)
      mediaData = mediaFiles.map(jsonutil.renameField(_, "mediaData", "data")).asJson
    } yield metaData.add("medias", mediaData).remove("isVipContent")
  }



  def sendFCM(fcmConfig: FCMConfig) = {
    val keys = Map(
      "Meditation Music" -> fcmConfig.meditationMusicKey,
      "Relax Sound" -> fcmConfig.relaxSoundKey
    )
    stream.deviceTokens()
      .flatMap { deviceTokens =>
        val groups = deviceTokens.groupBy(_.appId)
        ZStream.fromIterable(groups)
      }
      .mapM {
        case (appId, deviceTokens) => api.sendFCM(keys(appId), appId, deviceTokens.map(_.deviceToken)).either
      }
      .tap {
        case Right(json) => log.info(s"main::sendFCM ${json.toString()}")
        case Left(throwable) => log.error(s"main::sendFCM", Cause.Die(throwable))
      }
      .run(
        ZSink.foldLeft(0 -> 0) {
          case ((totalSuccess, totalFailure), Right(json)) =>
            val success = root.success.int.getOption(json).map(_ + totalSuccess).getOrElse(totalSuccess)
            val failure = root.failure.int.getOption(json).map(_ + totalFailure).getOrElse(totalFailure)
            success -> failure
          case (acc, Left(throwable)) => acc
        }
      )
  }



  def program = {
    val xs = for {
      category <- constants.categories
      ageTag <- constants.ageTag
      timeOfDay <- constants.timeOfDays
    } yield (category, ageTag, timeOfDay)


    val crawlEff = stream.categoryPackages(xs)
      .flatMap(stream.items)
      .map(standardize).collectSome
      .mapMParUnordered(16)(obj => db.upsert(obj.asJson))
      .zipWithIndex
      .tap {
        case (_, idx) => log.info(s"${idx}th Loaded")
      }
      .run(ZSink.count)

    for {
      crawlTotal <- crawlEff
      fcmConfig <- ZIO.access[Has[FCMConfig]](_.get)
      batchCount <- sendFCM(fcmConfig)
    } yield crawlTotal -> batchCount
  }





  def run(args: List[String]): URIO[ZEnv, ExitCode] = {

    val appConfigLayer = TypesafeConfig.fromHoconFile(new File("./.env.conf"), DeriveConfigDescriptor.descriptor[AppConfig])

    val logLayer = Logging.console()

    val apiConfigLayer = appConfigLayer.narrow(_.api)
    val httpClientLayer = AsyncHttpClientZioBackend.layer() ++ apiConfigLayer

    val fcmConfigLayer = appConfigLayer.narrow(_.fcm)
    val configLayer = appConfigLayer.narrow(_.db)
    val facebookConfigLayer = appConfigLayer.narrow(_.fb)

    val mongoLayer = configLayer >>> mongo.layer

    val dbLayer = ZLayer.fromServiceM { (conn: MongoConnection) =>
      for {
        conf <- ZIO.access[MongoConf](_.get)
        db <- ZIO.fromFuture(implicit ec => conn.database(conf.database))
      } yield db -> conf
    }

    val dbServiceLayer = (mongoLayer ++ configLayer) >>> dbLayer >>> db.Service.live

    val layer = httpClientLayer ++ dbServiceLayer ++ logLayer ++ facebookConfigLayer ++ fcmConfigLayer

    (for {
      start <- instant
      _ <- log.info(s"Starting...")
      (crawlCount, (success, failure)) <- program
      endTime <- instant
      message = s"""
                   |Crawl total $crawlCount records
                   |Send FCM $success success, $failure failure
                   |Total run times: ${start.until(endTime, ChronoUnit.MINUTES)}m ${start.until(endTime, ChronoUnit.SECONDS)}s
                   |""".stripMargin
    } yield message)
    .foldM(e => {
      log.throwable("Exception", e) *> api.sendFacebookMessage(e.getMessage)
    }, message => {
      log.info(message) *> api.sendFacebookMessage(message)
    })
    .provideCustomLayer(layer)
    .exitCode
  }
}