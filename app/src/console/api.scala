package console

import zio._
import zio.config._

import sttp.client3.SttpBackend
import sttp.capabilities.zio._
import sttp.capabilities._
import sttp.client3.circe._
import sttp.client3._
import sttp.client3.asynchttpclient.zio._
import sttp.model.Uri

import io.circe.generic.auto._
import io.circe.Json
import io.circe.syntax._


object api {

  val baseReq = basicRequest.header("Device-id", constants.deviceId)

  def run[L <: Throwable, R](req: Request[Either[L, R], Effect[Task] with ZioStreams with WebSockets]) = {
    send(req)
      .flatMap(res => ZIO.fromEither(res.body))
  }


  def mkUri(path: String) = ZIO.access[Has[ApiConfig]](_.get).map(conf => Uri.unsafeParse(s"${conf.host}$path"))

  def getDataOfCategory(category: String, ageTag: String, timeOfDay: String) = {
    mkUri("/content/all")
      .map { uri =>
        baseReq.get(
          uri
            .addPath(category)
            .addParam("ageTag", ageTag)
            .addParam("timeOfDayTag", timeOfDay)
        )
        .response(asJson[Seq[CategoryResponse]])

      }
      .flatMap(run)
  }

  def getDataOfSubCategory(category: String, subCategory: String, ageTag: String, timeOfDay: String) = {
    mkUri("/content/all")
      .map { uri =>
        baseReq.get(
          uri
            .addPath(category, subCategory)
            .addParam("ageTag", ageTag)
            .addParam("timeOfDayTag", timeOfDay)
        )
        .response(asJson[CategoryResponse])
      }
      .flatMap(run)
  }


  def getData(id: Int) = {
    mkUri(s"/content/$id")
    .map { uri => baseReq.get(uri).response(asJson[Json]) }
    .flatMap(run)
  }



  def sendFacebookMessage(message: String) = {

    getConfig[FacebookConfig] >>= { config =>
      val req = basicRequest.post(uri"https://graph.facebook.com/v11.0/me/messages?access_token=${config.pageAccessToken}")
        .body(Json.obj(
          "recipient" -> Json.obj(
            "id" -> config.userPSID.asJson
          ),
          "message" -> Json.obj(
            "text" -> message.asJson
          )
        )
      )
      .response(asStringAlways)

      send(req)
    }
  }


  def sendFCM(key: String, appId: String, deviceTokens: Array[String]) = {
    val req = basicRequest.post(uri"https://fcm.googleapis.com/fcm/send")
      .header("Authorization", s"key=${key}")
      .body(
        Json.obj(
          "registration_ids" -> deviceTokens.asJson,
          "notification" -> Json.obj(
            "title" -> appId.asJson,
            "body" -> "There are several new content! Check it out".asJson
          )
        )
      )
      .response(asJson[Json])

    run(req)
  }
}