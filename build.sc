import mill._, scalalib._

object app extends ScalaModule {
  def scalaVersion = "2.13.6"
  def ivyDeps = Agg(
    ivy"com.softwaremill.sttp.client3::async-http-client-backend-zio:3.3.6",
    ivy"io.circe::circe-bson:0.5.0",
    ivy"io.circe::circe-generic:0.14.1",
    ivy"io.circe::circe-generic-extras:0.14.1",
    ivy"io.circe::circe-optics:0.14.1",
    ivy"com.softwaremill.sttp.client3::circe:3.3.6",
    ivy"org.reactivemongo::reactivemongo-bson-api:1.0.4",
    ivy"org.reactivemongo::reactivemongo:1.0.4",
    ivy"dev.zio::zio-config-magnolia:1.0.6",
    ivy"dev.zio::zio-config-typesafe:1.0.6",
    ivy"dev.zio::zio-logging-slf4j:0.5.10"
  )

  def mainClass = Some("console.main")

}