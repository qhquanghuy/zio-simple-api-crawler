package console


final case class MongoConfig(user: String, pwd: String, host: String, port: String, database: String, collection: String)

final case class FacebookConfig(userPSID: String, pageAccessToken: String)

final case class ApiConfig(host: String)

final case class FCMConfig(
  meditationMusicKey: String,
  relaxSoundKey: String
)

final case class AppConfig(
  db: MongoConfig,
  fb: FacebookConfig,
  api: ApiConfig,
  fcm: FCMConfig
)