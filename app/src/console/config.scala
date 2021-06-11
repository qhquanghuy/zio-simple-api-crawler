package console


final case class MongoConfig(user: String, pwd: String, host: String, port: String, database: String, collection: String)

final case class FacebookConfig(userPSID: String, pageAccessToken: String)

final case class ApiConfig(host: String)