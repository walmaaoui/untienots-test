package unitenots.data.eng.batch.analytics

import com.typesafe.config.ConfigFactory
import scopt.OParser


final case class AppConfig(
    appName: String = Defaults.appName,
    wordTopicsPath: String = Defaults.wordTopicsPath,
    analyticsPath: String = Defaults.analyticsPath,
    belongingThreshold: Double = Defaults.belongingThreshold,
)

object AppConfig {
  private val builder = OParser.builder[AppConfig]
  val cliConfigParser = {
    import builder._
    OParser.sequence(
      programName(Defaults.appName),
      head(Defaults.appName, "0.1"),
      opt[String]("name")
        .action((x, c) => c.copy(appName = x))
        .text("App name, will be used also as the spark app name"),
      opt[String]("analytics-path")
        .action((x, c) => c.copy(analyticsPath = x))
        .text("Path to file-topic data"),
      opt[String]("word-topics-path")
        .action((x, c) => c.copy(wordTopicsPath = x))
        .text("Path to word-topics data"),
      opt[Double]("belonging-threshold")
        .action((x, c) => c.copy(belongingThreshold = x))
        .text("Kafka broker host:port to which data will be sent"),
      help("help").text("prints this usage text")
    )
  }
}

object Defaults {
  val config = ConfigFactory.load()
  val appName = config.getString("app.name")
  val wordTopicsPath = config.getString("app.word-topics-path")
  val analyticsPath = config.getString("app.analytics-path")
  val belongingThreshold = config.getDouble("app.belonging-threshold")
}
