package unitenots.data.eng.batch.filetokafka

import com.typesafe.config.ConfigFactory
import scopt.OParser

final case class AppConfig(
    appName: String = Defaults.appName,
    folderPath: String = Defaults.folderPath,
    kafkaBroker: String = Defaults.kafkaBroker,
    topic: String = Defaults.topic,
    toLowerCase: Boolean = Defaults.toLowerCase,
    removePunctuation: Boolean = Defaults.removePunctuation,
    removeCommonWords: Boolean = Defaults.removeCommonWords
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
      opt[String]("folder-path")
        .action((x, c) => c.copy(folderPath = x))
        .text("Glob path to input files"),
      opt[String]("topic")
        .action((x, c) => c.copy(topic = x))
        .text("Kafka topic to which data will be sent"),
      opt[String]("kafka-broker")
        .action((x, c) => c.copy(topic = x))
        .text("Kafka broker host:port to which data will be sent"),
      opt[Boolean]("to-lower-case")
        .action((x, c) => c.copy(toLowerCase = x))
        .text("Boolean, whether or not to transform input text to lower case"),
      opt[Boolean]("remove-punctuation")
        .action((x, c) => c.copy(removePunctuation = x))
        .text("Boolean, whether or not to remove punctuation from input text"),
      opt[Boolean]("remove-common-words")
        .action((x, c) => c.copy(removeCommonWords = x))
        .text("Boolean, whether or not to remove common words"),
      help("help").text("prints this usage text")
    )

  }

}
object Defaults {
  val config = ConfigFactory.load()
  val appName = config.getString("app.name")
  val folderPath = config.getString("app.default-folder-path")
  val topic = config.getString("app.default-topic")
  val toLowerCase = config.getBoolean("app.file-processing.to-lower-case")
  val removePunctuation =
    config.getBoolean("app.file-processing.remove-punctuation")
  val removeCommonWords =
    config.getBoolean("app.file-processing.remove-common-words")
  val kafkaBroker = config.getString("kafka.host")
}
