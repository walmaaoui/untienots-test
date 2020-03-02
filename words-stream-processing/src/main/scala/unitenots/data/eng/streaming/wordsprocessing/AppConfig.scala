package unitenots.data.eng.streaming.wordsprocessing

import java.util

import com.typesafe.config.ConfigFactory
import scopt.OParser
import models._
import utils.Utils._

import scala.collection.JavaConverters._

final case class AppConfig(
    appName: String = Defaults.appName,
    kafkaBroker: String = Defaults.kafkaBroker,
    wordsTopic: String = Defaults.wordsTopic,
    wordsTopicsTopic: String = Defaults.wordsTopicsTopic,
    filesTopicsTopic: String = Defaults.filesTopicsTopic,
    wordsByTopic: Map[Topic, Seq[Word]] = Defaults.wordsByTopic,
    kafkaAutoOffsetReset: String = Defaults.kafkaAutoOffsetReset,
    kafkaCheckPointLocation: String =Defaults.kafkaCheckPointLocation,
    kafkaFailOnDataLoss: Boolean = Defaults.kafkaFailOnDataLoss
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
      opt[String]("words-topic")
        .action((x, c) => c.copy(wordsTopic = x))
        .text("Kafka words topic which has the input stream"),
      opt[String]("words-topic")
        .action((x, c) => c.copy(wordsTopic = x))
        .text("Kafka words topic from which the words stream will be read"),
      opt[String]("words-topics-topic")
        .action((x, c) => c.copy(wordsTopicsTopic = x))
        .text("Kafka topic to which topics by word will be sent"),
      opt[String]("files-topics-topic")
        .action((x, c) => c.copy(filesTopicsTopic = x))
        .text("Kafka topic to which topic by file will be sent"),
      help("help").text("prints this usage text")
    )

  }
}

object Defaults {
  val config = ConfigFactory.load()
  val appName = config.getString("app.name")
  val wordsTopic = config.getString("app.words-topic")
  val wordsTopicsTopic = config.getString("app.words-topics-topic")
  val filesTopicsTopic = config.getString("app.files-topics-topic")
  val wordsByTopic =
    configToMap[util.ArrayList[String]](config.getConfig("app.topics")).map {
      case (k, v) =>
        (Topic(k), asScalaBuffer(v).map(Word(_)))
    }
  val kafkaBroker = config.getString("kafka.host")
  val kafkaAutoOffsetReset = config.getString("kafka.auto.offset.reset")
  val kafkaCheckPointLocation = config.getString("kafka.checkpointLocation")
  val kafkaFailOnDataLoss = config.getBoolean("kafka.failOnDataLoss")
}
