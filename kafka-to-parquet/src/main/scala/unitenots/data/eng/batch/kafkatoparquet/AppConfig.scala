package unitenots.data.eng.batch.kafkatoparquet

import com.typesafe.config.ConfigFactory
import scopt.OParser

import scala.collection.JavaConverters._

final case class AppConfig(
    appName: String = Defaults.appName,
    inputTopics: Seq[String] = Defaults.inputTopics,
    baseOutputPath: String = Defaults.baseOutputPath,
    kafkaBroker: String = Defaults.kafkaBroker,
    kafkaAutoOffsetReset: String = Defaults.kafkaAutoOffsetReset,
    kafkaCheckPointLocation: String = Defaults.kafkaCheckPointLocation,
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
      opt[Seq[String]]("input-topics")
        .action((x, c) => c.copy(inputTopics = x))
        .text("Topics to copy to parquet"),
      help("help").text("prints this usage text")
    )
  }
}

object Defaults {
  val config = ConfigFactory.load()
  val appName = config.getString("app.name")
  val inputTopics = config.getStringList("app.input-topics").asScala
  val baseOutputPath = config.getString("app.base-output-path")
  val kafkaBroker = config.getString("kafka.host")
  val kafkaAutoOffsetReset = config.getString("kafka.auto.offset.reset")
  val kafkaCheckPointLocation = config.getString("kafka.checkpointLocation")
  val kafkaFailOnDataLoss = config.getBoolean("kafka.failOnDataLoss")
}
