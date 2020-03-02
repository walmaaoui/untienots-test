package unitenots.data.eng.batch.kafkatoparquet

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession
import scopt.OParser

object KafkaToParquetJob extends App with StrictLogging {

  OParser.parse(AppConfig.cliConfigParser, args, AppConfig()) match {
    case Some(appConfig) =>
      logger.info(s"config: $appConfig")

      val spark = SparkSession
        .builder()
        .master("local[4]")
        .appName(appConfig.appName)
        .getOrCreate()

      val streamFactory = new StreamFactory(spark, appConfig)

      appConfig.inputTopics.foreach(topic =>
        streamFactory.processTopic(topic) match {
          case Some(_) => logger.info(s"Copying topic $topic is launched")
          case None    => logger.warn(s"Ignored topic $topic")
      })

      spark.streams.awaitAnyTermination()
      logger.info("Topics copied to parquet files")
    case _ =>
      logger.error(
        "Couldn't set up app configuration from input args, please check help cmd")
      throw new IllegalArgumentException
  }
}
