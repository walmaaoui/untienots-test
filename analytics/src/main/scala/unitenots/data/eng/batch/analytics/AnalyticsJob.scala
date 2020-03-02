package unitenots.data.eng.batch.analytics

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OParser
import unitenots.data.eng.batch.analytics.models._

object AnalyticsJob extends App with StrictLogging {

  OParser.parse(AppConfig.cliConfigParser, args, AppConfig()) match {
    case Some(appConfig) =>
      logger.info(s"config: $appConfig")

      val spark = SparkSession
        .builder()
        .master("local[4]")
        .appName(appConfig.appName)
        .getOrCreate()

      import spark.implicits._

      val processor = new AnalyticsProcessor(spark)

      val sourceWordTopics =
        spark.read.parquet(appConfig.wordTopicsPath).as[SourceWordTopics].cache
      val sourceWord = sourceWordTopics.as[SourceWord]

      val sourceTopic = processor
        .associateSourcesToTopics(sourceWordTopics,
                                  appConfig.belongingThreshold)
        .cache

      processor
        .falsePositive(sourceWordTopics, sourceTopic)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"${appConfig.analyticsPath}/false-positive")

      processor
        .keywordOccurrencesNbrBySource(sourceWord)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"${appConfig.analyticsPath}/key-words-occurrences-nbr")

      processor
        .wordsRelevanceByTopic(sourceWord, sourceTopic)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"${appConfig.analyticsPath}/words-relevance")

      logger.info(s"Batch analytics finished successfully")

    case _ =>
      logger.error(
        "Couldn't set up app configuration from input args, please check help cmd")
      throw new IllegalArgumentException
  }
}
