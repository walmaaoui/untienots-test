package unitenots.data.eng.streaming.wordsprocessing

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import unitenots.data.eng.streaming.wordsprocessing.models._
import unitenots.data.eng.streaming.wordsprocessing.utils.Utils._
import unitenots.data.eng.streaming.wordsprocessing.utils.Ops._
import scopt.OParser

object WordsStreamProcessing extends App with StrictLogging {

  OParser.parse(AppConfig.cliConfigParser, args, AppConfig()) match {
    case Some(appConfig) =>
      logger.info(s"config: $appConfig")

      val spark = SparkSession
        .builder()
        .master("local[4]")
        .appName(appConfig.appName)
        .getOrCreate()

      import spark.implicits._

      // If the topics definition is huge we can read broadcast it via the spark context to go faster
      // and if its too huge to fit in memory of each executor, we should read it as an rdd
      // and cache it to join it later with micro-batch rdds from the words stream
      val topicsByWord: Map[Word, Seq[Topic]] = inverseMap(
        appConfig.wordsByTopic)
      val topics = appConfig.wordsByTopic.keySet
      val inputSchema =
        ScalaReflection.schemaFor[WordMessage].dataType.asInstanceOf[StructType]

      val wordsStream = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", appConfig.kafkaBroker)
        .option("auto.offset.reset", appConfig.kafkaAutoOffsetReset)
        .option("failOnDataLoss", appConfig.kafkaFailOnDataLoss)
        .option("subscribe", appConfig.wordsTopic)
        .load()
        .selectExpr("CAST(value AS STRING) as value")
        .select(from_json($"value", inputSchema).as("message"))
        .select("message.*")
        .as[WordMessage]

      // Q2
      wordsStream
        .flatMap { inputMessage =>
          topicsByWord
            .get(Word(inputMessage.word))
            .map { topics =>
              WordTopicsMessage(inputMessage.source,
                                inputMessage.word,
                                topics.map(_.value))
            }
        }
        .select(to_json(struct("source", "word", "topics")).as("value"))
        .writeStreamWithOptions(appConfig.wordsTopicsTopic, appConfig)
        .start()

      // Q3
      wordsStream
        .flatMap { inputMessage =>
          if (topics.contains(Topic(inputMessage.word)))
            Some(FileTopicMessage(inputMessage.source, inputMessage.word))
          else None
        }
        .select(to_json(struct("source", "topic")).as("value"))
        .writeStreamWithOptions(appConfig.filesTopicsTopic, appConfig)
        .start()

      spark.streams.awaitAnyTermination()

    case _ =>
      logger.error(
        "Couldn't set up app configuration from input args, please check help cmd")
      throw new IllegalArgumentException
  }
}
