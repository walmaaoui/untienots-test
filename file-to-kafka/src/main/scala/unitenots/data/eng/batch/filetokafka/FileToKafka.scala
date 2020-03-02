package unitenots.data.eng.batch.filetokafka

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scopt.OParser
import unitenots.data.eng.batch.filetokafka.models._

object FileToKafka extends App with StrictLogging {

  OParser.parse(AppConfig.cliConfigParser, args, AppConfig()) match {
    case Some(appConfig) =>
      logger.info(s"config: $appConfig")

      val spark = SparkSession
        .builder()
        .master("local[4]")
        .appName(appConfig.appName)
        .getOrCreate()

      import spark.implicits._

      val df = spark.read.textFile(appConfig.folderPath).as[Line]

      val wordExtractor = new WordExtractor(spark)

      val onlyFileName = udf((fullPath: String) => fullPath.split("/").last)
      val kafkaMessageFromCols =
        to_json(
          struct(onlyFileName(input_file_name()).as("source"),
                 $"value".as("word")))
          .as("value")

      //cached to do the count and monitor the sent words count.
      // if too much data => use spark listeners to get the count instead to avoid cache
      val messages =
        wordExtractor
          .process(df, WordExtractor.Config.from(appConfig))
          .select(kafkaMessageFromCols)
          .cache()

      messages.write
        .format("kafka")
        .option("kafka.bootstrap.servers", appConfig.kafkaBroker)
        .option("topic", appConfig.topic)
        .save()

      logger.info(
        s"${messages.count} words sent successfully to topic ${appConfig.topic}")

    case _ =>
      logger.error(
        "Couldn't set up app configuration from input args, please check help cmd")
      throw new IllegalArgumentException
  }
}
