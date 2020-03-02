package unitenots.data.eng.batch.kafkatoparquet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import unitenots.data.eng.batch.kafkatoparquet.models._
import unitenots.data.eng.batch.kafkatoparquet.models.Topic._

class StreamFactory(spark: SparkSession, appConfig: AppConfig) {
  import spark.implicits._

  def processTopic(topic: String): Option[StreamingQuery] =
    supportedTopicsMap.get(topic).map(processTopic)

  private def processTopic(topic: Topic[_]): StreamingQuery =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", appConfig.kafkaBroker)
      .option("auto.offset.reset", appConfig.kafkaAutoOffsetReset)
      .option("failOnDataLoss", appConfig.kafkaFailOnDataLoss)
      .option("subscribe", topic.name)
      .load()
      .selectExpr("CAST(value AS STRING) as value")
      .select(from_json($"value", topic.schema).as("message"))
      .select("message.*")
      .writeStream
      .format("parquet")
      .option("path", s"${appConfig.baseOutputPath}/${topic.name}")
      .option("checkpointLocation",
              s"${appConfig.kafkaCheckPointLocation}/${topic.name}")
      .trigger(Trigger.Once())
      .start()

}
