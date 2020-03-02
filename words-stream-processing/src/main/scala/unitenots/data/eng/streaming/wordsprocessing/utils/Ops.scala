package unitenots.data.eng.streaming.wordsprocessing.utils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.DataStreamWriter
import unitenots.data.eng.streaming.wordsprocessing.AppConfig

object Ops {
  implicit class DatasetOps(ds: Dataset[_]) {
    def writeStreamWithOptions(outputTopic: String,
                               appConfig: AppConfig): DataStreamWriter[_] =
      ds.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", appConfig.kafkaBroker)
        .option("topic", outputTopic)
        .option("checkpointLocation",
                s"${appConfig.kafkaCheckPointLocation}/$outputTopic")
  }
}
