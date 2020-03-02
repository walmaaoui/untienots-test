package unitenots.data.eng.batch.kafkatoparquet.models

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import scala.reflect.runtime.universe._

final case class WordTopicsMessage(source: String,
                                   word: String,
                                   topics: Seq[String])
final case class FileTopicMessage(source: String, topic: String)

sealed abstract class Topic[T: TypeTag](val name: String) {
  def schema: StructType =
    ScalaReflection
      .schemaFor[T]
      .dataType
      .asInstanceOf[StructType]
}

object Topic {
  case object FilesTopics extends Topic[FileTopicMessage]("file_topic")
  case object WordsTopics extends Topic[WordTopicsMessage]("word_topics")

  val supportedTopicsMap: Map[String, Topic[_]] =
    Seq(FilesTopics, WordsTopics).map(topic => topic.name -> topic).toMap
}
