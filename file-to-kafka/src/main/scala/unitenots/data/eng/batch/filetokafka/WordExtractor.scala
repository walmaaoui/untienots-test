package unitenots.data.eng.batch.filetokafka

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import unitenots.data.eng.batch.filetokafka.WordExtractor._
import unitenots.data.eng.batch.filetokafka.models.{Line, Word, Words}
import unitenots.data.eng.batch.filetokafka.utils.Syntax._
import unitenots.data.eng.batch.filetokafka.utils.TransformerHelper._


class WordExtractor(spark: SparkSession) {
  import spark.implicits._
  def process(lines: Dataset[Line],
              config: WordExtractor.Config): Dataset[Word] =
    lines >
      (if (config.toLowerCase) toLowerCase else identity) >
      split >
      (if (config.removePunctuation) removePunctuation else identity) >
      (if (config.removeCommonWords) removeCommonWords else identity) >
      flattenWords

  private [filetokafka] def toLowerCase(lines: Dataset[Line]): Dataset[Line] =
    lines.map(_.value.toLowerCase).as[Line]

  private [filetokafka] def split(lines: Dataset[Line]): Dataset[Words] =
    lines.map(_.value.split(" ")).as[Words]

  private [filetokafka] def removePunctuation(words: Dataset[Words]): Dataset[Words] =
    words
      .map(words => words.value.map(_.replaceAll(punctuationRegex, "").trim))
      .as[Words]

  private [filetokafka] def removeCommonWords(words: Dataset[Words]): Dataset[Words] =
    transformInplace(new StopWordsRemover(), words, "value")
      .as[Words]

  private [filetokafka] def flattenWords(words: Dataset[Words]): Dataset[Word] =
    words
      .withColumn("value", explode($"value"))
      .as[Word]
      .filter(_.value.nonEmpty)

}

object WordExtractor {
  val punctuationRegex = "[,.!?:;]"
  final case class Config(toLowerCase: Boolean,
                          removePunctuation: Boolean,
                          removeCommonWords: Boolean)
  object Config {
    def from(appConfig: AppConfig): Config =
      Config(toLowerCase = appConfig.toLowerCase,
             removePunctuation = appConfig.removePunctuation,
             removeCommonWords = appConfig.removeCommonWords)
  }
}
