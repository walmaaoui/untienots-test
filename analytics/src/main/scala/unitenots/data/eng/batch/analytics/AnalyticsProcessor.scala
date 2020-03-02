package unitenots.data.eng.batch.analytics

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import unitenots.data.eng.batch.analytics.models._

class AnalyticsProcessor(spark: SparkSession) {

  import spark.implicits._

  // I prefer the format (source, keyword, nbr_occurrences) to (source, keyword_frequencies: Map[String, Long])
  // because its more readable and easier to query.
  // Does it have a further storage cost due to duplicated source column? I done't think that would be a problem thanks to parquet values encoding
  def keywordOccurrencesNbrBySource(
      sourcesWords: Dataset[SourceWord]): Dataset[SourceWordOccurrences] =
    sourcesWords
      .groupBy("source", "word")
      .agg(count("*").as("nbr_occurrences"))
      .as[SourceWordOccurrences]

  def associateSourcesToTopics(
                                sourcesWordsTopics: Dataset[SourceWordTopics],
                                belongingThreshold: Double): Dataset[SourceBelongingTopic] = {

    val sourcesWordsNbr = sourcesWordsTopics
      .groupBy("source")
      .agg(countDistinct("word").as("nbr_words_by_source"))

    sourcesWordsTopics
      .withColumn("topic", explode($"topics"))
      .groupBy("source", "topic")
      .agg(countDistinct("word").as("nbr_words_by_topic_by_source"))
      .join(sourcesWordsNbr, "source")
      .filter($"nbr_words_by_topic_by_source" / $"nbr_words_by_source" > belongingThreshold)
      .select("source", "topic")
      .as[SourceBelongingTopic]
  }

  def falsePositive(
      sourcesWordsTopics: Dataset[SourceWordTopics],
      sourcesTopics: Dataset[SourceBelongingTopic]): Dataset[SourceWordTopic] =
    sourcesWordsTopics
      .join(sourcesTopics, "source")
      .filter(!array_contains($"topics", $"topic"))
      .select("source", "word", "topic")
      .as[SourceWordTopic]

  def wordsRelevanceByTopic(
      sourcesWords: Dataset[SourceWord],
      sourcesTopics: Dataset[SourceBelongingTopic]): Dataset[TopicWordRelevanceStats] = {

    val globalNbrSources = sourcesWords.select("source").distinct.count

    //topic, word, nbr_presence_in_belonged_sources
    val presenceByTopic = sourcesWords
      .join(sourcesTopics, "source")
      .groupBy("topic", "word")
      .agg(countDistinct("source").as("nbr_presence_in_belonged_sources"))

    //word, global_nbr_presence
    val globalPresence = sourcesWords
      .select("word", "source")
      .groupBy("word")
      .agg(countDistinct("source").as("global_nbr_presence"))

    // topic, nbr_belonged_sources
    val sourceTopic = sourcesTopics
      .groupBy("topic")
      .agg(count("source").as("nbr_belonged_sources")) // by construction from associateSourcesToTopics, couples source/topic are unique so no need for countDistinct

    presenceByTopic
      .join(globalPresence, "word")
      .join(sourceTopic, "topic") // could be broadcast if small
      .withColumn("presence_rate_in_belonged_sources",
                  $"nbr_presence_in_belonged_sources" / $"nbr_belonged_sources")
      .withColumn("absence_rate_in_belonged_sources",
                  lit(1) - $"presence_rate_in_belonged_sources")
      .withColumn(
        "presence_rate_in_non_belonged_sources",
        ($"global_nbr_presence" - $"nbr_presence_in_belonged_sources") / (lit(
          globalNbrSources) - $"nbr_belonged_sources"))
      .select("topic",
      "word",
      "presence_rate_in_belonged_sources",
      "absence_rate_in_belonged_sources",
      "presence_rate_in_non_belonged_sources")
      .as[TopicWordRelevanceStats]
  }
}
