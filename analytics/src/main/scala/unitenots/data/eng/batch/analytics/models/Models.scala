package unitenots.data.eng.batch.analytics.models
final case class Word(value: String) extends AnyVal
final case class Topic(value: String) extends AnyVal
final case class SourceWordTopics(source: String,
                                  word: String,
                                  topics: Seq[String])
final case class SourceWordTopic(source: String, word: String, topic: String)
final case class SourceBelongingTopic(source: String, topic: String)
final case class SourceWord(source: String, word: String)
final case class SourceWordOccurrences(source: String,
                                       word: String,
                                       nbr_occurrences: Long)
final case class TopicWordRelevanceStats(
    topic: String,
    word: String,
    presence_rate_in_belonged_sources: Option[Double],
    presence_rate_in_non_belonged_sources: Option[Double],
    absence_rate_in_belonged_sources: Option[Double])
