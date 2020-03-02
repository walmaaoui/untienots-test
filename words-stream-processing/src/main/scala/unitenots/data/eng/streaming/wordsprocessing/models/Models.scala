package unitenots.data.eng.streaming.wordsprocessing.models

final case class Line(value: String) extends AnyVal
final case class Word(value: String) extends AnyVal
final case class Topic(value: String) extends AnyVal
final case class WordMessage(source: String, word: String)
final case class WordTopicsMessage(source: String, word: String, topics: Seq[String])
final case class FileTopicMessage(source: String, topic: String)
