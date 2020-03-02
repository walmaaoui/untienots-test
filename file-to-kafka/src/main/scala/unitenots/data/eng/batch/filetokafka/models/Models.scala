package unitenots.data.eng.batch.filetokafka.models

final case class Line(value: String) extends AnyVal
final case class Word(value: String) extends AnyVal
final case class Words(value: Seq[String]) extends AnyVal