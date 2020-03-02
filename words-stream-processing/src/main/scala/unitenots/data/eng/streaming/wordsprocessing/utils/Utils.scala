package unitenots.data.eng.streaming.wordsprocessing.utils

import com.typesafe.config.Config
import scala.collection.JavaConverters._

object Utils {

  def inverseMap[A, B](input: Map[A, Seq[B]]): Map[B, Seq[A]] =
    input.view.toSeq
      .flatMap { case (k, v) => v.map((k, _)) }
      .groupBy { case (_, v) => v }
      .mapValues(_.map(_._1))
      .map(identity) //https://github.com/scala/bug/issues/7005

  // A should be a java structure because the used lib java structures
  // conversion to scala structures should be done outside this function
  def configToMap[A](conf: Config): Map[String, A] =
    conf.entrySet.asScala
      .map(entry => (entry.getKey, entry.getValue.unwrapped.asInstanceOf[A]))
      .toMap
}
