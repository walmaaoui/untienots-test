package unitenots.data.eng.streaming.wordsprocessing.utils

import com.typesafe.config._
import org.scalatest.{FlatSpec, Matchers}
import unitenots.data.eng.streaming.wordsprocessing.utils.Utils._

import scala.collection.JavaConverters._

class UtilsSpec extends FlatSpec with Matchers {

  "inverseMap" should "transform Map[A, Seq[B]] to Map[B, Seq[A]" in {
    val map = Map(1 -> Seq("a", "b"), 2 -> Seq("b", "c"))
    println(map.flatMap { case (k, v) => v.map((k, _)) })
    println(
      map.flatMap { case (k, v) => v.map((k, _)) }.groupBy { case (_, v) => v })

    inverseMap(map).mapValues(_.sorted) should contain theSameElementsAs Map(
      "a" -> Seq(1),
      "b" -> Seq(1, 2),
      "c" -> Seq(2))
  }

  "configToMap" should "trasform config object to Map" in {
    val confMap = Map("k1" -> "v1", "k2" -> "v2")
    val conf = ConfigFactory.parseMap(confMap.asJava)
    configToMap[String](conf) should contain theSameElementsAs confMap
  }

  "configToMap" should "trasform config object with complex types to Map" in {
    // parseMap accepts only java structures
    val confMap = Map("k1" -> Seq("v1").asJava, "k2" -> Seq("v2").asJava)
    val conf = ConfigFactory.parseMap(confMap.asJava)
    configToMap[Seq[String]](conf) should contain theSameElementsAs confMap
  }
}
