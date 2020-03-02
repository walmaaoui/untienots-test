package unitenots.data.eng.batch.analytics

import CustomEquality._
import org.scalactic.TripleEquals._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalactic.{Equality, TolerantNumerics}
import unitenots.data.eng.batch.analytics.models._
import org.scalatest.{FlatSpec, Matchers}

object CustomEquality {
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1e-4)

  implicit val optionEq = new Equality[Option[Double]] {
    override def areEqual(a: Option[Double], b: Any) = b match {
      case g: Option[_] if g.isDefined && a.isDefined => a.get === g.get
      case g: Option[_] if g.isEmpty && a.isEmpty     => true
      case _                                               => false
    }
  }

  implicit val topicWordRelevanceStatsEq =
    new Equality[TopicWordRelevanceStats] {
      override def areEqual(a: TopicWordRelevanceStats, b: Any) = b match {
        case g: TopicWordRelevanceStats =>
          a.absence_rate_in_belonged_sources === g.absence_rate_in_belonged_sources && a.presence_rate_in_belonged_sources === g.presence_rate_in_belonged_sources && a.presence_rate_in_non_belonged_sources === g.presence_rate_in_non_belonged_sources
        case _ => false
      }
    }

}

class AnalyticsProcessorSpec
    extends FlatSpec
    with Matchers
    with DataFrameSuiteBase {
  import spark.implicits._
  lazy val processor = new AnalyticsProcessor(spark)

  "keywordOccurrencesNbrBySource" should "count nbr occurrences of each word per source" in {
    val sourcesWords = spark.createDataset(
      Seq(SourceWord("f1", "w1"),
          SourceWord("f1", "w1"),
          SourceWord("f1", "w2"),
          SourceWord("f2", "w1")))
    processor
      .keywordOccurrencesNbrBySource(sourcesWords)
      .collect() should contain theSameElementsAs Seq(
      SourceWordOccurrences("f1", "w1", 2),
      SourceWordOccurrences("f1", "w2", 1),
      SourceWordOccurrences("f2", "w1", 1))
  }

  "associateSourcesToTopics" should "associate Sources To Topics" in {
    val sourcesWordsTopics = spark.createDataset(
      Seq(
        SourceWordTopics("f1", "w1", Seq("sport", "color")),
        SourceWordTopics("f1", "w2", Seq("sport")),
        SourceWordTopics("f1", "w3", Seq("sport")),
        SourceWordTopics("f1", "w4", Seq("food")),
        SourceWordTopics("f2", "w4", Seq("food")),
        SourceWordTopics("f2", "w5", Seq("food")),
        SourceWordTopics("f2", "w5", Seq("color"))
      )
    )

    processor
      .associateSourcesToTopics(sourcesWordsTopics, 0.6)
      .collect() should contain theSameElementsAs Seq(
      SourceBelongingTopic("f1", "sport"),
      SourceBelongingTopic("f2", "food")
    )
  }

  "associateSourcesToTopics" should "deduplicate words to calculate the topic" in {
    val sourcesWordsTopics = spark.createDataset(
      Seq(
        SourceWordTopics("f1", "w1", Seq("sport")),
        SourceWordTopics("f1", "w1", Seq("sport")),
        SourceWordTopics("f1", "w2", Seq("color"))
      )
    )

    processor
      .associateSourcesToTopics(sourcesWordsTopics, 0.6)
      .collect() should contain theSameElementsAs Seq.empty
  }

  "falsePositive" should "return false positives" in {
    val sourcesWordsTopics = spark.createDataset(
      Seq(
        SourceWordTopics("f1", "w1", Seq("sport", "color")),
        SourceWordTopics("f1", "w0", Seq("color")), // false positive for topic sport
        SourceWordTopics("f1", "w2", Seq("sport")), // false positive for topic color
        SourceWordTopics("f2", "w3", Seq("color")), // false positive for topic sport
        SourceWordTopics("f2", "w4", Seq("color", "sport"))
      )
    )

    val sourcesWithBelongingTopic = spark.createDataset(
      Seq(
        SourceBelongingTopic("f1", "sport"),
        SourceBelongingTopic("f1", "color"),
        SourceBelongingTopic("f2", "sport")
      )
    )

    processor
      .falsePositive(sourcesWordsTopics, sourcesWithBelongingTopic)
      .collect() should contain theSameElementsAs Seq(
      SourceWordTopic("f2", "w3", "sport"),
      SourceWordTopic("f1", "w2", "color"),
      SourceWordTopic("f1", "w0", "sport"))
  }

  "wordsRelevanceByTopic" should "compute word relevance by topic" in {
    val sourcesWords = spark.createDataset(
      Seq(
        SourceWord("f1", "w1"),
        SourceWord("f2", "w1"),
        SourceWord("f3", "w1"),
        SourceWord("f4", "w2"),
        SourceWord("f5", "w1"),
        SourceWord("f6", "w2"),
      )
    )

    val sourcesWithBelongingTopic = spark.createDataset(
      Seq(
        SourceBelongingTopic("f1", "sport"),
        SourceBelongingTopic("f1", "color"),
        SourceBelongingTopic("f2", "sport"),
        SourceBelongingTopic("f3", "food"),
        SourceBelongingTopic("f4", "sport"),
        SourceBelongingTopic("f6", "color")
      )
    )

    processor
      .wordsRelevanceByTopic(sourcesWords, sourcesWithBelongingTopic)
      .collect()
      .filter(_.word == "w1") should contain theSameElementsAs Seq(
      TopicWordRelevanceStats("sport",
                              "w1",
                              Some(2d / 3),
                              Some(2d / 3),
                              Some(1d / 3)),
      TopicWordRelevanceStats("color", "w1", Some(0.5), Some(0.75), Some(0.5)),
      TopicWordRelevanceStats("food", "w1", Some(1), Some(0.6), Some(0))
    )
  }

}
