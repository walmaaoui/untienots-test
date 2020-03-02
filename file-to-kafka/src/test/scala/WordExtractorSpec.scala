import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import unitenots.data.eng.batch.filetokafka.WordExtractor
import unitenots.data.eng.batch.filetokafka.models.{Line, Word}

class WordExtractorSpec extends FlatSpec with Matchers with DataFrameSuiteBase {
  import spark.implicits._
  val defaultConf = WordExtractor.Config(toLowerCase = false,
                                         removePunctuation = false,
                                         removeCommonWords = false)

  lazy val wordExtractor = new WordExtractor(spark)
  "WordExtractor" should
    "split lines to words" in {
    val lines = spark.createDataset(Seq(Line("This a line!")))
    wordExtractor
      .process(lines, defaultConf)
      .collect() shouldBe Seq(Word("This"), Word("a"), Word("line!"))
  }

  it should "handle empty lines" in {
    val lines = spark.createDataset(Seq(Line("")))
    wordExtractor
      .process(lines, defaultConf)
      .collect() shouldBe Seq.empty
  }

  it should "transform words to lower case if the toLowerCase flag is specified" in {
    val lines = spark.createDataset(Seq(Line("This a line!")))
    wordExtractor
      .process(lines, defaultConf.copy(toLowerCase = true))
      .collect() shouldBe Seq(Word("this"), Word("a"), Word("line!"))
  }

  it should "remove punctuation from words if the removePunctuation flag is specified" in {
    val lines = spark.createDataset(Seq(Line("This ; a line!")))
    wordExtractor
      .process(lines, defaultConf.copy(removePunctuation = true))
      .collect() shouldBe Seq(Word("This"), Word("a"), Word("line"))
  }

  it should "remove common words if flag removeCommonWords is specified" in {
    val lines = spark.createDataset(Seq(Line("This a line")))
    wordExtractor
      .process(lines, defaultConf.copy(removeCommonWords = true))
      .collect() shouldBe Seq(Word("line"))
  }

}
