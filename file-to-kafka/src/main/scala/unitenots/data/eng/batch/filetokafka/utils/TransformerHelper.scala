package unitenots.data.eng.batch.filetokafka.utils

import java.util.Locale

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.{DataFrame, Dataset}

object TransformerHelper {
  // used in case-insensitive comparison in StopWordsRemover transformer
  // we specify it to not depend on the execution env default Locale.
  Locale.setDefault(Locale.ENGLISH)

  def transformInplace[T <: Transformer: SetInputAndOutput](
      transformer: T,
      ds: Dataset[_],
      inputCol: String): DataFrame = {
    val intermediateCol = "intermediate_col"
    val transformerWithInputSet =
      SetInputAndOutput.apply[T].setInputCol(transformer, inputCol)
    val transformerWithInputAndOutputSet =
      SetInputAndOutput
        .apply[T]
        .setOutputCol(transformerWithInputSet, intermediateCol)

    transformerWithInputAndOutputSet
      .transform(ds)
      .drop(inputCol)
      .withColumnRenamed(intermediateCol, inputCol)
  }
}

trait SetInputAndOutput[T] {
  def setInputCol(transformer: T, colName: String): T
  def setOutputCol(transformer: T, colName: String): T
}

object SetInputAndOutput {

  def apply[A](implicit set: SetInputAndOutput[A]): SetInputAndOutput[A] = set

  implicit val tokenizerSetInputAndOutput: SetInputAndOutput[Tokenizer] =
    new SetInputAndOutput[Tokenizer] {
      override def setInputCol(transformer: Tokenizer,
                               colName: String): Tokenizer =
        transformer.setInputCol(colName)
      override def setOutputCol(transformer: Tokenizer,
                                colName: String): Tokenizer =
        transformer.setOutputCol(colName)
    }
  implicit val stopWordsRemoverSetInputAndOutput
    : SetInputAndOutput[StopWordsRemover] =
    new SetInputAndOutput[StopWordsRemover] {
      override def setInputCol(transformer: StopWordsRemover,
                               colName: String): StopWordsRemover =
        transformer.setInputCol(colName)
      override def setOutputCol(transformer: StopWordsRemover,
                                colName: String): StopWordsRemover =
        transformer.setOutputCol(colName)
    }
}
