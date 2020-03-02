package unitenots.data.eng.batch.filetokafka.utils

import org.apache.spark.sql.Dataset

object Syntax {
  implicit class DatasetOps[A](inputDs: Dataset[A]){
    def >[B](transform: Dataset[A] => Dataset[B]): Dataset[B] =
    transform(inputDs)
  }
}
