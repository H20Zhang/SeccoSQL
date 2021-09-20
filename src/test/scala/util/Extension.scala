package util

object Extension {

  implicit class ArrayArrayExtension(arr: Array[Array[Int]]) {
    def toDoubleArray(): Array[Array[Double]] =
      arr.map(_.map(_.toDouble))

    def toDoubleSeq(): Seq[Seq[Double]] =
      toDoubleArray().toSeq.map(_.toSeq)
  }

}
