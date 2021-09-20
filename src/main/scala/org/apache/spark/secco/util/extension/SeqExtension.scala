package org.apache.spark.secco.util.`extension`

object SeqExtension extends Serializable {

  //value class combined with implicit as a method
  // to avoid object creation and inline the function call
  implicit class SubsetGen[A](val seq: Seq[A]) extends AnyVal {
    def subset[A]() = {
      val size = seq.size
      Range(1, size + 1).flatMap { subsetSize =>
        seq.combinations(subsetSize)
      }
    }
  }

  def subset[A](seq: Seq[A]): Seq[Seq[A]] = {
    val size = seq.size
    Range(1, size + 1).flatMap { subsetSize =>
      seq.combinations(subsetSize)
    }
  }

  def posOf[T](seq: Seq[T], element: T): Int = {
    var i = 0
    while (i < seq.size) {
      if (seq(i) == element) {
        return i
      }
      i += 1
    }
    return -1
  }
}
