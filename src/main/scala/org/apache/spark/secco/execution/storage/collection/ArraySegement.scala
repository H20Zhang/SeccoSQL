package org.apache.spark.secco.execution.storage.collection

import scala.collection.mutable.ArrayBuffer

case class ArraySegment(
    var array: Array[Double],
    var begin: Int,
    var end: Int,
    var size: Int
) {

  def apply(i: Int) = {
    array(begin + i)
  }

  def update(i: Int, value: Double) = {
    array(begin + i) = value
  }

  def set(
      _array: Array[Double],
      _begin: Int,
      _end: Int,
      _size: Int
  ): Unit = {
    array = _array
    begin = _begin
    end = _end
    size = _size
  }

  def slice(newBegin: Int, newEnd: Int): ArraySegment = {
    assert((newEnd + begin) < end)

    begin = begin + newBegin
    end = begin + newEnd
    size = end - begin

    this
  }

  def adjust(newBegin: Int, newEnd: Int): ArraySegment = {
    begin = newBegin
    end = newEnd
    size = end - begin

    this
  }

  def toArray() = {
    if (begin == 0 && size == array.size) {
      array
    } else {
      val buffer = ArrayBuffer[Double]()
      var i = begin
      while (i < end) {
        buffer += array(i)
        i += 1
      }

      buffer.toArray
    }
  }

  def toIterator =
    new Iterator[Double] {
      var pos = begin

      override def hasNext: Boolean = pos < end

      override def next(): Double = {
        val curPos = pos
        pos += 1
        array(curPos)
      }

      override def size: Int = {
        end - begin
      }
    }

  override def toString: String = {
    val stringBuilder = new StringBuilder()
    var i = begin
    while (i < end) {
      stringBuilder.append(s"${array(i)}, ")
      i += 1
    }

    stringBuilder.dropRight(2).toString()
  }

}

object ArraySegment {
  val emptyArraySegment = ArraySegment(Array.empty[Double], 0, 0, 0)

  def emptyArray() = emptyArraySegment
  def newEmptyArraySegment() =
    ArraySegment(Array.empty[Double], 0, 0, 0)
  def apply(array: Array[Double]): ArraySegment = {
    ArraySegment(array, 0, array.size, array.size)
  }
}
