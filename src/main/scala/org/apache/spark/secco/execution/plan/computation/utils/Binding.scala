package org.apache.spark.secco.execution.plan.computation.utils

import org.apache.spark.secco.execution.{InternalDataType, InternalRow}

import scala.collection.mutable.ArrayBuffer

class Binding {

  val array: InternalRow = null
  var end: Int = 0

  def partialBinding(i: Int) = {
    end = i
//    end = 0
    this
  }

  def setPos(i: Int, value: InternalDataType) = {
    array(i) = value
  }

  def getPos(i: Int) = {
    array(i)
  }
}

case class ArraySegment(
    var array: InternalRow,
    var begin: Int,
    var end: Int,
    var size: Int
) {

  def apply(i: Int) = {
    array(begin + i)
  }

  def update(i: Int, value: InternalDataType) = {
    array(begin + i) = value
  }

  def set(
      _array: Array[InternalDataType],
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
      val buffer = ArrayBuffer[InternalDataType]()
      var i = begin
      while (i < end) {
        buffer += array(i)
        i += 1
      }

      buffer.toArray
    }
  }

  def toIterator =
    new Iterator[InternalDataType] {
      var pos = begin

      override def hasNext: Boolean = pos < end

      override def next(): InternalDataType = {
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
  val emptyArraySegment = ArraySegment(Array.empty[InternalDataType], 0, 0, 0)

  def emptyArray() = emptyArraySegment
  def newEmptyArraySegment() =
    ArraySegment(Array.empty[InternalDataType], 0, 0, 0)
  def apply(array: Array[InternalDataType]): ArraySegment = {
    ArraySegment(array, 0, array.size, array.size)
  }
}
