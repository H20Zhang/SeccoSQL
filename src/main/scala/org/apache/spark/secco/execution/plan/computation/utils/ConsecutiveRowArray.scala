package org.apache.spark.secco.execution.plan.computation.utils

import org.apache.spark.secco.execution.{OldInternalDataType, OldInternalRow}

import scala.collection.mutable.ArrayBuffer

/** store InternalRows consecutively */
case class ConsecutiveRowArray(
    arity: Int,
    underlyingArray: Array[OldInternalDataType]
) {

  /** output consecutive stored InternalRows */
  class ConsecutiveRowIterator(
      arity: Int,
      underlyingArray: Array[OldInternalDataType]
  ) extends Iterator[OldInternalRow] {

    private val _outputRow = new Array[OldInternalDataType](arity)
    private var _curPos = 0
    private val _underlyingArraySize = underlyingArray.length

    override def hasNext: Boolean = _curPos < _underlyingArraySize
    override def next(): OldInternalRow = {
      var i = 0
      while (i < arity) {
        _outputRow(i) = underlyingArray(_curPos + i)
        i += 1
      }

      _curPos += arity
      _outputRow
    }
  }

  def iterator: Iterator[OldInternalRow] =
    new ConsecutiveRowIterator(arity, underlyingArray)
  def apply(idx: Int) = ???
  def isEmpty(): Boolean = underlyingArray.isEmpty

}

object ConsecutiveRowArray {

  def apply(arity: Int, array: Array[OldInternalRow]): ConsecutiveRowArray = {

    val arraySize = array.size
    val underlyingArray = new Array[OldInternalDataType](arity * arraySize)

    var j = 0
    while (j < arraySize) {
      var i = 0
      val row = array(j)
      while (i < arity) {
        underlyingArray(j * arity + i) = row(i)
        i += 1
      }

      j += 1
    }

    new ConsecutiveRowArray(arity, underlyingArray)
  }

  def apply(
      arity: Int,
      iterator: Iterator[OldInternalRow]
  ): ConsecutiveRowArray = {
    val buffer = ArrayBuffer[OldInternalDataType]()
    while (iterator.hasNext) {
      var i = 0
      val row = iterator.next()
      while (i < arity) {
        buffer += row(i)
        i += 1
      }
    }

    var i = 0
    val bufferSize = buffer.size
    val underlyingArray = new Array[OldInternalDataType](bufferSize)
    while (i < bufferSize) {
      underlyingArray(i) = buffer(i)
      i += 1
    }

    new ConsecutiveRowArray(arity, underlyingArray)
  }
}
