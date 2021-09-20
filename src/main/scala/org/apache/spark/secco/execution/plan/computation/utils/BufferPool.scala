package org.apache.spark.secco.execution.plan.computation.utils

import org.apache.spark.secco.execution.{OldInternalDataType, OldInternalRow}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait BufferPool[T] {
  def arity: Int
  def freeSize: Int
  def totalSize: Int
  def occupiedSize: Int
  def newInstance(): T
  def newInstanceWithArray(arr: OldInternalRow): T
  def reset(): Unit
}

object BufferPool {
  def makeInternalRowBufferPool(arity: Int) =
    new InternalRowBufferPoolImpl(arity)
//  def makeWrappedInternalRowBufferPool(arity: Int) =
//    new WrappedArrayBufferPoolImpl(arity)
}

class InternalRowBufferPoolImpl(val arity: Int)
    extends BufferPool[OldInternalRow] {
  private var pos: Int = -1
  private var buffer: ArrayBuffer[OldInternalRow] = ArrayBuffer()

  override def freeSize: Int = totalSize - occupiedSize

  override def totalSize: Int = buffer.size

  override def occupiedSize: Int = pos + 1

  override def newInstance(): OldInternalRow = {
//    pos += 1
//    if (pos < totalSize) {
//      buffer(pos)
//    } else {
    val newInternalRow = new Array[OldInternalDataType](arity)
//      buffer += newInternalRow
    newInternalRow
//    }
  }

  override def reset(): Unit = {
    pos = -1
    buffer.clear()
    buffer = ArrayBuffer()
  }

  override def newInstanceWithArray(arr: OldInternalRow): OldInternalRow = {
    val outputArr = newInstance()
    var i = 0
    while (i < arity) {
      outputArr(i) = arr(i)
      i += 1
    }
    outputArr
  }
}

//fixme: belowing class can lead to memory leakage. Thus, it is commented out.
//class WrappedArrayBufferPoolImpl(val arity: Int)
//    extends BufferPool[mutable.WrappedArray[InternalDataType]] {
//  private var pos: Int = -1
//  private var buffer: ArrayBuffer[mutable.WrappedArray[InternalDataType]] =
//    ArrayBuffer()
//
//  override def freeSize: Int = totalSize - occupiedSize
//
//  override def totalSize: Int = buffer.size
//
//  override def occupiedSize: Int = pos + 1
//
//  override def newInstance(): mutable.WrappedArray[InternalDataType] = {
////    pos += 1
////    if (pos < totalSize) {
////      buffer(pos)
////    } else {
//    val newInternalRow = new Array[InternalDataType](arity)
//    val newWrappedArray =
//      mutable.WrappedArray.make[InternalDataType](newInternalRow)
////      buffer += newWrappedArray
//    newWrappedArray
////    }
//  }
//
//  override def reset(): Unit = {
//    pos = -1
//    buffer.clear()
//    buffer = ArrayBuffer[mutable.WrappedArray[InternalDataType]]()
//  }
//
//  override def newInstanceWithArray(
//      arr: InternalRow
//  ): mutable.WrappedArray[InternalDataType] = {
//    val outputArr = newInstance()
//    var i = 0
//    while (i < arity) {
//      outputArr(i) = arr(i)
//      i += 1
//    }
//    outputArr
//  }
//}
