package org.apache.spark.secco.execution.plan.computation.newIter

//import org.apache.spark.secco.execution.storage.Utils
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.expression.codegen.GenerateUnsafeInternalRowJoiner
import org.apache.spark.secco.types.StructType

import scala.+:
import scala.collection.mutable.ArrayBuffer

/** The base class for cartesian production iterators. */
sealed abstract class BaseCartesianProductIterator extends SeccoIterator {}

/** The iterator that performs the cartesian product operation.
  *
  * Note that: the sort order is maintained.
  */
case class CartesianProductIterator(left: SeccoIterator, right: SeccoIterator)
    extends SeccoIterator {

  private var leftRowCache: InternalRow = _
  private var leftRowCacheValid: Boolean = false

  private val rightRowsArrayBuffer: ArrayBuffer[InternalRow] = ArrayBuffer()
  private var rightRowsIter: Iterator[InternalRow] = _
  private var rightRowsIterValid: Boolean = false

  private val leftSchema = StructType.fromAttributes(left.localAttributeOrder())
  private val rightSchema = StructType.fromAttributes(right.localAttributeOrder())
  private val joiner = GenerateUnsafeInternalRowJoiner.generate((leftSchema, rightSchema))

  override def localAttributeOrder(): Array[Attribute] = left.localAttributeOrder() ++ right.localAttributeOrder()

  override def isSorted(): Boolean = left.isSorted() && right.isSorted()

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = {
    val outputRowsArrayBuffer = ArrayBuffer[InternalRow]()
    val rightRows = right.results().toArray()
    for(leftRow <- left.results().toArray()){
      for(rightRow <- rightRows){
        outputRowsArrayBuffer.append(joiner.join(leftRow.asInstanceOf[UnsafeInternalRow],
          rightRow.asInstanceOf[UnsafeInternalRow]).copy())
      }
    }
    InternalBlock(outputRowsArrayBuffer.toArray, StructType(leftSchema ++ rightSchema))
  }

  override def children: Seq[SeccoIterator] = left :: right :: Nil

  override def hasNext: Boolean = {
    val leftHasNext = leftRowCacheValid ||  left.hasNext
    val rightHasNext = right.hasNext || {
      if (!rightRowsIterValid) {
        rightRowsIterValid = true
        rightRowsIter = rightRowsArrayBuffer.toIterator
        rightRowsIter.hasNext
      }
      else {
        rightRowsIter.hasNext || {
          rightRowsIter = rightRowsArrayBuffer.toIterator
          rightRowsIter.hasNext
        }
      }
    }
    leftHasNext && rightHasNext
  }

  // lgh TODO: currently it only supports UnsafeInternalRow
  override def next(): InternalRow = {
    if(!hasNext) throw new NoSuchElementException("next on empty iterator")
    if(!leftRowCacheValid) { leftRowCache = left.next(); leftRowCacheValid = true;}
    if(right.hasNext){
      val right_row = right.next()
      rightRowsArrayBuffer.append(right_row)
      if(!right.hasNext) leftRowCacheValid = false
      joiner.join(leftRowCache.asInstanceOf[UnsafeInternalRow], right_row.asInstanceOf[UnsafeInternalRow])
    }
    else{
      val right_row = rightRowsIter.next()
      if(!rightRowsIter.hasNext) leftRowCacheValid = false
      joiner.join(leftRowCache.asInstanceOf[UnsafeInternalRow], right_row.asInstanceOf[UnsafeInternalRow])
    }
  }
}
