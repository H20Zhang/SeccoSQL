package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.codegen.{GeneratePredicate, GenerateSafeProjection, GenerateUnsafeInternalRowJoiner}
import org.apache.spark.secco.expression.{Attribute, Expression}
import org.apache.spark.secco.types.StructType

import scala.collection.mutable.ArrayBuffer

/** The base class for performing hash join via iterator. */
sealed abstract class BaseHashJoinIterator extends SeccoIterator {

  def joinCondition: Expression

}

/** The iterator that performs hash join.
  *
  * Note: for now, you can assume it performs inner join.
  */
case class HashJoinIterator(
    left: SeccoIterator,
    right: BaseBuildIndexIterator,
    leftKeys: Seq[Expression],
//    rightKeys: Seq[Expression],
    joinCondition: Expression
) extends SeccoIterator {

  private var curLeftRow: InternalRow = _
  private var row: InternalRow = _
  private var hasNextCacheValid = true
  private var hasNextCache: Boolean = false
  private val initiallyHasNext: Boolean = tryGetNextWithNewLeftRow()

  private val joiner = GenerateUnsafeInternalRowJoiner.generate(
    (StructType.fromAttributes(left.localAttributeOrder()),
      StructType.fromAttributes(right.localAttributeOrder()))
  )

  private val conditionFunc = GeneratePredicate.generate(joinCondition, localAttributeOrder())

  private val getLeftKey = GenerateSafeProjection.generate(leftKeys, left.localAttributeOrder())

  private val rightIter = right.toIndexIterator()

  private def tryGetNextWithCurLeftRow(): Boolean = {
    while (!hasNextCache && rightIter.hasNext) {
      val tempRow = joiner.join(curLeftRow.asInstanceOf[UnsafeInternalRow],
        rightIter.next().asInstanceOf[UnsafeInternalRow])
      if (conditionFunc.eval(tempRow)) {
        hasNextCache = true
        row = tempRow
      }
    }
    hasNextCache
  }

  private def tryGetNextWithNewLeftRow(): Boolean = {
    while(!hasNextCache && left.hasNext) {
      curLeftRow = left.next()
      if (rightIter.setKey(getLeftKey(curLeftRow))) tryGetNextWithCurLeftRow()
    }
    hasNextCache
  }

  override def localAttributeOrder(): Array[Attribute] =
    left.localAttributeOrder() ++ right.localAttributeOrder()

  //lgh TODO: check whether the following is correct
  override def isSorted(): Boolean = left.isSorted() && right.isSorted()

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = {
    val rowArrayBuffer = ArrayBuffer[InternalRow]()
    val leftResultsIter = left.results().iterator
    val rightIndexIter = right.toIndexIterator()
//    val rightIndexIter = right.toIndexIterator().clone().asInstanceOf[SeccoIterator with IndexableSeccoIterator]di
    while(leftResultsIter.hasNext) {
      val curLeftResultsRow = leftResultsIter.next()
      if (rightIndexIter.setKey(getLeftKey(curLeftResultsRow))) {
        while (rightIter.hasNext) {
          val tempRow = joiner.join(curLeftResultsRow.asInstanceOf[UnsafeInternalRow],
            rightIndexIter.next().asInstanceOf[UnsafeInternalRow])
          if (conditionFunc.eval(tempRow)) rowArrayBuffer :+ tempRow
        }
      }
    }
    InternalBlock(rowArrayBuffer.toArray, StructType.fromAttributes(localAttributeOrder()))
  }

  override def children: Seq[SeccoIterator] = left :: right :: Nil

  override def hasNext: Boolean = initiallyHasNext &&  {
    if (!hasNextCacheValid) {
      hasNextCache=false
      tryGetNextWithCurLeftRow()
      tryGetNextWithNewLeftRow()
      hasNextCacheValid=true
    }
    hasNextCache
  }

  override def next(): InternalRow = {
    if(!hasNext) throw new NoSuchElementException("next on empty iterator")
    else
    {
      hasNextCacheValid = false
      row
    }
  }
}
