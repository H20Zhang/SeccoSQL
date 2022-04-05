package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.codegen.{GeneratePredicate, GenerateSafeProjection, GenerateUnsafeInternalRowJoiner, GenerateUnsafeProjection}
import org.apache.spark.secco.expression.{Attribute, Expression, JoinedRow}
import org.apache.spark.secco.types.StructType
import org.apache.spark.secco.util.misc.LogAble

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
    right: BuildHashMap,
    leftKeys: Seq[Expression],
    joinCondition: Expression
) extends SeccoIterator with LogAble {

  private var curLeftRow: InternalRow = _
  private var curRightIter: Iterator[InternalRow] = _
  private var row: InternalRow = _
  private var hasNextCacheValid = true
  private var hasNextCache: Boolean = false

  private lazy val joiner = GenerateUnsafeInternalRowJoiner.generate(
    (StructType.fromAttributes(left.localAttributeOrder()),
      StructType.fromAttributes(right.localAttributeOrder())
  ))
  private lazy val pseudoJoinRow = new JoinedRow()

  private lazy val conditionFunc = GeneratePredicate.generate(joinCondition, localAttributeOrder())

//  private lazy val getLeftKey = GenerateSafeProjection.generate(leftKeys, left.localAttributeOrder())
  private lazy val getLeftKey = GenerateUnsafeProjection.generate(leftKeys, left.localAttributeOrder())

  private lazy val hashMapBlock = right.results()

  private def tryGetNextWithCurLeftRow(): Boolean = {
    while (!hasNextCache && curRightIter.hasNext) {
      val curRightRow = curRightIter.next()
      val tempRow = pseudoJoinRow(curLeftRow, curRightRow)
      logTrace(s"in tryGetNextWithCurLeftRow(): tempRow=$tempRow")
      if (conditionFunc.eval(tempRow)) {
        hasNextCache = true
        row = joiner.join(curLeftRow.asInstanceOf[UnsafeInternalRow],
          curRightRow.asInstanceOf[UnsafeInternalRow]).copy()
        logTrace(s"in tryGetNextWithCurLeftRow(): conditionFunc.eval(tempRow)=true, joinCondition=$joinCondition")
      } else
        logTrace(s"in tryGetNextWithCurLeftRow(): conditionFunc.eval(tempRow)=false, joinCondition=$joinCondition")

    }
    hasNextCache
  }

  private def tryGetNextWithNewLeftRow(): Boolean = {
    while(!hasNextCache && left.hasNext) {
      curLeftRow = left.next()
      val key = getLeftKey(curLeftRow)
      if (hashMapBlock.contains(key)) {
        logTrace(s"in tryGetNextWithNewLeftRow(): right.contains(key)=true, key=$key ")
        curRightIter = hashMapBlock.get(key).toIterator
        tryGetNextWithCurLeftRow()
      }
      else
        logTrace(s"in tryGetNextWithNewLeftRow(): right.contains(key)=false, key=$key ")
    }
    hasNextCache
  }

  private lazy val initiallyHasNext: Boolean = tryGetNextWithNewLeftRow()

  if(!initiallyHasNext)
    logTrace(s"in HashJoinIterator: initiallyHasNext=$initiallyHasNext")

  override def localAttributeOrder(): Array[Attribute] =
    left.localAttributeOrder() ++ right.localAttributeOrder

  override def isSorted(): Boolean = left.isSorted() && right.isSorted

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = {
    val rowArrayBuffer = ArrayBuffer[InternalRow]()
    val leftRowArray = left.results().toArray()
    for(curLeftResultsRow <- leftRowArray) {
      val key = getLeftKey(curLeftResultsRow)
      if (hashMapBlock.contains(key)) {
        val curRightIter = hashMapBlock.get(key).toIterator
        while (curRightIter.hasNext) {
          val tempRow = pseudoJoinRow(curLeftResultsRow, curRightIter.next())
          if (conditionFunc.eval(tempRow)) rowArrayBuffer.append(tempRow.copy())
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
      UnsafeInternalRow.fromInternalRow(StructType.fromAttributes(localAttributeOrder()), row)
    }
  }
}
