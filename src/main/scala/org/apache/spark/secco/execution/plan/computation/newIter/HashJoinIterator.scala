package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.codegen.{GeneratePredicate, GenerateSafeProjection, GenerateUnsafeInternalRowJoiner}
import org.apache.spark.secco.expression.{Attribute, Expression, JoinedRow}
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
//    right: BaseBuildIndexIterator,
    right: IndexableHashMapTableIterator,
    leftKeys: Seq[Expression],
//    rightKeys: Seq[Expression],
    joinCondition: Expression
) extends SeccoIterator {

  private var curLeftRow: InternalRow = _
  private var row: InternalRow = _
  private var hasNextCacheValid = true
  private var hasNextCache: Boolean = false
  private val joiner = GenerateUnsafeInternalRowJoiner.generate(
    (StructType.fromAttributes(left.localAttributeOrder()),
      StructType.fromAttributes(right.localAttributeOrder)
  ))
//  private val joiner = new JoinedRow

  private val conditionFunc = GeneratePredicate.generate(joinCondition, localAttributeOrder())

  private val getLeftKey = GenerateSafeProjection.generate(leftKeys, left.localAttributeOrder())

//  private val rightIter = right.toIndexIterator()
  private val rightIter = right

  private def tryGetNextWithCurLeftRow(): Boolean = {
    while (!hasNextCache && rightIter.hasNext) {
      val tempRow = joiner.join(curLeftRow.asInstanceOf[UnsafeInternalRow],
        rightIter.next().asInstanceOf[UnsafeInternalRow]).copy()
      println(s"in tryGetNextWithCurLeftRow(): tempRow=$tempRow")
      if (conditionFunc.eval(tempRow)) {
        hasNextCache = true
        row = tempRow
        println(s"in tryGetNextWithCurLeftRow(): conditionFunc.eval(tempRow)=true, joinCondition=$joinCondition")
      } else
        println(s"in tryGetNextWithCurLeftRow(): conditionFunc.eval(tempRow)=false, joinCondition=$joinCondition")

    }
    hasNextCache
  }

  private def tryGetNextWithNewLeftRow(): Boolean = {
    while(!hasNextCache && left.hasNext) {
      curLeftRow = left.next()
      val key = getLeftKey(curLeftRow)
      if (rightIter.setKey(key)) {
        println(s"in tryGetNextWithNewLeftRow(): rightIter.setKey(key)=true, key=$key ")
        tryGetNextWithCurLeftRow()
      }
      else
        println(s"in tryGetNextWithNewLeftRow(): rightIter.setKey(key)=false, key=$key ")
    }
    hasNextCache
  }

  private val initiallyHasNext: Boolean = tryGetNextWithNewLeftRow()

  if(!initiallyHasNext)
    println(s"in HashJoinIterator: initiallyHasNext=$initiallyHasNext")

  override def localAttributeOrder(): Array[Attribute] =
    left.localAttributeOrder() ++ right.localAttributeOrder

  //lgh TODO: check whether the following is correct
  override def isSorted(): Boolean = left.isSorted() && right.isSorted

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = {
    val rowArrayBuffer = ArrayBuffer[InternalRow]()
    val leftRowArray = left.results().toArray()
//    val rightIndexIter = right.toIndexIterator()
    val rightIndexIter = right
//    val rightIndexIter = right.toIndexIterator().clone().asInstanceOf[SeccoIterator with IndexableSeccoIterator]di
    for(curLeftResultsRow <- leftRowArray) {
      val key = getLeftKey(curLeftResultsRow)
      if (rightIndexIter.setKey(key)) {
        while (rightIndexIter.hasNext) {
          val tempRow = joiner.join(curLeftResultsRow.asInstanceOf[UnsafeInternalRow],
            rightIndexIter.next().asInstanceOf[UnsafeInternalRow]).copy()
          if (conditionFunc.eval(tempRow)) rowArrayBuffer.append(tempRow)
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
