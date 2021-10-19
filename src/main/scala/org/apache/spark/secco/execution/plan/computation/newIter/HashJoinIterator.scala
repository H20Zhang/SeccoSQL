package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{Attribute, Expression}

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
    right: SeccoIterator with IndexableSeccoIterator,
    joinCondition: Expression
) extends SeccoIterator {

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}
