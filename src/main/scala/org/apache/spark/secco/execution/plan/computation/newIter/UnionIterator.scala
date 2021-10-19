package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

/** The base class for performing union via iterator. */
sealed abstract class BaseUnionIterator extends SeccoIterator {}

/** The iterator that performs union.
  *
  * Note: the sort order is maintained.
  */
case class UnionIterator(left: SeccoIterator, right: SeccoIterator)
    extends BaseUnionIterator {

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}
