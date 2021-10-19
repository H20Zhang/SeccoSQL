package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

/** The base class for cartesian production iterators. */
sealed abstract class BaseCartesianProductIterator extends SeccoIterator {}

/** The iterator that performs the cartesian product operation.
  *
  * Note that: the sort order is maintained.
  */
case class CartesianProductIterator(left: SeccoIterator, right: SeccoIterator)
    extends SeccoIterator {

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}
