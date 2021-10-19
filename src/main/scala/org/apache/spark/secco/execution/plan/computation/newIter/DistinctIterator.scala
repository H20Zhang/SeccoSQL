package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

/** The base class for performing distinct operation via iterator */
sealed abstract class BaseDistinctIterator extends SeccoIterator {}

/** The iterator that performs distinct operation
  *
  * Note that: whether this operator is blocking relies on whether the child iterator is sorted.
  */
case class DistinctIterator(childIter: SeccoIterator)
    extends BaseDistinctIterator {

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}

/** The iterator that performs distinct operation and support index-like operations. */
case class IndexableDistinctIterator(
    childIter: SeccoIterator with IndexableSeccoIterator,
    keyAttributes: Array[Attribute]
) extends BaseDistinctIterator
    with IndexableSeccoIterator {

  override def setKey(key: InternalRow): Boolean = ???

  override def getOneRow(key: InternalRow): Option[InternalRow] = ???

  override def unsafeGetOneRow(key: InternalRow): InternalRow = ???

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}
