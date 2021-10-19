package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.{IndexLike, InternalBlock}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

/** The base class for table iterator */
sealed abstract class BaseTableIterator extends SeccoIterator {

  /** The underlying [[InternalBlock]] that supports the iterator */
  def block(): InternalBlock

}

/** The table iterator of the none index like [[InternalBlock]], i.e., [[GenericInternalBlock]],
  * [[UnsafeInternalBlock]]
  */
case class TableIterator(
    block: InternalBlock,
    localAttributeOrder: Array[Attribute],
    isSorted: Boolean
) extends BaseTableIterator {

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???

  override def children: Seq[SeccoIterator] = Seq()
}

/** The table iterator of index like [[InternalBlock]] */
case class IndexableTableIterator(
    block: InternalBlock with IndexLike,
    keyAttributes: Array[Attribute],
    localAttributeOrder: Array[Attribute],
    isSorted: Boolean
) extends BaseTableIterator
    with IndexableSeccoIterator {

  override def setKey(key: InternalRow): Boolean = ???

  override def getOneRow(key: InternalRow): Option[InternalRow] = ???

  override def unsafeGetOneRow(key: InternalRow): InternalRow = ???

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???

  override def children: Seq[SeccoIterator] = Seq()
}
