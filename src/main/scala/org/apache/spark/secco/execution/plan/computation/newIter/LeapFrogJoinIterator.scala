package org.apache.spark.secco.execution.plan.computation.newIter

import org.apache.spark.secco.execution.storage.block.{
  ArrayTrieInternalBlock,
  InternalBlock
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

/** The base class for performing leapfrog join via iterator */
sealed abstract class BaseLeapFrogJoinIterator extends SeccoIterator {

  /** The tries of the relations */
  def tries: Array[ArrayTrieInternalBlock]
}

/** The iterator that performs leapfrog join
  * @param children the children which is IndexableTableIterator with TrieInternalRow as block
  * @param localAttributeOrder the attribute order to perform leapfrog join
  *
  * Note: the output of leapfrog join is always sorted w.r.t localAttributeOrder.
  */
case class LeapFrogJoinIterator(
    children: Seq[IndexableTableIterator],
    localAttributeOrder: Array[Attribute]
) extends BaseLeapFrogJoinIterator {

  override def tries: Array[ArrayTrieInternalBlock] = ???

  override def isSorted(): Boolean = true

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}

/** The iterator that performs leapfrog join with index-like operations
  * @param children the children which is IndexableTableIterator with TrieInternalRow as block
  * @param localAttributeOrder the attribute order to perform leapfrog join
  *                            @param keyAttributes the key attributes
  *
  * Note: the output of leapfrog join is always sorted w.r.t localAttributeOrder.
  * The keyAttributes should be match the prefix of localAttributeOrder.
  */
case class IndexableLeapFrogIterator(
    children: Seq[IndexableTableIterator],
    localAttributeOrder: Array[Attribute],
    keyAttributes: Array[Attribute]
) extends BaseLeapFrogJoinIterator
    with IndexableSeccoIterator {

  override def tries: Array[ArrayTrieInternalBlock] = ???

  override def setKey(key: InternalRow): Boolean = ???

  override def getOneRow(key: InternalRow): Option[InternalRow] = ???

  override def unsafeGetOneRow(key: InternalRow): InternalRow = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}
