package org.apache.spark.secco.execution.plan.computation.newIter

import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

/** The base class for building index. */
sealed abstract class BaseBuildIndexIterator extends BlockingSeccoIterator {

  /** build the index and convert it to a IndexableSeccoIterator */
  def toIndexIterator(): SeccoIterator with IndexableSeccoIterator

  override def isBreakPoint(): Boolean = true
}

case class BuildTrie(
    childIter: SeccoIterator,
    localAttributeOrder: Array[Attribute]
) extends BaseBuildIndexIterator {

  override def toIndexIterator(): SeccoIterator with IndexableSeccoIterator =
    ???

  override def isSorted(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = childIter :: Nil
}

case class BuildHashMap(childIter: SeccoIterator, keyAttrs: Array[Attribute])
    extends BaseBuildIndexIterator {

  override def toIndexIterator(): SeccoIterator with IndexableSeccoIterator =
    ???

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = childIter :: Nil
}

case class BuildSet(childIter: SeccoIterator, keyAttrs: Array[Attribute])
    extends BaseBuildIndexIterator {

  override def toIndexIterator(): SeccoIterator with IndexableSeccoIterator =
    ???

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = childIter :: Nil
}
