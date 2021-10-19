package org.apache.spark.secco.execution.plan.computation.newIter

import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{Attribute, Expression}
import org.apache.spark.secco.expression.codegen.PredicateFunc

/** The base class for performing selection via iterator. */
sealed abstract class BaseSelectIterator extends SeccoIterator {

  /** The condition for filtering the tuples */
  def condition(): Expression

  /** The generated function for filtering the tuples */
  def filterFunc(): PredicateFunc
}

/** The iterator that performs selection operation */
case class SelectIterator(childIter: SeccoIterator, condition: Expression)
    extends BaseSelectIterator {

  override def filterFunc(): PredicateFunc = ???

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???

  override def children: Seq[SeccoIterator] = childIter :: Nil
}

/** The iterator that performs selection operation and also support index-like operations if its
  * chlidIter support index-like operations.
  */
case class IndexableSelectIterator(
    childIter: SeccoIterator with IndexableSeccoIterator,
    condition: Expression,
    keyAttributes: Array[Attribute]
) extends BaseSelectIterator
    with IndexableSeccoIterator {

  override def filterFunc(): PredicateFunc = ???

  override def setKey(key: InternalRow): Boolean = ???

  override def getOneRow(key: InternalRow): Option[InternalRow] = ???

  override def unsafeGetOneRow(key: InternalRow): InternalRow = ???

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???

  override def children: Seq[SeccoIterator] = childIter :: Nil
}
