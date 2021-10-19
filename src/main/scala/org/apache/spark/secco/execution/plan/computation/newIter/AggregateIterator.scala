package org.apache.spark.secco.execution.plan.computation.newIter

import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{Attribute, NamedExpression}

/** The base class for performing aggregation via iterator */
sealed abstract class BaseAggregateIterator extends SeccoIterator {

  /** The group attributes */
  def groupingExpression: Array[NamedExpression]

  /** The aggregate expressions. */
  def aggregateExpressions: Array[NamedExpression]

}

/** The iterator that performs aggregation */
case class AggregateIterator(
    childIter: SeccoIterator,
    groupingExpressionn: Array[NamedExpression],
    aggregateExpressions: Array[NamedExpression]
) extends SeccoIterator {

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}

/** The iterator that performs aggregation and support index-like operations.
  *
  * Note that keyAttributes should be the same as grouping expression after convert named expression to attribute.
  */
case class IndexableAggregateIterator(
    childIter: SeccoIterator,
    keyAttributes: Array[Attribute],
    groupingExpression: Array[NamedExpression],
    aggregateExpressions: Array[NamedExpression]
) extends SeccoIterator
    with IndexableSeccoIterator {

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???

  /** Set the key for this iterator
    *
    * @param key the key in [[InternalRow]]
    * @return return true if the key exists, otherwise return false
    */
  override def setKey(key: InternalRow): Boolean = ???

  /** Get one row for the given key
    *
    * @param key the key in [[InternalRow]]
    * @return one of the row under given key
    */
  override def getOneRow(key: InternalRow): Option[InternalRow] = ???

  /** Get one row for the given key
    *
    * @param key the key in [[InternalRow]]
    * @return if the key exists returns one row, otherwise return null
    */
  override def unsafeGetOneRow(key: InternalRow): InternalRow = ???
}
