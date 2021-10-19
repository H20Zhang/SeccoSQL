package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.codegen.BaseProjectionFunc
import org.apache.spark.secco.expression.{Attribute, NamedExpression}

/** The base class for performing project via iterator. */
sealed abstract class BaseProjectIterator extends SeccoIterator {

  def projectionList: Array[NamedExpression]

  def projectionFunctions: Array[BaseProjectionFunc]

}

/** The iterator that performs projection */
case class ProjectIterator(
    childIter: SeccoIterator,
    projectionList: Array[NamedExpression]
) extends BaseProjectIterator {

  override def projectionFunctions: Array[BaseProjectionFunc] = ???

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???

  override def children: Seq[SeccoIterator] = childIter :: Nil
}

/** The iterator that performs projection and support index-like operation if childIter support index-like operations
  *
  * Note that: not all projectionList is valid in this context, as we need to get the inverse function of projectionList
  * to help us compute the key to query childIter, which may not always exists.
  *
  * Also, currently we only support compute the inverse function of very simple expressions in projectionList, i.e.,
  * simple arithmetic operations (x, x+y, x-y)
  */
case class IndexableProjectIterator(
    childIter: SeccoIterator with IndexableSeccoIterator,
    projectionList: Array[NamedExpression]
) extends BaseProjectIterator
    with IndexableSeccoIterator {

  override def projectionFunctions: Array[BaseProjectionFunc] = ???

  override def keyAttributes(): Array[Attribute] = ???

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
