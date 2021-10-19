package org.apache.spark.secco.execution.plan.computation.newIter

import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.trees.TreeNode

/** The base trait for an operator in a pipeline. */
trait SeccoIterator extends TreeNode[SeccoIterator] with Iterator[InternalRow] {

  /** The attribute order of the output.
    *
    * Note: if the isSorted is true, the output is sorted in dictionary order.
    */
  def localAttributeOrder(): Array[Attribute]

  /** If the output is sorted with dictionary order under [[localAttributeOrder]] */
  def isSorted(): Boolean

  /** If this operator is the pipeline breaker, i.e., whether it blocks the pipeline */
  def isBreakPoint(): Boolean

  /** Return the whole results of this iterator in [[InternalBlock]] */
  def results(): InternalBlock

  override def nodeName: String =
    getClass.getSimpleName.replaceAll("Iterator$", "")

  override def verboseString: String = simpleString

}

/** The trait for an blocking operator in a pipeline */
trait BlockingSeccoIterator extends SeccoIterator {
  override def hasNext: Boolean = throw new NoSuchMethodException()

  override def next(): InternalRow = throw new NoSuchMethodException()

  override def isBreakPoint(): Boolean = true
}

/** The trait for operator that support index-like operations in a pipeline.
  *
  * For such an operator, it needs to support two functions:
  *
  *   - setKey: sets the key, and the values can be retrieved via iterator interface, i.e., hasNext, and next.
  *   - getOneRow: get one row of the value based on key.
  */
trait IndexableSeccoIterator {

  self: SeccoIterator =>

  /** The key attributes */
  def keyAttributes(): Array[Attribute]

  /** Set the key for this iterator
    * @param key the key in [[InternalRow]]
    * @return return true if the key exists, otherwise return false
    */
  def setKey(key: InternalRow): Boolean

  /** Get one row for the given key
    * @param key the key in [[InternalRow]]
    * @return one of the row under given key
    */
  def getOneRow(key: InternalRow): Option[InternalRow]

  /** Get one row for the given key
    * @param key the key in [[InternalRow]]
    * @return if the key exists returns one row, otherwise return null
    */
  def unsafeGetOneRow(key: InternalRow): InternalRow
}
