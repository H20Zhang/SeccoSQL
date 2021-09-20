package org.apache.spark.secco.execution.storage.block

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.storage.row.{
  GenericInternalRow,
  InternalRow,
  UnsafeInternalRow
}
import org.apache.spark.secco.types.StructType

import scala.collection.mutable

class HashSetInternalBlock extends InternalBlock with SetLike {
  var rows: mutable.HashSet[InternalRow] = _
  var blockSchema: StructType = _

  def this(rows: mutable.HashSet[InternalRow], blockSchema: StructType) {
    this()
    this.rows = rows
    this.blockSchema = blockSchema
  }

  /** The iterator for accessing InternalBlock.
    * Note: The [[InternalRow]] returned by this class will be reused.
    */
  override def iterator: Iterator[InternalRow] = rows.toIterator

  /** Return the numbers of rows of [[InternalBlock]] */
  override def size(): Long = rows.size

  /** Return true if the [[InternalBlock]] is empty */
  override def isEmpty(): Boolean = rows.isEmpty

  /** Return true if the [[InternalBlock]] is non-empty */
  override def nonEmpty(): Boolean = rows.nonEmpty

  /** Return the schema of [[InternalBlock]] */
  override def schema(): StructType = blockSchema

  /** Sort the rows by an dictionary order.
    *
    * @param DictionaryOrder the dictionary orders to be used to sort InternalRow.
    * @return a new sorted InternalBlock.
    */
  override def sortBy(DictionaryOrder: Seq[String]): InternalBlock =
    throw new Exception("sort is not allowed in HashSetInternalBlock")

  /** Merge two (sorted) [[InternalBlock]]
    *
    * @param other             the other [[InternalBlock]] to be merged
    * @param maintainSortOrder whether the sorting order in InternalBlock should be maintained in merged [[InternalBlock]]
    * @return an merged (sorted) [[InternalBlock]]
    */
  override def merge(
      other: InternalBlock,
      maintainSortOrder: Boolean
  ): InternalBlock = {
    if (maintainSortOrder)
      throw new Exception("sort is not allowed in HashMapInternalBlock")
    val newHashSet = mutable.HashSet[InternalRow]()
    val otherIterator = other.iterator
    while (otherIterator.hasNext) {
      val otherInternalRow = otherIterator.next()
      newHashSet.add(otherInternalRow)
    }
    for (row <- rows) newHashSet.add(row)
    new HashSetInternalBlock(newHashSet, blockSchema)
  }

  /** Partition an [[InternalBlock]] into multiple [[InternalBlock]]s based on a partitioner.
    *
    * @param partitioner the partitioner used to partition the [[InternalBlock]]
    * @return an array of partitioned [[InternalBlock]]s
    */
  override def partitionBy(partitioner: Partitioner): Array[InternalBlock] = ???

  /** Show the first `num` rows */
  override def show(num: Int): Unit = {
    val showRows = rows.take(num)
    for (row <- showRows) {
      if (row.isInstanceOf[GenericInternalRow]) println(row)
      else row.asInstanceOf[UnsafeInternalRow].show(blockSchema)
    }
  }

  /** Convert the InternalBlock to array of [[InternalRow]] */
  override def toArray(): Array[InternalRow] = rows.toArray

  override def contains(key: InternalRow): Boolean = rows.contains(key)

  override def getDictionaryOrder: Option[Seq[String]] =
    throw new Exception("No dictionaryOrder in HashSetInternalBlock")
}

class HashSetInternalBlockBuilder(schema: StructType)
    extends InternalBlockBuilder {
  val rows: mutable.HashSet[InternalRow] = mutable.HashSet[InternalRow]()

  /** Add a new row to builder. */
  override def add(row: InternalRow): Unit = rows.add(row.copy())

  /** Build the InternalBlock. */
  override def build(): HashSetInternalBlock =
    new HashSetInternalBlock(rows, schema)
}
