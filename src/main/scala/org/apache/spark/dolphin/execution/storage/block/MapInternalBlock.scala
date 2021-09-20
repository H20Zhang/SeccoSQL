package org.apache.spark.dolphin.execution.storage.block

import org.apache.spark.Partitioner
import org.apache.spark.dolphin.types.StructType
import org.apache.spark.dolphin.execution.storage.row.{InternalRow, UnsafeInternalRow, GenericInternalRow}

import scala.collection.mutable
import scala.collection.AbstractIterator

class HashMapInternalBlock extends InternalBlock with MapLike {
  /** The iterator for accessing InternalBlock.
   * Note: The [[InternalRow]] returned by this class will be reused.
   */
  private var rows: mutable.HashMap[InternalRow, InternalRow] = _
  private var blockSchema: StructType = _

  def this(rows: mutable.HashMap[InternalRow, InternalRow], blockSchema: StructType) {
    this()
    this.rows = rows
    this.blockSchema = blockSchema
  }

  override def iterator: Iterator[InternalRow] = new AbstractIterator[InternalRow] {
    val pseudoIterator: Iterator[(InternalRow, InternalRow)] = rows.toIterator

    override def hasNext: Boolean = pseudoIterator.hasNext

    override def next(): InternalRow = pseudoIterator.next()._2
  }

  /** Return the numbers of rows of [[InternalBlock]] */
  override def size(): Long = rows.size

  /** Return true if the [[InternalBlock]] is empty */
  override def isEmpty(): Boolean = rows.isEmpty

  /** Return true if the [[InternalBlock]] is non-empty */
  override def nonEmpty(): Boolean = rows.nonEmpty

  /** Return the schema of [[InternalBlock]] */
  override def schema(): StructType = blockSchema

  /**
   * Sort the rows by an dictionary order.
   *
   * @param DictionaryOrder the dictionary orders to be used to sort InternalRow.
   * @return a new sorted InternalBlock.
   */
  override def sortBy(DictionaryOrder: Seq[String]): InternalBlock = throw new Exception("sort is not allowed in HashMapInternalBlock")

  /**
   * Merge two (sorted) [[InternalBlock]]
   *
   * @param other             the other [[InternalBlock]] to be merged
   * @param maintainSortOrder whether the sorting order in InternalBlock should be maintained in merged [[InternalBlock]]
   * @return an merged (sorted) [[InternalBlock]]
   */
  override def merge(other: InternalBlock, maintainSortOrder: Boolean): InternalBlock = {
    if (maintainSortOrder) throw new Exception("sort is not allowed in HashMapInternalBlock")
    val otherIterator = other.iterator
    val newHashMap = new mutable.HashMap[InternalRow, InternalRow]()
    while (otherIterator.hasNext) {
      val otherInternalRow = otherIterator.next().copy()
      newHashMap.put(otherInternalRow, otherInternalRow)
    }
    for ((key, value) <- rows) {
      newHashMap.put(key.copy(), value.copy())
    }
    new HashMapInternalBlock(newHashMap, blockSchema)
  }

  /**
   * Partition an [[InternalBlock]] into multiple [[InternalBlock]]s based on a partitioner.
   *
   * @param partitioner the partitioner used to partition the [[InternalBlock]]
   * @return an array of partitioned [[InternalBlock]]s
   */
  override def partitionBy(partitioner: Partitioner): Array[InternalBlock] = ???

  /** Show the first `num` rows */
  override def show(num: Int): Unit = {
    val showIterator = this.iterator.take(num)
    while (showIterator.hasNext) {
      val row = showIterator.next()
      if (row.isInstanceOf[GenericInternalRow]) println(row)
      else row.asInstanceOf[UnsafeInternalRow].show(blockSchema)
    }
  }

  /** Convert the InternalBlock to array of [[InternalRow]] */
  override def toArray(): Array[InternalRow] = {
    val returnArray = new Array[InternalRow](rows.size)
    var index = 0
    for ((key, value) <- rows) {
      returnArray(index) = value
      index += 1
    }
    returnArray
  }

  override def contains(key: InternalRow): Boolean = rows.contains(key)

  override def get(key: InternalRow): InternalRow = rows(key)

  override def getDictionaryOrder: Option[Seq[String]] = throw new Exception("No dictionaryOrder in HashMapInternalBlock")
}

class HashMapInternalBlockBuilder(schema: StructType) extends InternalBlockBuilder {
  val rows: mutable.HashMap[InternalRow, InternalRow] = mutable.HashMap[InternalRow, InternalRow]()

  /** Add a new row to builder. */
  override def add(row: InternalRow): Unit = rows.put(row.copy(), row.copy())

  /** Build the InternalBlock. */
  override def build(): HashMapInternalBlock = new HashMapInternalBlock(rows, schema)
}