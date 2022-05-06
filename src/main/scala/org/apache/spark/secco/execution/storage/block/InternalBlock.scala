package org.apache.spark.secco.execution.storage.block

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.storage.row.{
  GenericInternalRow,
  InternalRow
}
import org.apache.spark.secco.types.StructType

import scala.reflect.ClassTag

/** The base trait for supporting additional function for [[InternalBlock]]. */
trait InternalBlockProperty {}

/** The base trait for supporting [[InternalBlock]] that can be used as index. */
trait IndexLike extends InternalBlockProperty {}

/** The trait for supporting Map like capability for [[InternalBlock]]. */
trait MapLike extends IndexLike {
  def contains(key: InternalRow): Boolean
  def get(key: InternalRow): Array[InternalRow]
}

/** The trait for supporting Trie like capability for [[InternalBlock]] */
trait TrieLike extends IndexLike {
  def containPrefix(prefix: InternalRow): Boolean
  //  def get[T: ClassTag](key: InternalRow): Array[T]
  def get(key: InternalRow): Array[Any]
  def getRows(key: InternalRow): Array[InternalRow]
}

/** The trait for supporting Set like capability for [[InternalBlock]]. */
trait SetLike extends IndexLike {
  def contains(key: InternalRow): Boolean
}

/** The trait for supporting accessing [[InternalBlock]] by row. */
trait RowLike extends InternalBlock {
  def getRow(i: Int): InternalRow
}

/** The trait for supporting accessing [[InternalBlock]] by column. */
trait ColumnLike extends InternalBlock {
  def getColumnByOrdinal[T: ClassTag](i: Int): Array[T]
  def getColumn[T: ClassTag](columnName: String): Array[T]
}

/** The base class for InternalBlock */
abstract class InternalBlock {

  /* DictionaryOrder, if sorted */
  def getDictionaryOrder: Option[Seq[String]]

  /** The iterator for accessing InternalBlock.
    *  Note: The [[InternalRow]] returned by this class will be reused.
    */
  def iterator: Iterator[InternalRow]

  /* Block Property */
  /** Return the numbers of rows of [[InternalBlock]] */
  def size(): Long

  /** Return true if the [[InternalBlock]] is empty */
  def isEmpty(): Boolean

  /** Return true if the [[InternalBlock]] is non-empty */
  def nonEmpty(): Boolean

  /** Return the schema of [[InternalBlock]] */
  def schema(): StructType

  /* Transformation */

  /** Sort the rows by an dictionary order.
    * @param DictionaryOrder the dictionary orders to be used to sort InternalRow.
    * @return a new sorted InternalBlock.
    */
  def sortBy(DictionaryOrder: Seq[String]): InternalBlock

  /** Merge two (sorted) [[InternalBlock]]
    * @param other the other [[InternalBlock]] to be merged
    * @param maintainSortOrder whether the sorting order in InternalBlock should be maintained in merged [[InternalBlock]]
    * @return an merged (sorted) [[InternalBlock]]
    */
  def merge(
      other: InternalBlock,
      maintainSortOrder: Boolean = true
  ): InternalBlock

  /** Partition an [[InternalBlock]] into multiple [[InternalBlock]]s based on a partitioner.
    * @param partitioner the partitioner used to partition the [[InternalBlock]]
    * @return an array of partitioned [[InternalBlock]]s
    */
  def partitionBy(partitioner: Partitioner): Array[InternalBlock]

  /* Misc */

  /** Show the first `num` rows */
  def show(num: Int = 20): Unit

  /** Convert the InternalBlock to array of [[InternalRow]] */
  def toArray(): Array[InternalRow]

  /** Convert to human readable string */
  def verboseString(isHumanReadable: Boolean = true): String = {

    if (isHumanReadable) {
      toArray()
        .map(f => new GenericInternalRow(f.toSeq(schema).toArray))
        .mkString("{", ",", "}")
    } else {
      toArray().map(_.toString).mkString("[", ";\n", "]")
    }

  }

  override def toString: String = verboseString()

}

object InternalBlock {

  /** This method can be used to construct a [[InternalBlock]] with rows and the schema given .
    */
//  def apply(rows: Array[InternalRow], schema: StructType): InternalBlock = UnsafeInternalBlock(rows, schema)
//  def apply(rows: Array[InternalRow], schema: StructType): InternalBlock = TrieInternalBlock(rows, schema)
  def apply(rows: Array[InternalRow], schema: StructType): InternalBlock =
    ColumnarInternalBlock(rows, schema)
//  def apply(rows: Array[InternalRow], schema: StructType): InternalBlock = GenericInternalBlock(rows, schema)
  // edited by lgh

  /** This method can be used to construct a [[InternalRow]] from a [[Seq]] of values.
    */
  def fromSeq(rows: Seq[InternalRow], schema: StructType): InternalBlock =
    UnsafeInternalRowBlock(rows.toArray, schema)

  /** Returns an empty [[InternalRow]]. */
  val empty = apply(Array.empty[InternalRow], new StructType)

}
