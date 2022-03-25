package org.apache.spark.secco.execution.plan.communication

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.plan.computation.utils.{
  ConsecutiveRowArray,
  InternalRowHashMap
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.types.StructType

/** The abstract class for representing a set of [[InternalRow]] stored in a block. */
abstract class InternalPartition extends Serializable {

  /** The output attributes of the block. */
  def output: Seq[Attribute]

  /** The schema of the block. */
  def schema: StructType = StructType.fromAttributes(output)

  /** The actual data in the block. */
  def blockContents: Seq[RawData]

  /** Check if the block is empty. */
  def isEmpty: Boolean
}

/** The trait for [[InternalPartition]] that can be indexed, which is a partition of a relation. */
trait Indexed {
  self: InternalPartition =>
  def partitioner: Partitioner = ???
  def index: Array[Int]
}

/** The abstract class for representing raw data inside the block. */
trait RawData {}

/** The raw block data organized as [[Array]]. */
case class ArrayData(content: Array[InternalRow]) extends RawData

///** The raw block data organized as [[Trie]]. */
//case class TrieData(content: Trie) extends RawData

/** The row block data organized as [[InternalRowHashMap]] */
case class HashMapData(content: InternalRowHashMap) extends RawData

/** The row block organized as an consecutive memory of tuples. */
case class RawArrayData(content: ConsecutiveRowArray) extends RawData

/** The generic raw block. */
case class GeneralRawData[V](content: V) extends RawData

/** The indexed block that composes of multiple [[InternalPartition]] */
case class MultiTableIndexedPartition(
    output: Seq[Attribute],
    index: Array[Int],
    subBlocks: Seq[InternalPartition]
) extends InternalPartition
    with Indexed {

  override def blockContents: Seq[RawData] =
    subBlocks.flatMap(_.blockContents)

  override def isEmpty: Boolean = subBlocks.forall(_.isEmpty)
}

/** The indexed block that stores Array[InternalRow]. */
case class ArrayIndexedPartition(
    output: Seq[Attribute],
    index: Array[Int],
    blockContent: ArrayData
) extends InternalPartition
    with Indexed {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The indexed block that stores [[InternalRowHashMap]]. */
case class HashMapIndexedPartition(
    output: Seq[Attribute],
    index: Array[Int],
    blockContent: HashMapData
) extends InternalPartition
    with Indexed {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The indexed block that stores [[Trie]]. */
//case class TrieIndexedPartition(
//    output: Seq[Attribute],
//    index: Array[Int],
//    blockContent: TrieData
//) extends InternalPartition
//    with Indexed {
//  override def blockContents: Seq[RawData] = Seq(blockContent)
//
//  //TODO: implement more efficient version
//  override def isEmpty: Boolean = blockContent.content.toInternalRows().isEmpty
//}

/** The indexed block that stores [[ConsecutiveRowArray]]. */
case class RawArrayIndexedPartition(
    output: Seq[Attribute],
    index: Array[Int],
    blockContent: RawArrayData
) extends InternalPartition
    with Indexed {
  override def blockContents: Seq[RawData] = Seq(blockContent)
  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The block that stores [[InternalRowHashMap]]. */
case class HashMapPartition(
    output: Seq[Attribute],
    blockContent: HashMapData
) extends InternalPartition {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The block that stores [[Trie]]. */
//case class TriePartition(
//    output: Seq[Attribute],
//    blockContent: TrieData
//) extends InternalPartition {
//  override def blockContents: Seq[RawData] = Seq(blockContent)
//
//  //TODO: implement more efficient version
//  override def isEmpty: Boolean = blockContent.content.toInternalRows().isEmpty
//}

/** The block that stores [[ConsecutiveRowArray]]. */
case class RawArrayPartition(
    output: Seq[Attribute],
    blockContent: RawArrayData
) extends InternalPartition {
  override def blockContents: Seq[RawData] = Seq(blockContent)
  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The block that stores Array[InternalRow]. */
case class ArrayPartition(
    output: Seq[Attribute],
    blockContent: ArrayData
) extends InternalPartition {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The block that stores multiple blocks. */
case class MultiPartition(
    output: Seq[Attribute],
    subBlocks: Seq[InternalPartition]
) extends InternalPartition {

  override def blockContents: Seq[RawData] =
    subBlocks.flatMap(_.blockContents)

  override def isEmpty: Boolean = subBlocks.forall(_.isEmpty)
}

/** The block that stores any data. */
case class GeneralPartition[V](
    output: Seq[Attribute],
    blockContent: GeneralRawData[V]
) extends InternalPartition {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = {
    throw new Exception("isEmpty is not implemented for GeneralBlock")
  }
}
