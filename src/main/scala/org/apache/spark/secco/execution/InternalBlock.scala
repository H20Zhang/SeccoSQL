package org.apache.spark.secco.execution

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.plan.computation.utils.{
  ConsecutiveRowArray,
  InternalRowHashMap,
  Trie
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.types.StructType

/** The abstract class for representing a set of [[InternalRow]] stored in a block. */
abstract class InternalBlock extends Serializable {

  /** The output attributes of the block. */
  def output: Seq[Attribute]

  /** The schema of the block. */
  def schema: StructType = StructType.fromAttributes(output)

  /** The actual data in the block. */
  def blockContents: Seq[RawData]

  /** Check if the block is empty. */
  def isEmpty: Boolean
}

/** The trait for [[InternalBlock]] that can be indexed, which is a partition of a relation. */
trait Indexed {
  self: InternalBlock =>
  def partitioner: Partitioner = ???
  def index: Array[Int]
}

/** The abstract class for representing raw data inside the block. */
trait RawData {}

/** The raw block data organized as [[Array]]. */
case class ArrayData(content: Array[InternalRow]) extends RawData

/** The raw block data organized as [[Trie]]. */
case class TrieData(content: Trie) extends RawData

/** The row block data organized as [[InternalRowHashMap]] */
case class HashMapData(content: InternalRowHashMap) extends RawData

/** The row block organized as an consecutive memory of tuples. */
case class RawArrayData(content: ConsecutiveRowArray) extends RawData

/** The generic raw block. */
case class GeneralRawData[V](content: V) extends RawData

/** The indexed block that composes of multiple [[InternalBlock]] */
case class MultiTableIndexedBlock(
    output: Seq[Attribute],
    index: Array[Int],
    subBlocks: Seq[InternalBlock]
) extends InternalBlock
    with Indexed {

  override def blockContents: Seq[RawData] =
    subBlocks.flatMap(_.blockContents)

  override def isEmpty: Boolean = subBlocks.forall(_.isEmpty)
}

/** The indexed block that stores Array[InternalRow]. */
case class ArrayIndexedBlock(
    output: Seq[Attribute],
    index: Array[Int],
    blockContent: ArrayData
) extends InternalBlock
    with Indexed {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The indexed block that stores [[InternalRowHashMap]]. */
case class HashMapIndexedBlock(
    output: Seq[Attribute],
    index: Array[Int],
    blockContent: HashMapData
) extends InternalBlock
    with Indexed {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The indexed block that stores [[Trie]]. */
case class TrieIndexedBlock(
    output: Seq[Attribute],
    index: Array[Int],
    blockContent: TrieData
) extends InternalBlock
    with Indexed {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  //TODO: implement more efficient version
  override def isEmpty: Boolean = blockContent.content.toInternalRows().isEmpty
}

/** The indexed block that stores [[ConsecutiveRowArray]]. */
case class RawArrayIndexedBlock(
    output: Seq[Attribute],
    index: Array[Int],
    blockContent: RawArrayData
) extends InternalBlock
    with Indexed {
  override def blockContents: Seq[RawData] = Seq(blockContent)
  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The block that stores [[InternalRowHashMap]]. */
case class HashMapBlock(
    output: Seq[Attribute],
    blockContent: HashMapData
) extends InternalBlock {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The block that stores [[Trie]]. */
case class TrieBlock(
    output: Seq[Attribute],
    blockContent: TrieData
) extends InternalBlock {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  //TODO: implement more efficient version
  override def isEmpty: Boolean = blockContent.content.toInternalRows().isEmpty
}

/** The block that stores [[ConsecutiveRowArray]]. */
case class RawArrayBlock(
    output: Seq[Attribute],
    blockContent: RawArrayData
) extends InternalBlock {
  override def blockContents: Seq[RawData] = Seq(blockContent)
  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The block that stores Array[InternalRow]. */
case class ArrayBlock(
    output: Seq[Attribute],
    blockContent: ArrayData
) extends InternalBlock {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

/** The block that stores multiple blocks. */
case class MultiBlock(
    output: Seq[Attribute],
    subBlocks: Seq[InternalBlock]
) extends InternalBlock {

  override def blockContents: Seq[RawData] =
    subBlocks.flatMap(_.blockContents)

  override def isEmpty: Boolean = subBlocks.forall(_.isEmpty)
}

/** The block that stores any data. */
case class GeneralBlock[V](
    output: Seq[Attribute],
    blockContent: GeneralRawData[V]
) extends InternalBlock {
  override def blockContents: Seq[RawData] = Seq(blockContent)

  override def isEmpty: Boolean = {
    throw new Exception("isEmpty is not implemented for GeneralBlock")
  }
}
