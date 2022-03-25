package org.apache.spark.secco.execution.storage

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.plan.communication.{
  Coordinate,
  PairPartitioner
}
import org.apache.spark.secco.execution.plan.computation.utils.{
  ConsecutiveRowArray,
  InternalRowHashMap
}
import org.apache.spark.secco.execution.storage.block.{
  ColumnarInternalBlock,
  GenericInternalBlock,
  HashMapInternalBlock,
  HashSetInternalBlock,
  InternalBlock,
  TrieInternalBlock,
  UnsafeInternalBlock
}
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.types.StructType

/** The abstract class for representing a partition of data inside a machine. */
abstract class InternalPartition extends Serializable {

  /** The output attributes of the partition. */
  def output: Seq[Attribute]

  /** The schema of the partition. */
  def schema: StructType = StructType.fromAttributes(output)

  /** The partitioner used to create this partition. */
  def partitioner: Option[Partitioner]

  /** For [[PairPartitioner]], the coordinate stores the hash
    * values on each attributes.
    */
  def coordinate: Option[Coordinate]

  /** The actual data in block. */
  def data: Seq[InternalBlock]

  /** Check if the partition is empty.
    *
    * Note: we assume that all data is stored on the first block.
    */
  def isEmpty: Boolean = data.isEmpty || data.head.isEmpty()
}

object InternalPartition {

  def fromInternalBlock(
      output: Seq[Attribute],
      block: InternalBlock,
      index: Option[Coordinate] = None,
      partitioner: Option[Partitioner] = None
  ): InternalPartition = {
    block match {
      case u: UnsafeInternalBlock =>
        UnsafeBlockPartition(output, Seq(u), index, partitioner)
      case h: HashMapInternalBlock =>
        HashMapPartition(output, Seq(h), index, partitioner)
      case s: HashSetInternalBlock =>
        SetPartition(output, Seq(s), index, partitioner)
      case t: TrieInternalBlock =>
        TrieIndexedPartition(output, Seq(t), index, partitioner)
      case c: ColumnarInternalBlock =>
        ColumnarBlockPartition(output, Seq(c), index, partitioner)
      case g: GenericInternalBlock =>
        GenericBlockPartition(output, Seq(g), index, partitioner)
      case _ => throw new Exception("not supported block type")
    }
  }
}

/** The partition that composes of multiple [[InternalPartition]] */
case class PairedPartition(
    output: Seq[Attribute],
    pairedPartitions: Seq[InternalPartition],
    coordinate: Option[Coordinate],
    partitioner: Option[Partitioner]
) extends InternalPartition {

  override def data: Seq[InternalBlock] =
    pairedPartitions.flatMap(_.data)

  override def isEmpty: Boolean = pairedPartitions.forall(_.isEmpty)
}

/** The partition that composes of [[UnsafeInternalBlock]].
  *
  * Note: we assume all data is stored in the first [[InternalBlock]]
  */
case class UnsafeBlockPartition(
    output: Seq[Attribute],
    data: Seq[UnsafeInternalBlock],
    coordinate: Option[Coordinate],
    partitioner: Option[Partitioner]
) extends InternalPartition {}

/** The partition that composes of [[HashMapInternalBlock]]. */
case class HashMapPartition(
    output: Seq[Attribute],
    data: Seq[HashMapInternalBlock],
    coordinate: Option[Coordinate],
    partitioner: Option[Partitioner]
) extends InternalPartition {}

/** The partition that composes of [[TrieInternalBlock]]. */
case class TrieIndexedPartition(
    output: Seq[Attribute],
    data: Seq[TrieInternalBlock],
    coordinate: Option[Coordinate],
    partitioner: Option[Partitioner]
) extends InternalPartition {}

/** The partition that composes of [[HashSetInternalBlock]]. */
case class SetPartition(
    output: Seq[Attribute],
    data: Seq[HashSetInternalBlock],
    coordinate: Option[Coordinate],
    partitioner: Option[Partitioner]
) extends InternalPartition {}

/** The partition that composes of [[ColumnarInternalBlock]]. */
case class ColumnarBlockPartition(
    output: Seq[Attribute],
    data: Seq[ColumnarInternalBlock],
    coordinate: Option[Coordinate],
    partitioner: Option[Partitioner]
) extends InternalPartition {}

/** The partition that composes of [[GenericInternalBlock]]. */
case class GenericBlockPartition(
    output: Seq[Attribute],
    data: Seq[GenericInternalBlock],
    coordinate: Option[Coordinate],
    partitioner: Option[Partitioner]
) extends InternalPartition {}
