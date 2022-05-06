package org.apache.spark.secco.execution.storage

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.plan.communication.{
  Coordinate,
  HyperCubePartitioner
}
import org.apache.spark.secco.execution.plan.computation.utils.{
  ConsecutiveRowArray,
  InternalRowHashMap
}
import org.apache.spark.secco.execution.storage.block.{
  ColumnarInternalBlock,
  GenericInternalRowBlock,
  HashMapInternalBlock,
  HashSetInternalBlock,
  InternalBlock,
  TrieInternalBlock,
  UnsafeInternalRowBlock
}
import org.apache.spark.secco.execution.storage.row.GenericInternalRow
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

  /** For [[HyperCubePartitioner]], the coordinate stores the hash
    * values on each attributes.
    */
  def coordinate: Option[Coordinate]

  /** The actual data in block. */
  def data: Seq[InternalBlock]

  /** The head block in data. */
  def headBlock: InternalBlock = data.head

  /** Check if the partition is empty.
    *
    * Note: we assume that all data is stored on the first block.
    */
  def isEmpty: Boolean = data.isEmpty || data.head.isEmpty()

  override def toString: String = {
    s"""
       |${getClass.getName}
       |schema:${schema}
       |coordinate:${coordinate}
       |data: ${data.mkString(",")}
       |
       |""".stripMargin
  }

}

object InternalPartition {

  def fromInternalBlock(
      output: Seq[Attribute],
      block: InternalBlock,
      index: Option[Coordinate] = None,
      partitioner: Option[Partitioner] = None
  ): InternalPartition = {
    block match {
      case u: UnsafeInternalRowBlock =>
        UnsafeRowBlockPartition(output, Seq(u), index, partitioner)
      case h: HashMapInternalBlock =>
        HashMapPartition(output, Seq(h), index, partitioner)
      case s: HashSetInternalBlock =>
        SetPartition(output, Seq(s), index, partitioner)
      case t: TrieInternalBlock =>
        TrieIndexedPartition(output, Seq(t), index, partitioner)
      case c: ColumnarInternalBlock =>
        ColumnarBlockPartition(output, Seq(c), index, partitioner)
      case g: GenericInternalRowBlock =>
        GenericRowBlockPartition(output, Seq(g), index, partitioner)
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

/** The partition that composes of [[UnsafeInternalRowBlock]].
  *
  * Note: we assume all data is stored in the first [[InternalBlock]]
  */
case class UnsafeRowBlockPartition(
    output: Seq[Attribute],
    data: Seq[UnsafeInternalRowBlock],
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

/** The partition that composes of [[GenericInternalRowBlock]]. */
case class GenericRowBlockPartition(
    output: Seq[Attribute],
    data: Seq[GenericInternalRowBlock],
    coordinate: Option[Coordinate],
    partitioner: Option[Partitioner]
) extends InternalPartition {}
