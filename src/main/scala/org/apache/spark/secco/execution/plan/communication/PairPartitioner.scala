package org.apache.spark.secco.execution.plan.communication.utils

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.OldInternalRow
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

/** The partitioner that partitions the relation according to the space defined by share,
  *  the i-th share decide how much "share" the domain of relation on i-th local attribute will be partitioned.
  */
//TODO: optimize the implementation.
class PairPartitioner(attributes: Array[Attribute], shares: AttributeMap[Int])
    extends Partitioner {

  assert(
    attributes.map(shares).map(_.toLong).product < Int.MaxValue,
    s"Total numbers of partitions is larger than Int.MaxValue"
  )

  val artiy = attributes.size
  val shareSpaceVector = attributes.map(shares)
  val productFactor = 1 +: Range(1, artiy)
    .map(i => shareSpaceVector.dropRight(artiy - i).product)
    .toArray

  override def numPartitions: Int = attributes.map(shares).product

  override def getPartition(key: Any): Int =
    key match {
      case row: InternalRow if row.numFields == artiy =>
        val coordinateBuffer = ArrayBuffer[Int]()
        var i = 0
        while (i < row.numFields) {
          coordinateBuffer += Utils.nonNegativeMod(
            row.get(i, attributes(i).dataType).hashCode(),
            shareSpaceVector(i)
          )
          i += 1
        }

        getServerId(coordinateBuffer)
      case _ =>
        throw new Exception(s"key:${key} is not supported in PairPartitioner")
    }

  /** Get the serverID based on the coordinate. */
  def getServerId(coordinate: Seq[Int]): Int = {
    assert(
      coordinate.forall(_ >= 0),
      s"all pos of coordiante:${coordinate} should >= 0"
    )
    coordinate.zipWithIndex.map { case (value, i) =>
      value * productFactor(i)
    }.sum
  }

  /** Get the coordinate based on the serverID. */
  def getCoordinate(serverId: Int): Array[Int] = {
    var i = artiy - 1
    val coordinate = new Array[Int](artiy)
    var remain = serverId
    while (i >= 0) {
      coordinate(i) = remain / productFactor(i)
      remain = remain % productFactor(i)
      i -= 1
    }
    coordinate
  }
}
