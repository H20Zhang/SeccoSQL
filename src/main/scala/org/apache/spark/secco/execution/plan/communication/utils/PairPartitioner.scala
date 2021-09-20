package org.apache.spark.secco.execution.plan.communication.utils

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.InternalRow
import org.apache.spark.util.Utils

/** partition the relation according to the space defined by share,
  *  the i-th share decide how much "share" the domain of relation on i-th local attribute will be splited
  */
class PairPartitioner(shareSpaceVector: Array[Int]) extends Partitioner {

  assert(
    shareSpaceVector.map(_.toLong).product < Int.MaxValue,
    s"product of share:${shareSpaceVector.toSeq} is larger than Int.MaxValue"
  )

  val artiy = shareSpaceVector.size
  val productFactor = 1 +: Range(1, artiy)
    .map(i => shareSpaceVector.dropRight(artiy - i).product)
    .toArray

  def nonNegativeModForLong(x: Long, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0) toInt
  }

  def nonNegativeModForDouble(x: Double, mod: Int): Int = {
    val rawMod = x.toLong.hashCode() % mod
    rawMod + (if (rawMod < 0) mod else 0) toInt
  }

  override def numPartitions: Int = shareSpaceVector.product

  override def getPartition(key: Any): Int =
    key match {
      case array: Array[Int] if array.size == artiy =>
        val coordinate = array.zipWithIndex.map {
          case (value, i) =>
            Utils.nonNegativeMod(value, shareSpaceVector(i))
        }
        getServerId(coordinate)
      case array: Array[Long] if array.size == artiy =>
        val coordinate = array.zipWithIndex.map {
          case (value, i) => nonNegativeModForLong(value, shareSpaceVector(i))
        }
        getServerId(coordinate)
      case array: InternalRow if array.size == artiy =>
        val coordinate = array.zipWithIndex.map {
          case (value, i) => nonNegativeModForDouble(value, shareSpaceVector(i))
        }
        getServerId(coordinate)
      case _ =>
        throw new Exception(s"key:${key} is not supported in PairPartitioner")
    }

  def getServerId(coordinate: Seq[Int]): Int = {
    assert(
      coordinate.forall(_ >= 0),
      s"all pos of coordiante:${coordinate} should >= 0"
    )
    coordinate.zipWithIndex.map {
      case (value, i) => value * productFactor(i)
    }.sum
  }

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

//{
//  var i = 0
//  var hashValue = 0
//  while (i < artiy) {
//  val ithHashValue = Utils.nonNegativeMod(array(i), shareSpaceVector(i))
//  hashValue += (ithHashValue * productFactor(i))
//  i += 1
//}
//  hashValue
//}

//{
//  var i = 0
//  var hashValue = 0
//  while (i < artiy) {
//  val ithHashValue =
//  nonNegativeModForLong(array(i), shareSpaceVector(i))
//  hashValue += (ithHashValue * productFactor(i))
//  i += 1
//}
//  hashValue
//}
