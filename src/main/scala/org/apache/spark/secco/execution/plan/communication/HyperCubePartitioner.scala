package org.apache.spark.secco.execution.plan.communication

import org.apache.spark.Partitioner
import org.apache.spark.secco.debug
import org.apache.spark.secco.execution.storage.row.{
  GenericInternalRow,
  InternalRow
}
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

/** The partitioner that partitions the relation according to the space defined by share,
  *  the i-th share decide how much "share" the domain of relation on i-th local attribute will be partitioned.
  *
  *  @param localAttributes Attributes to be partitioned.
  *  @param shareValues The ShareValues.
  */

//TODO: optimize the implementation.
class HyperCubePartitioner(
    localAttributes: Array[Attribute],
    shareValues: ShareValues
) extends Partitioner {

  /** The equivalence between attributes. */
  private val equiAttrs = shareValues.equivalenceAttrs

  /** Shares of the local attributes. */
  private val localShareMap = localAttributes
    .map(attr => (attr, shareValues.rawShares(attr)))
    .toMap

  /** The per position share value. */
  private val localShareVector = localAttributes.map(localShareMap)

  /** One localCoordinate in the hyperspace defined by the localShareVector. */
  private val localCoordinate = Coordinate(
    localAttributes,
    Array.fill(localAttributes.size)(0),
    equiAttrs
  )

  /** The representation attributes of the local attributes.. */
  private val repAttrs = localCoordinate.repAttrs

  /** Numbers of the representative attributes. */
  private val repAttrsArtiy = repAttrs.size

  /** The share value for the representative attributes. */
  private val repShareMap = localShareMap.map { case (key, value) =>
    (equiAttrs.attr2RepAttr(key), value)
  }

  /** The per position share value for representation attributes. */
  private val repShareVector = repAttrs.map(repShareMap)

  /** The product factor to transform localIndex of the representative attributes to a Server ID. */
  val repAttrsProductFactor = 1 +: Range(1, repAttrsArtiy)
    .map(i => repShareVector.dropRight(repAttrsArtiy - i).product)
    .toArray

  // Sanity check.
  assert(
    localAttributes.map(localShareMap).map(_.toLong).product < Int.MaxValue,
    s"Total numbers of partitions is larger than Int.MaxValue"
  )

  assert(
    localAttributes
      .map(attr => (equiAttrs.attr2RepAttr(attr), localShareMap(attr)))
      .groupBy(_._1)
      .forall(_._2.distinct.size == 1),
    s"Conflicts found in the share values for local attributes:${shareValues} w.r.t attributes corresponds to same representative attribtues."
  )

  override def numPartitions: Int = repAttrs.map(repShareMap).product

  override def getPartition(key: Any): Int =
    key match {
      case row: InternalRow if row.numFields == localAttributes.size =>
        var i = 0
        while (i < row.numFields) {
          localCoordinate(i) = Utils.nonNegativeMod(
            row.get(i, localAttributes(i).dataType).hashCode(),
            localShareVector(i)
          )
          i += 1
        }

        val serverId = getServerIdInternal(localCoordinate)

        serverId
      case _ =>
        val row = key.asInstanceOf[GenericInternalRow]
        throw new Exception(
          s"key:${key.getClass} is not supported in PairPartitioner"
        )
    }

  /** Same as getServerId but saves the sanity checking. */
  private def getServerIdInternal(coordinate: Coordinate): Int = {

    //TODO: debug here, we need to use the index from the coordinate rather than localIndex.

    var sum = 0
    var i = 0

    val repAttr2Attrs = equiAttrs.repAttr2Attr

    while (i < repAttrs.size) {

      // Get one one of the attr corresponds to representative attr.
      // Note: repAttr may not be one of the attr in attributes of partitioner.
      val attr = repAttr2Attrs(repAttrs(i))
        .filter(attr => localAttributes.contains(attr))
        .head

      sum += coordinate(
        localAttributes.indexOf(attr)
      ) * repAttrsProductFactor(i)
      i += 1
    }

    sum
  }

  /** Get the serverID based on the localCoordinate. */
  def getServerId(coordinate: Coordinate): Int = {
    assert(
      coordinate.index.forall(_ >= 0),
      s"all pos of localIndex:${coordinate} should >= 0"
    )

    assert(
      coordinate.attributes.toSeq == localAttributes.toSeq,
      s"attributes in given localCoordinate:${coordinate.attributes.toSeq} does not match with that of Partitioner:${localAttributes.toSeq}"
    )

    getServerIdInternal(coordinate)
  }

  /** Get the localCoordinate based on the server ID. */
  def getIndex(serverId: Int): Coordinate = {
    var i = repAttrsArtiy - 1
    val repIndex = new Array[Int](repAttrsArtiy)
    var remain = serverId
    while (i >= 0) {
      repIndex(i) = remain / repAttrsProductFactor(i)
      remain = remain % repAttrsProductFactor(i)
      i -= 1
    }

    val repAttr2IndexValue = AttributeMap(repAttrs.zip(repIndex))

//    debug("getIndex", repAttrs.toSeq, localAttributes.toSeq, repAttr2IndexValue)

    val attr2RepAttr = equiAttrs.attr2RepAttr

    val index =
      localAttributes.map(attr => repAttr2IndexValue(attr2RepAttr(attr)))

    Coordinate(localAttributes, index, shareValues.equivalenceAttrs)
  }
}
