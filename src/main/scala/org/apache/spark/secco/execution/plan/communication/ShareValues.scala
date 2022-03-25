package org.apache.spark.secco.execution.plan.communication

import org.apache.spark.secco.execution.plan.communication.ShareValues.findEquivilanceAttrs
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.execution.{OldInternalRow, SharedContext}
import org.apache.spark.secco.expression.{
  Attribute,
  Expression,
  PredicateHelper
}
import org.apache.spark.secco.expression.utils.{AttributeMap, AttributeSet}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** An coordinate in HyperCube.
  *
  * Note: it can be used to represent one of the following two things
  * 1. Coordinate of the sub-task in [[PullPairExchangeExec]], which can be used to identify partitions of relations to
  * load.
  * 2. Coordinate of the partitions of the relation.
  */
case class Coordinate(attributes: Array[Attribute], index: Array[Int])

/** The shares that defines HyperCube.
  *
  * In [[ShareValues]], each attribute is associated with an share number, which determines domains of hash functions on that
  * attribute.
  *
  * For example, for share {"a" -> 2, "b" -> 2}
  */
class ShareValues(
    var rawShares: AttributeMap[Int],
    var equivalenceAttrs: Array[AttributeSet]
) {

  def isInitialized(): Boolean = rawShares.isEmpty && equivalenceAttrs.isEmpty

  def apply(attr: Attribute): Int = rawShares(attr)

  /** Generate coordinates for performing HyperCube Shuffle.
    * @param attrs attributes that participates in HyperCube Shuffle. Note: Equivalence attributes will be assigned same
    *              coordinate point.
    * @return an [[Array]] of [[Coordinate]]
    */
  def genHyperCubeCoordinates(attrs: Seq[Attribute]): Array[Coordinate] = {

    // find representative for each attrs based on equivalenceAttrs
    val equiSet2Representative =
      equivalenceAttrs.map(equiSet => (equiSet, equiSet.head)).toMap

    val representativeAttrs = AttributeSet(attrs.map { attr =>
      equiSet2Representative
        .find { case (key, value) =>
          key.contains(attr)
        }
        .get
        ._2
    }).toArray

    val attrs2Representative = attrs.map { attr =>
      val representative = equiSet2Representative
        .find { case (key, value) =>
          key.contains(attr)
        }
        .get
        ._2
      (attr, representative)
    }.toMap

    // generate the raw coordinate values for the representatives attributes
    val attrsShareSpace = representativeAttrs
      .map(attr => rawShares(attr))
    var i = 0
    val attrsSize = attrsShareSpace.size
    var buffer = new ArrayBuffer[Array[Int]]()

    while (i < attrsSize) {
      if (i == 0) {
        buffer ++= Range(0, attrsShareSpace(i)).map(f => Array(f))
      } else {
        buffer = buffer.flatMap { shareVector =>
          Range(0, attrsShareSpace(i)).map(f => shareVector :+ f)
        }
      }
      i += 1
    }

    // assign coordinate values to other attributes
    val coordinates = buffer.map { rawCoordinate =>
      val index = attrs.map(attr =>
        rawCoordinate(representativeAttrs.indexOf(attrs2Representative(attr)))
      )

      Coordinate(attrs.toArray, index.toArray)
    }

    coordinates.toArray
  }

  /** Generate the partitioner for partitioning rows for HyperCube Shuffle. */
  def genTaskPartitioner(attributes: Array[Attribute]): PairPartitioner =
    new PairPartitioner(attributes, this)

  /** Generate the sentry tuples, which consists of (sentryTuple, isSentryTuple), for HyperCube Shuffle */
  def genSentryRows(attrs: Array[Attribute]): Array[(InternalRow, Boolean)] = {
    genHyperCubeCoordinates(attrs).map(f => (InternalRow(f.index), true))
  }

  /** Get the sub-coordinate under the given attributes. */
  def getSubCoordinate(
      coordinate: Coordinate,
      attributes: Array[Attribute]
  ): Coordinate = {
    val newIndex = attributes.map { attr =>
      coordinate.index(coordinate.attributes.indexOf(attr))
    }

    Coordinate(attributes, newIndex)
  }

}

object ShareValues extends PredicateHelper {
  def apply(
      rawShares: AttributeMap[Int],
      equalCondition: Expression
  ): ShareValues =
    new ShareValues(rawShares, findEquivilanceAttrs(equalCondition).toArray)
}

/** The context of share values. */
case class ShareValuesContext(shares: ShareValues)
    extends SharedContext[ShareValues](shares)

/** The constraint on the share number an attribute can take. */
class ShareConstraint(
    var rawConstraint: AttributeMap[Int],
    var equivalenceAttrs: Array[AttributeSet]
) {

  def isInitialized(): Boolean =
    rawConstraint.isEmpty && equivalenceAttrs.isEmpty

  def apply(attr: Attribute): Int = rawConstraint(attr)
}

object ShareConstraint extends PredicateHelper {
  def apply(
      rawConstraint: AttributeMap[Int],
      equalCondition: Expression
  ): ShareConstraint =
    new ShareConstraint(
      rawConstraint,
      findEquivilanceAttrs(equalCondition).toArray
    )
}

/** The context of share constraint. */
case class ShareConstraintContext(shareConstraint: ShareConstraint)
    extends SharedContext[ShareConstraint](shareConstraint)
