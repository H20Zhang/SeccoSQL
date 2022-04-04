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
import org.apache.spark.secco.optimization.util.EquiAttributes
import org.apache.spark.secco.util.misc.LogAble

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** An coordinate in HyperCube.
  *
  * Note: it can be used to represent one of the following two things
  *  a. Coordinate of the sub-task in [[PullPairExchangeExec]], which can be used to identify partitions of relations to
  * load.
  *  a. Coordinate of the partitions of the relation.
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
    var equivalenceAttrs: EquiAttributes
) {

  def isInitialized(): Boolean = rawShares.isEmpty && equivalenceAttrs.isEmpty

  def apply(attr: Attribute): Int = rawShares(attr)

  /** Generate coordinates for performing HyperCube Shuffle.
    * @param attrs attributes that participates in HyperCube Shuffle. Note: Equivalence attributes will be assigned same
    *              coordinate point.
    * @return an [[Array]] of [[Coordinate]]
    */
  def genHyperCubeCoordinates(attrs: Seq[Attribute]): Array[Coordinate] = {

    val repAttrs = equivalenceAttrs.repAttrs
    val attr2RepAttr = equivalenceAttrs.attr2RepAttr

    // generate the raw coordinate values for the representatives attributes
    val attrsShareSpace = repAttrs
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
      val index =
        attrs.map(attr => rawCoordinate(repAttrs.indexOf(attr2RepAttr(attr))))

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
    new ShareValues(
      rawShares,
      EquiAttributes.fromCondition(rawShares.keys.toSeq, equalCondition)
    )
}

/** The context of share values. */
case class ShareValuesContext(shares: ShareValues)
    extends SharedContext[ShareValues](shares)

/** The class that constraint on the share number an attribute can take.
  *
  * Note:
  *  a. The equivalence attributes in terms of Equal To condition must share the same constraint.
  *  a.  Every attribute at least have a naive equivalence, i.e., itself.
  */
class ShareConstraint(
    var rawConstraint: AttributeMap[Int],
    var equivalenceAttrs: EquiAttributes
) extends LogAble {

  assert(
    AttributeSet(rawConstraint.keys)
      .subsetOf(equivalenceAttrs.toAttributeSet()),
    "Attributes in rawConstraint and equivilantAttrs does not match."
  )

  /** Merge the rawConstraint and equivalenceAttrs from another [[ShareConstraint]] */
  def addNewConstraints(shareConstraint: ShareConstraint): Unit = {

    // Compute the new equivalence attributes.
    val newEquivalenceAttrs =
      equivalenceAttrs.merge(shareConstraint.equivalenceAttrs)

    // Compute the new raw constraints. Note: we assume all raw constraints value is 1.
    val newRawConstraint = AttributeMap(
      newEquivalenceAttrs.repAttr2Attr.toSeq
        .filter { case (_, equiAttrs) =>
          rawConstraint.find { case (key, value) =>
            equiAttrs.contains(key)
          }.nonEmpty
        }
        .flatMap { case (_, equiAttrs) => equiAttrs.toSeq.map(g => (g, 1)) }
    )

    // Update rawConstraint and equivalenceAttrs.
    rawConstraint = newRawConstraint
    equivalenceAttrs = newEquivalenceAttrs
  }

  def isInitialized(): Boolean =
    rawConstraint.isEmpty && equivalenceAttrs.isEmpty

  def apply(attr: Attribute): Int = rawConstraint(attr)
}

object ShareConstraint extends PredicateHelper {

  /** Build [[ShareConstraint]] from a raw constraint of attributes. */
  def fromRawConstraint(rawConstraint: AttributeMap[Int]): ShareConstraint = {
    val equivilanceAttrs =
      EquiAttributes.fromAttributes(rawConstraint.keys.toSeq)
    new ShareConstraint(rawConstraint, equivilanceAttrs)
  }

  /** Build [[ShareConstraint]] from a raw constraint of attributes and expression that contains equal to conditions. */
  def fromRawConstraintAndCond(
      rawConstraint: AttributeMap[Int],
      equalCondition: Expression
  ): ShareConstraint = {

    val equivilanceAttrs =
      EquiAttributes.fromCondition(rawConstraint.keys.toSeq, equalCondition)

    // Propagate constraint among equivalence attributes.
    val newRawConstraint = AttributeMap(
      equivilanceAttrs.repAttr2Attr.toSeq
        .filter { case (_, equiAttrs) =>
          rawConstraint.find { case (key, value) =>
            equiAttrs.contains(key)
          }.nonEmpty
        }
        .flatMap { case (_, equiAttrs) => equiAttrs.map(g => (g, 1)) }
    )

    new ShareConstraint(newRawConstraint, equivilanceAttrs)
  }
}

/** The context of share constraint. */
case class ShareConstraintContext(shareConstraint: ShareConstraint)
    extends SharedContext[ShareConstraint](shareConstraint) {}
