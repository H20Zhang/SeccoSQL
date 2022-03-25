package org.apache.spark.secco.execution.plan.communication

import org.apache.spark.secco.execution.plan.communication.Shares.findEquivilanceAttrs
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
  * In [[Shares]], each attribute is associated with an share number, which determines domains of hash functions on that
  * attribute.
  *
  * For example, for share {"a" -> 2, "b" -> 2}
  */
class Shares(
    rawShares: AttributeMap[Int],
    equivalenceAttrs: Array[AttributeSet]
) {

  def apply(attr: Attribute): Int = rawShares(attr)

  /** Generate coordinates for performing HyperCube Shuffle.
    * @param attrs attributes that participates in HyperCube Shuffle. Note: Equivalence attributes can only appear once.
    * @return an [[Array]] of [[Coordinate]]
    */
  def genHyperCubeCoordinates(attrs: Seq[Attribute]): Array[Coordinate] = ???

//  {
//    val attrsShareSpace = attrs
//      .filter(share.contains)
//      .map(attrID => share.get(attrID).get)
//    var i = 0
//    val attrsSize = attrsShareSpace.size
//    var buffer = new ArrayBuffer[Array[Int]]()
//
//    while (i < attrsSize) {
//      if (i == 0) {
//        buffer ++= Range(0, attrsShareSpace(i)).map(f => Array(f))
//      } else {
//        buffer = buffer.flatMap { shareVector =>
//          Range(0, attrsShareSpace(i)).map(f => shareVector :+ f)
//        }
//      }
//      i += 1
//    }
//
//    buffer.toArray
//  }

  /** Generate the partitioner for partitioning rows for HyperCube Shuffle. */
  def genTaskPartitioner(attributes: Array[Attribute]): PairPartitioner = ???

  /** Generate the sentry tuples, which consists of (sentryTuple, isSentryTuple), for HyperCube Shuffle */
  def genSentryRows(attrs: Array[Attribute]): Array[(InternalRow, Boolean)] =
    ???

//  {
//    genShareVectorsForAttrs(attrs).map(f => (f.map(_.toDouble), true))
//  }

  /** Get the sub-coordinate under the given attributes. */
  def getSubCoordinate(
      coordinate: Coordinate,
      attributes: Array[Attribute]
  ): Coordinate = ???

//  /** generate the all share vectors according to "share" and "attrs" */
//  def genShareVectorsForAttrs(attrs: Seq[String]): Array[Array[Int]] = {
//
//  }

}

object Shares extends PredicateHelper {
  def apply(rawShares: AttributeMap[Int], equalCondition: Expression): Shares =
    new Shares(rawShares, findEquivilanceAttrs(equalCondition).toArray)
}

case class SharesContext(shares: Shares) extends SharedContext[Shares](shares)

class ShareConstraint(
    rawConstraint: AttributeMap[Int],
    equivalenceAttrs: Array[AttributeSet]
) {
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

case class ShareConstraintContext(shareConstraint: ShareConstraint)
    extends SharedContext[ShareConstraint](shareConstraint)
