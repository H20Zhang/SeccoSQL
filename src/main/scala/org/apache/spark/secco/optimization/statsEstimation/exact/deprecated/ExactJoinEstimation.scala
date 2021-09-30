package org.apache.spark.secco.optimization.statsEstimation.exact.deprecated

import org.apache.spark.secco.optimization.plan.{MultiwayNaturalJoin, Relation}
import org.apache.spark.secco.optimization.statsEstimation.exact.NoExactCardinalityException
import org.apache.spark.secco.optimization.statsEstimation.{
  Estimation,
  Statistics
}

object ExactJoinEstimation extends Estimation[MultiwayNaturalJoin] {

  private var _defaultCardinality = 1L
  private var _cardinalityMap: Option[Map[Set[String], Long]] =
    None

  def setCardinalityMap(cardinalityMap: Map[Set[String], Long]): Unit = {
    assert(cardinalityMap.forall(_._2 >= 0))
    _cardinalityMap = Some(cardinalityMap)
  }

  def getCardinalityMap(): Map[Set[String], Long] = {
    _cardinalityMap.get
  }

  def setDefaultCardinality(cardinality: Long): Unit = {
    assert(cardinality >= 0)
    _defaultCardinality = cardinality
  }

  def getDefaultCardinality(): Long = {
    _defaultCardinality
  }

  override def estimate(x: MultiwayNaturalJoin): Option[Statistics] = {

    if (_cardinalityMap.isEmpty) {
      Some(
        Statistics(
          BigInt(getDefaultCardinality())
        )
      )
    } else {
      val mergedPlan = Estimation.mergeJoin(x)
      assert(mergedPlan.isInstanceOf[MultiwayNaturalJoin])
      val mergedJoin = mergedPlan.asInstanceOf[MultiwayNaturalJoin]

      assert(
        mergedJoin.children.forall(_.isInstanceOf[Relation]),
        s"mergedJoin:${mergedJoin}"
      )
      val relationNameSet =
        mergedJoin.children
          .map(f => f.asInstanceOf[Relation].tableIdentifier)
          .toSet

      val cardinalityOpt = _cardinalityMap.get.get(relationNameSet)

      if (cardinalityOpt.isEmpty) {
        throw new NoExactCardinalityException(x)
      } else {
        val cardinality = cardinalityOpt.get
        Some(
          Statistics(
            BigInt(cardinality)
          )
        )
      }
    }

  }
}
