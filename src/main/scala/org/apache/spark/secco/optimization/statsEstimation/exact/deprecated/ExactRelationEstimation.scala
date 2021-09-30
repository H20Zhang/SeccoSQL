package org.apache.spark.secco.optimization.statsEstimation.exact.deprecated

import org.apache.spark.secco.optimization.plan.Relation
import org.apache.spark.secco.optimization.statsEstimation.exact.NoExactCardinalityException
import org.apache.spark.secco.optimization.statsEstimation.{
  Estimation,
  Statistics
}

object ExactRelationEstimation extends Estimation[Relation] {

  private var _defaultCardinality = 1L
  private var _cardinalityMap: Option[Map[String, Long]] =
    None

  def setCardinalityMap(cardinalityMap: Map[String, Long]): Unit = {
    assert(cardinalityMap.forall(_._2 >= 0))
    _cardinalityMap = Some(cardinalityMap)
  }

  def getCardinalityMap(): Map[String, Long] = {
    _cardinalityMap.get
  }

  def setDefaultCardinality(cardinality: Long): Unit = {
    assert(cardinality >= 0)
    _defaultCardinality = cardinality
  }

  def getDefaultCardinality(): Long = {
    _defaultCardinality
  }

  override def estimate(x: Relation): Option[Statistics] = {

    if (_cardinalityMap.isEmpty) {
      Some(
        Statistics(
          BigInt(getDefaultCardinality())
        )
      )
    } else {
      val cardinalityOpt = _cardinalityMap.get.get(x.tableIdentifier)

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
