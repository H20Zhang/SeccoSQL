package org.apache.spark.secco.optimization.statsEstimation.exact

import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.optimization.plan.MultiwayJoin
import org.apache.spark.secco.optimization.statsEstimation.{
  Estimation,
  Statistics
}

import scala.collection.mutable

object ExactLogicalPlanEstimation extends Estimation[LogicalPlan] {

  private var cardinalityMap: mutable.HashMap[String, Long] = mutable.HashMap()

  def setCardinality(
      relationalString: String,
      cardinality: Long,
      isReplace: Boolean = true
  ): Unit = {
    if (cardinalityMap.contains(relationalString) && !isReplace) {
      throw new Exception(
        s"cardinality of ${relationalString} has already been set with value:${cardinalityMap(relationalString)}, new value:${cardinality} won't be updated."
      )
    }

    cardinalityMap(relationalString) = cardinality

  }

  override def estimate(plan: LogicalPlan): Option[Statistics] = {
    cardinalityMap
      .get(plan.relationalString)
      .map(cardinality =>
        Statistics(
          BigInt(cardinality)
        )
      )
  }

  def clear(): Unit = {
    cardinalityMap = mutable.HashMap()
  }

}
