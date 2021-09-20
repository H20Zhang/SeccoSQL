package org.apache.spark.secco.optimization.costModel

/** A trait that represents the cost model of a plan */
trait CostModel[T] {

  /** Estimate the communication cost of the plan */
  def communicationCost(plan: T): Double

  /** Estimate the computation cost of the plan */
  def computationCost(plan: T): Double

}
