package org.apache.spark.secco.optimization.support

import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.optimization.costModel.CommunicationCostPlanVisitor

/**
  * A trait that adds cost model for estimating communication and computation cost to [[LogicalPlan]]
  */
trait CostModelSupport {
  self: LogicalPlan =>

  /**
    * Computation of this operator
    * @return computation cost in double
    */
  def computationCost(): Double = ???

  /**
    * Computation of all operator contains in the operator tree
    * @return computation cost in double
    */
  def allComputationCost(): Double = ???

  /**
    * Communication cost of this operator
    * @return communication cost in double
    */
  def communicationCost(): Double = CommunicationCostPlanVisitor.visit(this)

  /**
    * Communication of all operator contains in the operator tree
    * @return communication cost in double
    */
  def allCommunicationCost(): Double = {
    map(child => CommunicationCostPlanVisitor.visit(child)).sum
  }

}
