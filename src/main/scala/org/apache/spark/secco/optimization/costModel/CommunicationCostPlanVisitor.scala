package org.apache.spark.secco.optimization.costModel

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.execution.plan.communication.utils.EnumShareComputer
import org.apache.spark.secco.optimization.{LogicalPlan, LogicalPlanVisitor}
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  Filter,
  MultiwayNaturalJoin,
  LocalStage,
  Partition,
  Project,
  Union
}
import org.apache.spark.secco.optimization.statsEstimation.{
  Statistics,
  StatsPlanVisitor
}
import org.apache.spark.secco.optimization.statsEstimation.histogram.HistogramBasedStatsPlanVisitor

/** An [[LogicalPlanVisitor]] that estimate the communication cost of the plan.
  */
object CommunicationCostPlanVisitor extends LogicalPlanVisitor[Double] {

  /** Falls back to the estimation computed by [[HistogramBasedStatsPlanVisitor]]. */
  private def fallback(p: LogicalPlan): Double = {
    if (p.children.size != 0) {
      p.children
        .map(child => StatsPlanVisitor.visit(child).rowCount.toDouble)
        .sum
    } else {
      0
    }
  }

  override def default(p: LogicalPlan): Double = fallback(p)

  override def visitAggregate(p: Aggregate): Double = fallback(p)

  override def visitFilter(p: Filter): Double = 0

  override def visitJoin(p: MultiwayNaturalJoin): Double = fallback(p)

  override def visitProject(p: Project): Double = fallback(p)

  override def visitUnion(p: Union): Double = fallback(p)

  override def visitPartition(p: Partition): Double = fallback(p)

  override def visitLocalStage(p: LocalStage): Double = {
    LocalStageCostModel.communicationCost(p)
  }
}
