package org.apache.spark.secco.optimization.statsEstimation

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.optimization.{LogicalPlan, LogicalPlanVisitor}
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  Filter,
  Join,
  LocalStage,
  Partition,
  Project,
  Union
}
import org.apache.spark.secco.optimization.statsEstimation.exact.ExactStatsPlanVisitor
import org.apache.spark.secco.optimization.statsEstimation.histogram.HistogramBasedStatsPlanVisitor
import org.apache.spark.secco.optimization.statsEstimation.naive.RowCountOnlyStatsPlanVisitor
import org.apache.spark.secco.util.misc.LogAble

object StatsPlanVisitor extends LogicalPlanVisitor[Statistics] with LogAble {

  val defaultStatsEstimator =
    SeccoSession.currentSession.sessionState.conf.estimator match {
      case "Naive"     => RowCountOnlyStatsPlanVisitor
      case "Exact"     => ExactStatsPlanVisitor
      case "Histogram" => HistogramBasedStatsPlanVisitor
      case est: String =>
        throw new Exception(s"estimator:${est} is not supported.")
    }

  override def default(p: LogicalPlan): Statistics =
    defaultStatsEstimator.default(p)

  override def visitAggregate(p: Aggregate): Statistics =
    defaultStatsEstimator.visitAggregate(p)

  override def visitFilter(p: Filter): Statistics =
    defaultStatsEstimator.visitFilter(p)

  override def visitJoin(p: Join): Statistics =
    defaultStatsEstimator.visitJoin(p)

  override def visitProject(p: Project): Statistics =
    defaultStatsEstimator.visitProject(p)

  override def visitUnion(p: Union): Statistics =
    defaultStatsEstimator.visitUnion(p)

  override def visitPartition(p: Partition): Statistics =
    defaultStatsEstimator.visit(p)

  override def visitLocalStage(p: LocalStage): Statistics =
    defaultStatsEstimator.visit(p)
}
