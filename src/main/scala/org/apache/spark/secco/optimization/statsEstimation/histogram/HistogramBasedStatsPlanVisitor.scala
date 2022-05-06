package org.apache.spark.secco.optimization.statsEstimation.histogram

import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.optimization.statsEstimation.Statistics
import org.apache.spark.secco.optimization.statsEstimation.naive.RowCountOnlyStatsPlanVisitor
import org.apache.spark.secco.optimization.{LogicalPlan, LogicalPlanVisitor}
import org.apache.spark.secco.util.misc.LogAble

object HistogramBasedStatsPlanVisitor
    extends LogicalPlanVisitor[Statistics]
    with LogAble {

  /** Falls back to the estimation computed by [[RowCountOnlyStatsPlanVisitor]]. */
  private def fallback(p: LogicalPlan): Statistics =
    RowCountOnlyStatsPlanVisitor.visit(p)

  override def default(p: LogicalPlan): Statistics = fallback(p)

  override def visitAggregate(p: Aggregate): Statistics = {
    HistogramAggregateEstimation.estimate(p).getOrElse(fallback(p))
  }

  override def visitFilter(p: Filter): Statistics = {
    HistogramFilterEstimation.estimate(p).getOrElse(fallback(p))
  }

  override def visitJoin(p: Join): Statistics = {
    val stats = HistogramJoinEstimation
      .estimate(p)
      .getOrElse(fallback(p.asInstanceOf[LogicalPlan]))

    logTrace(
      s"rowCount:${stats.rowCount.toString()} of operator:\n${p.toString}"
    )

    stats
  }

  override def visitProject(p: Project): Statistics = {
    HistogramProjectEstimation.estimate(p).getOrElse(fallback(p))
  }

  override def visitUnion(p: Union): Statistics = fallback(p)

  override def visitPartition(p: Partition): Statistics = visit(p.child)

  override def visitLocalStage(p: PairThenCompute): Statistics =
    visit(p.unboxedPlan())
}
