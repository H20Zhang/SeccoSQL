package org.apache.spark.secco.optimization.statsEstimation.exact

import org.apache.spark.secco.optimization.{LogicalPlan, LogicalPlanVisitor}
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  Filter,
  Join,
  LeafNode,
  PairThenCompute,
  MultiwayJoin,
  Partition,
  Project,
  Relation,
  Union
}
import org.apache.spark.secco.optimization.statsEstimation.Statistics
import org.apache.spark.secco.util.misc.LogAble

class NoExactCardinalityException(
    @transient val plan: LogicalPlan
) extends Exception(s"There is no exact cardinality for ${plan}")

/** An estimator that returns the exact cardinality of the estimated operator.
  *
  * Ideally, it should support two modes to returns the exact cardinality estimation.
  *
  * 1. Provided Mode: The exact cardinality is provided by external input.
  * 2. Computed Mode: The exact cardinality is computed by Secco.
  *
  * Currently, only the provided mode is supported.
  */
object ExactStatsPlanVisitor
    extends LogicalPlanVisitor[Statistics]
    with LogAble {
  override def default(p: LogicalPlan): Statistics = {

//    println(s"[debug]:${p.relationalString}")
    ExactLogicalPlanEstimation
      .estimate(p)
      .getOrElse(throw new NoExactCardinalityException(p))
  }

  //    p match {
//      case r: Relation =>
//        ExactRelationEstimation
//          .estimate(r)
//          .getOrElse(throw new NoExactCardinalityException(p))
//      case plan: LogicalPlan => throw new NoExactCardinalityException(plan)
//    }

  override def visitAggregate(p: Aggregate): Statistics = default(p)
//    ExactAggregateEstimation
//      .estimate(p)
//      .getOrElse(throw new NoExactCardinalityException(p))

  override def visitFilter(p: Filter): Statistics = default(p)
//    ExactFilterEstimation
//      .estimate(p)
//      .getOrElse(throw new NoExactCardinalityException(p))

  override def visitJoin(p: Join): Statistics = default(
    p.asInstanceOf[LogicalPlan]
  )
//    ExactJoinEstimation
//      .estimate(p)
//      .getOrElse(throw new NoExactCardinalityException(p))

  override def visitProject(p: Project): Statistics = default(p)
//    ExactProjectEstimation
//      .estimate(p)
//      .getOrElse(throw new NoExactCardinalityException(p))

  override def visitUnion(p: Union): Statistics = default(p)

  override def visitPartition(p: Partition): Statistics = visit(p.child)

  override def visitLocalStage(p: PairThenCompute): Statistics = {
    visit(p.recoupledPlan())
  }

}
