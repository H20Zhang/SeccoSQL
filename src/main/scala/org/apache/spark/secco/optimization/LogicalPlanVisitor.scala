package org.apache.spark.secco.optimization

import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  Filter,
  Join,
  PairThenCompute,
  MultiwayJoin,
  Partition,
  Project,
  Union
}

/** A visitor pattern for traversing a [[LogicalPlan]] tree and computing some properties.
  */
trait LogicalPlanVisitor[T] {
  def visit(p: LogicalPlan): T =
    p match {
      case p: Aggregate       => visitAggregate(p)
      case p: Filter          => visitFilter(p)
      case p: Join            => visitJoin(p)
      case p: Project         => visitProject(p)
      case p: Union           => visitUnion(p)
      case p: Partition       => visitPartition(p)
      case p: PairThenCompute => visitLocalStage(p)
      case p: LogicalPlan     => default(p)

    }

  def default(p: LogicalPlan): T

  def visitAggregate(p: Aggregate): T

  def visitFilter(p: Filter): T

  def visitJoin(p: Join): T

  def visitProject(p: Project): T

  def visitUnion(p: Union): T

  def visitPartition(p: Partition): T

  def visitLocalStage(p: PairThenCompute): T

}
