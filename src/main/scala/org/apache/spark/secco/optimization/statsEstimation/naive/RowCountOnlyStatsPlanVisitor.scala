package org.apache.spark.secco.optimization.statsEstimation.naive

import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.optimization.statsEstimation.Statistics
import org.apache.spark.secco.optimization.{LogicalPlan, LogicalPlanVisitor}

/** An [[LogicalPlanVisitor]] that computes a single dimension for plan stats: size in bytes.
  */
object RowCountOnlyStatsPlanVisitor extends LogicalPlanVisitor[Statistics] {

  /** A default, commonly used estimation for unary nodes. We assume the input row number is the
    * same as the output row number, and compute sizes based on the column types.
    */
  private def visitUnaryNode(p: UnaryNode): Statistics = {
    // There should be some overhead in Row object, the size should not be zero when there is
    // no columns, this help to prevent divide-by-zero error.
    // Assume there will be the same number of rows as child has.
    var rowCount = p.child.stats.rowCount
    if (rowCount == 0) {
      // sizeInBytes can't be zero, or sizeInBytes of BinaryNode will also be zero
      // (product of children).
      rowCount = 1
    }

    // Don't propagate rowCount and attributeStats, since they are not estimated here.
    Statistics(rowCount = rowCount)
  }

  /** For leaf nodes, use its `computeStats`. For other nodes, we assume the row count is the
    * product of all of the children's `computeStats`.
    */
  override def default(p: LogicalPlan): Statistics =
    p match {
      case p: LeafNode => p.computeStats()
      case _: LogicalPlan =>
        Statistics(rowCount = p.children.map(_.stats.rowCount).product)
    }

  override def visitAggregate(p: Aggregate): Statistics = {
    if (p.groupingExpressions.isEmpty) {
      Statistics(rowCount = 1)
    } else {
      visitUnaryNode(p)
    }
  }

  override def visitFilter(p: Filter): Statistics = visitUnaryNode(p)

  override def visitJoin(p: Join): Statistics = {
    val stats = default(p.asInstanceOf[LogicalPlan])
    stats
  }

  override def visitProject(p: Project): Statistics = visitUnaryNode(p)

  override def visitUnion(p: Union): Statistics = {
    Statistics(rowCount = p.children.map(_.stats.rowCount).sum)
  }

  override def visitPartition(p: Partition): Statistics = {
    p.child.stats
  }

  override def visitLocalStage(p: LocalStage): Statistics = {

    val concreteRootPlan = p.unboxedPlan()

    concreteRootPlan.stats
  }
}
