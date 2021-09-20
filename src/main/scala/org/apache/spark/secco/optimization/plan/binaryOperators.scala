package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.expression.Expression
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.plan.JoinType.JoinType

abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override final def children: Seq[LogicalPlan] = Seq(left, right)
}

/**
  * An operator that computes the difference between left child and right child
  * @param left left child logical plan
  * @param right right child logical plan
  * @param mode execution mode
  */
case class Diff(left: LogicalPlan, right: LogicalPlan, mode: ExecMode)
    extends BinaryNode {

  /** The output attributes */
  override def outputOld: Seq[String] = left.outputOld
}

/**
  * An operator that performs primary key foreign key join between [[left]] and [[right]]
  * @param left left child logical plan
  * @param right right child logical plan
  * @param joinType [[JoinType.PKFK]]
  * @param mode execution mode
  */
case class PKFKJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType = JoinType.PKFK,
    mode: ExecMode,
    condition: Option[Expression] = None
) extends BinaryNode {

  override def primaryKeys: Seq[String] = {
    if (
      (left.outputOld
        .intersect(right.outputOld)
        .toSet == left.primaryKeys.toSet) && (left.primaryKeys.toSet == right.primaryKeys.toSet)
    ) {
      left.primaryKeys
    } else {
      Seq()
    }
  }

  /** The output attributes */
  override def outputOld: Seq[String] = children.flatMap(_.outputOld).distinct

  override def output: Seq[Attribute] =
    children.flatMap(_.output)

  override def relationalSymbol: String = s"⧓"
}

case class UnionByUpdate(
    left: LogicalPlan,
    right: LogicalPlan,
    keys: Seq[String],
    projectionAdded: Boolean
) extends BinaryNode {
  def duplicateResolved = left.outputSet.intersect(right.outputSet).isEmpty

  override def output = left.output ++ right.output

  override lazy val resolved = expressions.forall(
    _.resolved
  ) && childrenResolved && duplicateResolved && projectionAdded

  override def mode: ExecMode = ExecMode.Atomic

  override def outputOld: Seq[String] = Seq()
}
