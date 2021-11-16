package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.expression.{
  Attribute,
  EqualTo,
  Expression,
  PredicateHelper
}
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.types.BooleanType

/* ---------------------------------------------------------------------------------------------------------------------
 * This file contains logical plans with two children.
 *
 * 0.  BinaryNode: base class of logical plan with two children.
 * 1.  Intersection: intersect the results of left child and right child.
 * 2.  Diff: perform difference between results of left child and right child.
 * 3.  BinaryJoin: perform join between left child and right child.
 * 4.  PKFKJoin: perform primary-key foreign-key join between left child and right child.
 * 5.  UnionByUpdate: update results of left child by results of right child.
 *
 * ---------------------------------------------------------------------------------------------------------------------
 */

/** A [[LogicalPlan]] with two children. */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override final def children: Seq[LogicalPlan] = Seq(left, right)
}

/** A [[LogicalPlan]] that computes the intersection between tuples of left child and tuples of right child.
  * @param left left child [[LogicalPlan]]
  * @param right right child [[LogicalPlan]]
  * @param mode execution mode
  */
case class Intersection(
    left: LogicalPlan,
    right: LogicalPlan,
    mode: ExecMode = ExecMode.Coupled
) extends BinaryNode {

  override def primaryKey: Seq[Attribute] = left.primaryKey

  override def output: Seq[Attribute] = left.output

  override def relationalSymbol: String = s"⋂"
}

/** A [[LogicalPlan]] that computes the difference between tuples of left child and tuples of right child.
  * @param left left child [[LogicalPlan]]
  * @param right right child [[LogicalPlan]]
  * @param mode execution mode
  */
case class Except(
    left: LogicalPlan,
    right: LogicalPlan,
    mode: ExecMode = ExecMode.Coupled
) extends BinaryNode {

  override def primaryKey: Seq[Attribute] = left.primaryKey

  override def output: Seq[Attribute] = left.output

  override def relationalSymbol: String = s"-"
}

/** A [[LogicalPlan]] that perform cartesian product between tuples of left child and tuples of right child
  * @param left left child [[LogicalPlan]]
  * @param right right child [[LogicalPlan]]
  * @param mode execution mode
  */
case class CartesianProduct(
    left: LogicalPlan,
    right: LogicalPlan,
    mode: ExecMode = ExecMode.Coupled
) extends BinaryNode {

  override def primaryKey: Seq[Attribute] = left.primaryKey ++ right.primaryKey

  override def output: Seq[Attribute] = left.output ++ right.output

  override def relationalSymbol: String = s"⨉"
}

/** A [[LogicalPlan]] that performs binary join between tuples of left child and tuple of right child.
  * @param left left child [[LogicalPlan]]
  * @param right right child [[LogicalPlan]]
  * @param joinType types of join, i.e., right-outer join, left-outer join, full-outer join, join
  * @param mode execution mode
  */
case class BinaryJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression],
    property: Set[JoinProperty] = Set(),
    mode: ExecMode = ExecMode.Coupled
) extends BinaryNode
    with PredicateHelper
    with Join {

  override def primaryKey: Seq[Attribute] = {

    condition match {
      case None => Seq()
      case Some(expr) =>
        val conditions = splitConjunctivePredicates(expr)
        val newPrimaryKey = conditions.collect {
          case EqualTo(a: Attribute, b: Attribute)
              if (left.primaryKeySet.contains(a) && right.primaryKeySet
                .contains(b)) || (left.primaryKeySet.contains(
                b
              ) && right.primaryKeySet.contains(a)) =>
            Seq(a, b)
        }.flatten
        newPrimaryKey
    }
  }

  def duplicateResolved: Boolean =
    left.outputSet.intersect(right.outputSet).isEmpty

  // Joins are only resolved if they don't introduce ambiguous expression ids.
  // NaturalJoin should be ready for resolution only if everything else is resolved here
  lazy val resolvedExceptNatural: Boolean = {
    childrenResolved &&
    expressions.forall(_.resolved) &&
    duplicateResolved &&
    condition.forall(_.dataType == BooleanType)
  }

  // if not a natural join, use `resolvedExceptNatural`. if it is a natural join or
  // using join, we still need to eliminate natural or using before we mark it resolved.
  override lazy val resolved: Boolean = joinType match {
    case NaturalJoin(_)  => false
    case UsingJoin(_, _) => false
    case _               => resolvedExceptNatural
  }

  override def output: Seq[Attribute] =
    children.flatMap(_.output)

  override def relationalSymbol: String = s"⧓"
}

/** A [[LogicalPlan]] that updates tuples of the left by tuples of the right.
  * @param left left child [[LogicalPlan]]
  * @param right right child [[LogicalPlan]]
  * @param keys key attributes
  * @param projectionAdded ???
  */
case class UnionByUpdate(
    left: LogicalPlan,
    right: LogicalPlan,
    keys: Seq[Attribute],
    projectionAdded: Boolean
) extends BinaryNode {
  def duplicateResolved = left.outputSet.intersect(right.outputSet).isEmpty

  override def output = left.output ++ right.output

  override lazy val resolved = expressions.forall(
    _.resolved
  ) && childrenResolved && duplicateResolved && projectionAdded

  override def mode: ExecMode = ExecMode.Atomic

}
