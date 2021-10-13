package org.apache.spark.secco.optimization.support

import org.apache.spark.secco.expression.{Expression, PredicateHelper}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.plan.{
  BinaryJoin,
  Filter,
  Inner,
  InnerLike,
  JoinProperty,
  JoinType
}

/** A pattern that collects consecutive inner joins with specific join property.
  *
  *           Join
  *          /    \            ---->      (Seq(plan0, plan1, plan2), mode)
  *       Join   plan2
  *      /    \
  *   plan0    plan1
  *
  * Note that: the behavior of this pattern extractor can be customized by `setRequiredJoinProperties`.
  */
object ExtractConsecutiveInnerJoins extends PredicateHelper {

  private var _requiredJoinProperties: Set[JoinProperty] = Set()

  def requiredJoinProperties: Set[JoinProperty] = {
    _requiredJoinProperties
  }

  def setRequiredJoinProperties(joinProperties: Seq[JoinProperty]): Unit = {
    _requiredJoinProperties = joinProperties.toSet
  }

  def clearRequiredJoinProperties(): Unit = {
    _requiredJoinProperties = Set()
  }

  /** Flatten all inner joins, which are next to each other and satisfies requirements. */
  def flattenJoin(
      plan: LogicalPlan,
      requiredJoinType: JoinType = Inner,
      requiredJoinProperty: Set[JoinProperty] = Set(),
      requiredExecMode: ExecMode = ExecMode.Coupled
  ): (Seq[BinaryJoin], ExecMode) = plan match {
    case j @ BinaryJoin(left, right, joinType, cond, property, mode)
        if mode == requiredExecMode && joinType == requiredJoinType && requiredJoinProperty
          .subsetOf(property) =>
      val (lPlans, _) = flattenJoin(
        left,
        requiredJoinType,
        requiredJoinProperty,
        requiredExecMode
      )

      val (rPlans, _) = flattenJoin(
        right,
        requiredJoinType,
        requiredJoinProperty,
        requiredExecMode
      )

      (lPlans ++ rPlans :+ j, requiredExecMode)
    case _ => (Seq(), requiredExecMode)
  }

  def unapply(
      plan: LogicalPlan
  ): Option[(Seq[BinaryJoin], ExecMode)] = plan match {
    case j @ BinaryJoin(_, _, Inner, _, property, mode) =>
      Some(
        flattenJoin(j, Inner, _requiredJoinProperties, requiredExecMode = mode)
      )
    case _ => None
  }

}

/** A pattern that collects the filter and inner joins.
  *
  *          Filter
  *            |
  *        inner Join
  *          /    \            ---->      (Seq(plan0, plan1, plan2), conditions)
  *      Filter   plan2
  *        |
  *  inner join
  *      /    \
  *   plan0    plan1
  *
  * Note: This pattern currently only works for left-deep trees.
  */
object ExtractFiltersAndInnerJoins extends PredicateHelper {

  /** Flatten all inner joins, which are next to each other.
    * Return a list of logical plans to be joined with a boolean for each plan indicating if it
    * was involved in an explicit cross join. Also returns the entire list of join conditions for
    * the left-deep tree.
    */
  def flattenJoin(
      plan: LogicalPlan,
      parentJoinType: InnerLike = Inner,
      parentExecMode: ExecMode
  ): (Seq[(LogicalPlan, InnerLike)], Seq[Expression]) = plan match {
    case BinaryJoin(left, right, joinType: InnerLike, cond, property, mode)
        if mode == parentExecMode =>
      val (plans, conditions) = flattenJoin(left, joinType, mode)
      (
        plans ++ Seq((right, joinType)),
        conditions ++
          cond.toSeq.flatMap(splitConjunctivePredicates)
      )
    case Filter(
          j @ BinaryJoin(
            left,
            right,
            _: InnerLike,
            joinCondition,
            property,
            childMode
          ),
          filterCondition,
          mode
        ) if mode == parentExecMode && mode == childMode =>
      val (plans, conditions) = flattenJoin(j, parentExecMode = mode)
      (plans, conditions ++ splitConjunctivePredicates(filterCondition))

    case _ => (Seq((plan, parentJoinType)), Seq.empty)
  }

  def unapply(
      plan: LogicalPlan
  ): Option[((Seq[(LogicalPlan, InnerLike)], Seq[Expression]), ExecMode)] =
    plan match {
      case f @ Filter(
            j @ BinaryJoin(_, _, joinType: InnerLike, _, property, childMode),
            filterCondition,
            mode
          ) if mode == childMode =>
        Some((flattenJoin(f, joinType, mode), mode))
      case j @ BinaryJoin(_, _, joinType, _, property, mode) =>
        Some((flattenJoin(j, parentExecMode = mode), mode))
      case _ => None
    }
}
