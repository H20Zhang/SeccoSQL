//package org.apache.spark.dolphin.expression
//
//import org.apache.spark.dolphin.optimization.LogicalPlan
//import org.apache.spark.dolphin.trees.QueryPlan
//import org.apache.spark.dolphin.types.DataType
//
///**
//  * An interface for expressions that contain a [[QueryPlan]].
//  */
//abstract class PlanExpression[T <: QueryPlan[_]] extends Expression {
//
//  /** The id of the subquery expression. */
//  def exprId: ExprId
//
//  /** The plan being wrapped in the query. */
//  def plan: T
//
//  /** Updates the expression with a new plan. */
//  def withNewPlan(plan: T): PlanExpression[T]
//
//  protected def conditionString: String = children.mkString("[", " && ", "]")
//}
//
///**
//  * A base interface for expressions that contain a [[LogicalPlan]].
//  */
//abstract class SubqueryExpression(
//    plan: LogicalPlan,
//    children: Seq[Expression],
//    exprId: ExprId
//) extends PlanExpression[LogicalPlan] {
//  override lazy val resolved: Boolean = childrenResolved
//  override def withNewPlan(plan: LogicalPlan): SubqueryExpression
//}
//
///**
//  * A subquery that will return only one row and one column. This will be converted into a physical
//  * scalar subquery during planning.
//  *
//  * Note: `exprId` is used to have a unique name in explain string output.
//  */
//case class ScalarSubquery(
//    plan: LogicalPlan,
//    children: Seq[Expression] = Seq.empty,
//    exprId: ExprId = NamedExpression.newExprId
//) extends SubqueryExpression(plan, children, exprId)
//    with Unevaluable {
//  override def dataType: DataType = plan.output.head.dataType
//  override def nullable: Boolean = true
//  override def withNewPlan(plan: LogicalPlan): ScalarSubquery =
//    copy(plan = plan)
//  override def toString: String =
//    s"scalar-subquery#${exprId.id} $conditionString"
//}
//
///**
//  * A [[ListQuery]] expression defines the query which we want to search in an IN subquery
//  * expression. It should and can only be used in conjunction with an IN expression.
//  *
//  * For example (SQL):
//  * {{{
//  *   SELECT  *
//  *   FROM    a
//  *   WHERE   a.id IN (SELECT  id
//  *                    FROM    b)
//  * }}}
//  */
//case class ListQuery(
//    plan: LogicalPlan,
//    children: Seq[Expression] = Seq.empty,
//    exprId: ExprId = NamedExpression.newExprId
//) extends SubqueryExpression(plan, children, exprId)
//    with Unevaluable {
//  override def dataType: DataType = plan.output.head.dataType
//
//  override def nullable: Boolean = false
//
//  override def withNewPlan(plan: LogicalPlan): ListQuery = copy(plan = plan)
//
//  override def toString: String = s"list#${exprId.id} $conditionString"
//}
//
///**
//  * The [[Exists]] expression checks if a row exists in a subquery given some correlated condition.
//  *
//  * For example (SQL):
//  * {{{
//  *   SELECT  *
//  *   FROM    a
//  *   WHERE   EXISTS (SELECT  *
//  *                   FROM    b
//  *                   WHERE   b.id = a.id)
//  * }}}
//  */
//case class Exists(
//    plan: LogicalPlan,
//    children: Seq[Expression] = Seq.empty,
//    exprId: ExprId = NamedExpression.newExprId
//) extends SubqueryExpression(plan, children, exprId)
//    with Predicate
//    with Unevaluable {
//  override def nullable: Boolean = false
//
//  override def withNewPlan(plan: LogicalPlan): Exists = copy(plan = plan)
//
//  override def toString: String = s"exists#${exprId.id} $conditionString"
//}
