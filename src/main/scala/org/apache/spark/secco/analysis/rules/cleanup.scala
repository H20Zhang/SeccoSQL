package org.apache.spark.secco.analysis.rules

import org.apache.spark.secco.expression.{Alias, Expression, NamedExpression}
import org.apache.spark.secco.optimization.plan.{Aggregate, Project}
import org.apache.spark.secco.optimization.{LogicalPlan, Rule}
import org.apache.spark.sql.catalyst.analysis.MultiAlias

/** Cleans up unnecessary Aliases inside the plan. Basically we only need Alias as a top level
  * expression in Project(project list) or Aggregate(aggregate expressions) or
  * Window(window expressions). Notice that if an expression has other expression parameters which
  * are not in its `children`, e.g. `RuntimeReplaceable`, the transformation for Aliases in this
  * rule can't work for those parameters.
  */
object CleanupAliases extends Rule[LogicalPlan] {
  private def trimAliases(e: Expression): Expression = {
    e.transformDown {
      case Alias(child, _)      => child
      case MultiAlias(child, _) => child
    }
  }

  def trimNonTopLevelAliases(e: Expression): Expression = e match {
    case a: Alias =>
      a.copy(child = trimAliases(a.child))(
        exprId = a.exprId,
        qualifier = a.qualifier
      )
    case a: MultiAlias =>
      a.copy(child = trimAliases(a.child))
    case other => trimAliases(other)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case Project(projectList, child) =>
      val cleanedProjectList =
        projectList.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Project(cleanedProjectList, child)

    case Aggregate(grouping, aggs, child) =>
      val cleanedAggs =
        aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
      Aggregate(grouping.map(trimAliases), cleanedAggs, child)

//    case Window(windowExprs, partitionSpec, orderSpec, child) =>
//      val cleanedWindowExprs =
//        windowExprs.map(e => trimNonTopLevelAliases(e).asInstanceOf[NamedExpression])
//      Window(cleanedWindowExprs, partitionSpec.map(trimAliases),
//        orderSpec.map(trimAliases(_).asInstanceOf[SortOrder]), child)
//
//    // Operators that operate on objects should only have expressions from encoders, which should
//    // never have extra aliases.
//    case o: ObjectConsumer => o
//    case o: ObjectProducer => o
//    case a: AppendColumns => a

    case other =>
      other transformExpressionsDown { case Alias(child, _) =>
        child
      }
  }
}
