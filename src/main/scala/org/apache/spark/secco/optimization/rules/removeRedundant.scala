package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.optimization.{LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan.{Filter, Project, Union}

/*---------------------------------------------------------------------------------------------------------------------
 *  This file contains rules for removing redundant logical operators.
 *
 *  0. MergeUnion: merge consecutive union into one union.
 *  1. MergeProjection: merge consecutive projection into one projection.
 *  2. MergeSelection: merge consecutive selection into one selection.
 *  3. RemoveRedundantSelection: remove redundant selection predicates.
 *  4. RemoveRedundantProjection: remove redundant projection functions.
 */

/** A rule that merges consecutive union into one union */
object MergeUnion extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case u @ Union(children, mode) => {
        val unionInputs = children.flatMap { f =>
          f match {
            case j @ Union(grandsons, _) => grandsons
            case _                       => f :: Nil
          }
        }
        Union(unionInputs, mode)
      }
    }
}

/** A rule that merges consecutive projection into one projection */
//TODO: we should have an analyzer to check the validity of the expression.
object MergeProjection extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case Project(
            Project(in, _, mode1, _),
            projectList1,
            mode2,
            _
          ) => {
        Project(in, projectList1, mode2)
      }
    }
}

/** A rule that merges consecutive selection into one selection */
object MergeSelection extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case s @ Filter(Filter(child, expr1, mode1, _), expr2, mode2, _) => {
        Filter(child, (expr1 ++ expr2).distinct, mode1)
      }
    }
}

/** A rule that removes redundant selections,
  * where the output of the child is not presented in the predicates
  */
object RemoveRedundantSelection extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case s @ Filter(child, expr, mode, _) => {

        val childOutput = child.outputOld
        val filteredSelectionList = expr.filter(p =>
          childOutput.contains(p._1) && childOutput.contains(p._3)
        )

        if (filteredSelectionList.isEmpty) {
          child
        } else {
          Filter(child, filteredSelectionList, mode)
        }
      }
    }
}

/** A rule that removes redundant projection,
  * where the output of the child is the same as the projection list
  */
object RemoveRedundantProjection extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case p @ Project(child, expr, mode, _) => {
        if (expr.toSet == child.outputOld.toSet) {
          child
        } else {
          Project(child, expr.intersect(child.outputOld), mode)
        }
      }
    }
}
