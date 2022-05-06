package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.analysis.rules.CleanupAliases
import org.apache.spark.secco.expression.{
  Alias,
  And,
  Attribute,
  Literal,
  NamedExpression,
  PredicateHelper
}
import org.apache.spark.secco.expression.utils.{AttributeMap, ExpressionSet}
import org.apache.spark.secco.optimization.{LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  Distinct,
  Filter,
  Limit,
  Project,
  RootNode,
  Union
}
import org.apache.spark.secco.types.BooleanType
import shapeless.syntax.std.tuple.productTupleOps

import scala.collection.mutable

/*
 *  This file contains rules for removing redundant logical operators.
 *
 *  1. MergeUnion: merge consecutive union into one union.
 *  2. MergeProjection: merge consecutive projection into one projection.
 *  3. MergeSelection: merge consecutive selection into one selection.
 *  4. MergeLimit: merge consecutive limit.
 *  5. RemoveRedundantSelection: remove redundant selection predicates.
 *  6. RemoveRedundantProjection: remove redundant projection functions.
 *  7. RemoveRedundantSort: remove redundant sort operator. (TBD)
 */

/** A rule that merges consecutive union into one union */
object MergeUnion extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case u: Union => flattenUnion(u)
  }

  private def flattenUnion(union: Union): Union = {

    //traverse the children of union with a stack.
    val stack = mutable.Stack[LogicalPlan](union)
    val flattened = mutable.ArrayBuffer.empty[LogicalPlan]
    while (stack.nonEmpty) {
      stack.pop() match {
        case Union(children, _) =>
          stack.pushAll(children.reverse)
        case child =>
          flattened += child
      }
    }
    Union(flattened)
  }
}

/** A rule that merges consecutive [[Project]] into one. */
object MergeProjection extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p1 @ Project(p2: Project, _, _) =>
      if (
        haveCommonNonDeterministicOutput(p1.projectionList, p2.projectionList)
      ) {
        p1
      } else {
        p2.copy(projectionList =
          buildCleanedProjectList(p1.projectionList, p2.projectionList)
        )
      }
    case p @ Project(agg: Aggregate, _, _) =>
      if (
        haveCommonNonDeterministicOutput(
          p.projectionList,
          agg.aggregateExpressions
        )
      ) {
        p
      } else {
        agg.copy(aggregateExpressions =
          buildCleanedProjectList(p.projectionList, agg.aggregateExpressions)
        )
      }
  }

  private def collectAliases(
      projectList: Seq[NamedExpression]
  ): AttributeMap[Alias] = {
    AttributeMap(projectList.collect { case a: Alias =>
      a.toAttribute -> a
    })
  }

  private def haveCommonNonDeterministicOutput(
      upper: Seq[NamedExpression],
      lower: Seq[NamedExpression]
  ): Boolean = {
    // Create a map of Aliases to their values from the lower projection.
    // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    val aliases = collectAliases(lower)

    // Collapse upper and lower Projects if and only if their overlapped expressions are all
    // deterministic.
    upper.exists(_.collect {
      case a: Attribute if aliases.contains(a) => aliases(a).child
    }.exists(!_.deterministic))
  }

  private def buildCleanedProjectList(
      upper: Seq[NamedExpression],
      lower: Seq[NamedExpression]
  ): Seq[NamedExpression] = {
    // Create a map of Aliases to their values from the lower projection.
    // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    val aliases = collectAliases(lower)

    // Substitute any attributes that are produced by the lower projection, so that we safely
    // eliminate it.
    // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
    // Use transformUp to prevent infinite recursion.
    val rewrittenUpper = upper.map(_.transformUp { case a: Attribute =>
      aliases.getOrElse(a, a)
    })
    // collapse upper and lower Projects may introduce unnecessary Aliases, trim them here.
    rewrittenUpper.map { p =>
      CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
    }
  }
}

/** A rule that merges consecutive selection into one selection */
object MergeSelection extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // The query execution/optimization does not guarantee the expressions are evaluated in order.
    // We only can combine them if and only if both are deterministic.
    case Filter(nf @ Filter(grandChild, nc, childMode), fc, mode)
        if fc.deterministic && nc.deterministic && mode == childMode =>
      (ExpressionSet(splitConjunctivePredicates(fc)) --
        ExpressionSet(splitConjunctivePredicates(nc))).reduceOption(And) match {
        case Some(ac) =>
          Filter(grandChild, And(nc, ac), mode)
        case None =>
          nf
      }
  }
}

/** A rule that merges consecutive limit into one. */
object MergeLimit extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Limit(Limit(grandChild, tuple2, childMode), tuple1, mode)
        if childMode == mode =>
      Limit(grandChild, tuple1, mode)
  }
}

/** A rule that removes redundant selections predicates that can evaluated trivially.
  *
  * 1. by eliding the filter for cases where it will always evaluate to `true`.
  * 2. by substituting a dummy empty relation when the filter will always evaluate to `false`.
  * 3. by eliminating the always-true conditions given the constraints on the child's output.
  */
object RemoveRedundantSelection extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(child, Literal(true, BooleanType), _) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
//    case Filter(child, Literal(null, _), _) =>
//      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
//    case Filter(child, Literal(false, BooleanType), _) =>
//      LocalRelation(child.output, data = Seq.empty, isStreaming = plan.isStreaming)
    // If any deterministic condition is guaranteed to be true given the constraints on the child's
    // output, remove the condition
//    case f @ Filter(fc, p: LogicalPlan) =>
//      val (prunedPredicates, remainingPredicates) =
//        splitConjunctivePredicates(fc).partition { cond =>
//          cond.deterministic && p.constraints.contains(cond)
//        }
//      if (prunedPredicates.isEmpty) {
//        f
//      } else if (remainingPredicates.isEmpty) {
//        p
//      } else {
//        val newCond = remainingPredicates.reduce(And)
//        Filter(newCond, p)
//      }
  }
}

/** A rule that removes redundant projection,
  * where the output of the child is the same as the projection list
  */
object RemoveRedundantProjection extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ Project(child, _, _) if p.output == child.output => child
  }
}
