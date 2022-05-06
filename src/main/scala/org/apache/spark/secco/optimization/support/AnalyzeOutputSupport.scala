package org.apache.spark.secco.optimization.support

import org.apache.spark.secco.catalog.TableIdentifier
import org.apache.spark.secco.expression.{
  Attribute,
  EqualTo,
  Literal,
  PredicateHelper
}
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.plan._

/** A trait to add heuristic to analyze the output of [[LogicalPlan]].
  */
trait AnalyzeOutputSupport extends PredicateHelper {

  /** Check if an logical plan is static */
  def isStatic(
      plan: LogicalPlan,
      deltaTableIdentifiers: Seq[TableIdentifier]
  ): Boolean = {

    plan
      .collect {
        case s: Relation if deltaTableIdentifiers.contains(s.tableIdentifier) =>
          Seq(s)
        case l: PairThenCompute =>
          l.localPlan.collect {
            case s: Relation
                if deltaTableIdentifiers.contains(s.tableIdentifier) =>
              s
          }
      }
      .flatMap(f => f)
      .isEmpty
  }

  /** Check if output is small enough for materialization. */
  def isMaterializable(plan: LogicalPlan): Boolean = {
    plan match {
      case s: Filter          => isPredicatesStrict(s) || isMaterializable(s.child)
      case p: Project         => isMaterializable(p.child)
      case d: Distinct        => isDistinctiveOutputSmall(d)
      case a: Aggregate       => isDistinctiveOutputSmall(a)
      case u: Union           => u.children.forall(isMaterializable)
      case d: Except          => isMaterializable(d.left) && isMaterializable(d.right)
      case p: Partition       => isMaterializable(p.child)
      case l: PairThenCompute => isMaterializable(l.localPlan)
      case c: Cache           => isMaterializable(c.child)
      case bj: BinaryJoin if isJoinOutputSmall(bj) =>
        true
      case mj: MultiwayJoin if isJoinOutputSmall(mj)         => false
      case c: CartesianProduct                               => false
      case plan: LogicalPlan if isInputOperator(plan)        => true
      case plan: LogicalPlan if plan.mode == ExecMode.Atomic => true
      case _                                                 => false
    }
  }

  /** Check if the join's output is small. */
  def isJoinOutputSmall(plan: LogicalPlan): Boolean = {
    plan match {
      case bj: BinaryJoin =>
        bj.property.contains(PrimaryKeyForeignKeyJoinConstraintProperty)
      case mj: MultiwayJoin => false
    }
  }

  /** Check if the operator is an input operator. */
  def isInputOperator(plan: LogicalPlan): Boolean =
    plan.isInstanceOf[LocalRows] || plan.isInstanceOf[RDDRows] || plan
      .isInstanceOf[PartitionedRDDRows]

  /** Check if the predicates of the operator is strict, which lead to small outputs. */
  def isPredicatesStrict(plan: LogicalPlan) = {
    plan match {
      case f @ Filter(child, condition, _) =>
        val conditions = splitConjunctivePredicates(condition)
        conditions.exists { predicate =>
          predicate match {
            case EqualTo(a: Attribute, l: Literal) => true
            case _                                 => false
          }
        }
      case _ => false
    }
  }

  /** Check if operator's output size is small. */
  def isDistinctiveOutputSmall(plan: LogicalPlan) = {

    val attrs = plan match {
      case a: Aggregate => a.groupingExpressions.map(_.toAttribute)
      case d: Distinct  => d.output
    }

    plan
      .find(childPlan =>
        if (isInputOperator(childPlan)) {
          AttributeSet(attrs).subsetOf(childPlan.outputSet)
        } else if (childPlan.isInstanceOf[PlaceHolder]) {
          AttributeSet(attrs).subsetOf(childPlan.outputSet)
        } else {
          false
        }
      )
      .nonEmpty
  }
}
