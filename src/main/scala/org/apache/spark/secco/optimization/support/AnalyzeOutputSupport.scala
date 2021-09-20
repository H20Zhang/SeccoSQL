package org.apache.spark.secco.optimization.support

import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.plan._

/**
  * A trait to add heuristic to analyze the output of [[LogicalPlan]].
  */
trait AnalyzeOutputSupport {

  /** check if an logical plan is static */
  def isStatic(
      plan: LogicalPlan,
      deltaTableIdentifiers: Seq[String]
  ): Boolean = {

    plan
      .collect {
        case s: Relation if deltaTableIdentifiers.contains(s.tableName) =>
          Seq(s)
        case l: LocalStage =>
          l.localPlan.collect {
            case s: Relation if deltaTableIdentifiers.contains(s.tableName) =>
              s
          }
      }
      .flatMap(f => f)
      .isEmpty
  }

  /** check if output is materializable */
  def isMaterializable(plan: LogicalPlan): Boolean = {
    plan match {
      case s: Filter                                         => true && !s.child.isInstanceOf[CartesianProduct]
      case p: Project                                        => isOutputSmall(p) || p.projectionListOld.size == 1
      case a: Aggregate                                      => isOutputSmall(a) || a.groupingListOld.size == 1
      case u: Union                                          => u.children.forall(isOutputSmall)
      case pk: PKFKJoin                                      => true
      case d: Diff                                           => isOutputSmall(d.left) && isOutputSmall(d.right)
      case j: Join                                           => false
      case p: Partition                                      => isMaterializable(p.child)
      case sc: Relation                                      => true
      case re: Rename                                        => isMaterializable(re.child)
      case l: LocalStage                                     => isMaterializable(l.localPlan)
      case c: Cache                                          => isMaterializable(c.child)
      case t: Transform                                      => true
      case c: CartesianProduct                               => false
      case plan: LogicalPlan if plan.mode == ExecMode.Atomic => true
      case _                                                 => false
    }
  }

  /** check if output size is small */
  private def isOutputSmall(plan: LogicalPlan) = {

    val attrs = plan match {
      case a: Aggregate => a.outputOld.diff(a.producedOutput)
      case p: Project   => plan.outputOld
    }

    plan
      .find(childPlan =>
        if (childPlan.isInstanceOf[Relation]) {
          attrs.toSet.subsetOf(childPlan.outputOld.toSet)
        } else if (childPlan.isInstanceOf[PlaceHolder]) {
          attrs.toSet.subsetOf(childPlan.outputOld.toSet)
        } else {
          false
        }
      )
      .nonEmpty
  }
}
