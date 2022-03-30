package org.apache.spark.secco.execution.planning

import org.apache.spark.secco.execution.plan.computation.{
  LocalProcessingExec,
  LocalStageExec
}
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.trees.TreeNode
import org.apache.spark.internal.Logging
import org.apache.spark.secco.optimization.plan.PairThenCompute

/** Given a [[LogicalPlan]], returns a list of `PhysicalPlan`s that can
  * be used for execution. If this strategy does not apply to the given logical operation then an
  * empty list should be returned.
  */
abstract class GenericStrategy[PhysicalPlan <: TreeNode[PhysicalPlan]]
    extends Logging {

  /** Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
    * filled in automatically by the QueryPlanner using the other execution strategies that are
    * available.
    */
  protected def planLater(plan: LogicalPlan): PhysicalPlan

  /** Returns a placeholder for a local physical plan that executes `plan`. This placeholder will be
    * filled in automatically by the QueryPlanner using the other execution strategies that are
    * available.
    */
  protected def localPlanLater(
      plan: LogicalPlan,
      localStage: LocalStageExec
  ): LocalProcessingExec

  def apply(plan: LogicalPlan): Seq[PhysicalPlan]
}

/** Abstract class for transforming [[LogicalPlan]]s into physical plans.
  * Child classes are responsible for specifying a list of [[GenericStrategy]] objects that
  * each of which can return a list of possible physical plan options.
  * If a given strategy is unable to plan all of the remaining operators in the tree,
  * it can call [[GenericStrategy#planLater planLater]], which returns a placeholder
  * object that will be [[collectPlaceholders collected]] and filled in
  * using other available strategies.
  *
  * TODO: RIGHT NOW ONLY ONE PLAN IS RETURNED EVER...
  *       PLAN SPACE EXPLORATION WILL BE IMPLEMENTED LATER.
  *
  * @tparam PhysicalPlan The type of physical plan produced by this [[QueryPlanner]]
  */
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {

  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...

    // Collect physical plan candidates.
    val candidates = strategies.iterator.flatMap(_(plan))

    // The candidates may contain placeholders marked as [[planLater]],
    // so try to replace them by their child plans.
    val plans = candidates.flatMap { candidate =>
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        // Plan the logical plan marked as [[planLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Plan the logical plan for the placeholder.
            val childPlans = this.plan(logicalPlan)

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholders =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholders.transformUp {
                  case p if p.eq(placeholder) => childPlan
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    assert(pruned.hasNext, s"No plan for $plan")
    pruned
  }

  /** Collects placeholders marked using [[GenericStrategy#planLater planLater]]
    * by [[strategies]].
    */
  protected def collectPlaceholders(
      plan: PhysicalPlan
  ): Seq[(PhysicalPlan, LogicalPlan)]

  /** Prunes bad plans to prevent combinatorial explosion. */
  protected def prunePlans(
      plans: Iterator[PhysicalPlan]
  ): Iterator[PhysicalPlan]
}
