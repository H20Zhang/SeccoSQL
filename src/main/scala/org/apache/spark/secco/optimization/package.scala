package org.apache.spark.secco

import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  BinaryJoin,
  CartesianProduct,
  Distinct,
  Except,
  Filter,
  MultiwayJoin,
  Project,
  Sort,
  Union
}

package object optimization {

  /** Execution mode of the logical plan.
    *   <ul>
    *     <li> Coupled: op that whose computation and communication can be decoupled </li>
    *     <li> CoupledWithComputationDelay: op that whose computation and communication can be decoupled, and
    *     computation is to be delayed </li>
    *     <li> Communication: op that only involves communication </li>
    *     <li> Computation: op that only involves computation, and computation is not delayed </li>
    *     <li> DelayComputation: op that only involves computation, and computation is to be delayed </li>
    *     <li> Atomic: op that won't be decoupled into communication and computation op</li>
    *   </ul>
    */
  object ExecMode extends Enumeration {
    type ExecMode = Value

    val Atomic, Coupled, MarkedDelay, Communication, Computation,
        DelayComputation =
      Value

    def newPlanWithMode(plan: LogicalPlan, newMode: ExecMode): LogicalPlan =
      plan match {
        case s: Sort             => s.copy(mode = newMode)
        case d: Distinct         => d.copy(mode = newMode)
        case p: Project          => p.copy(mode = newMode)
        case a: Aggregate        => a.copy(mode = newMode)
        case d: Except           => d.copy(mode = newMode)
        case b: BinaryJoin       => b.copy(mode = newMode)
        case u: Union            => u.copy(mode = newMode)
        case j: MultiwayJoin     => j.copy(mode = newMode)
        case c: CartesianProduct => c.copy(mode = newMode)
        case f: Filter           => f.copy(mode = newMode)
        case _ =>
          throw new Exception(
            s"not supported plan:${plan.nodeName} for mutating ExecMode"
          )
      }

  }
}
