package org.apache.spark.secco

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

  }
}
