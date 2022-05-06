package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.optimization.{LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan.RootNode

object CleanRoot extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform { case r: RootNode =>
      r.child
    }
}
