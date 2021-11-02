package org.apache.spark.secco.execution.planning

import org.apache.spark.SparkContext
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.{SeccoPlan, Strategy}
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.util.misc.SparkSingle

class SeccoPlanner(
    val sparkContext: SparkContext = SparkSingle.getSparkContext(),
    val conf: SeccoConfiguration = SeccoConfiguration.newDefaultConf()
) extends SeccoStrategies {

  def numPartitions: Int = conf.numPartition

  //TODO: refactor Planner Rules
  override def strategies: Seq[Strategy] = Nil
//    IOStrategy :: LOpStrategy :: LocalExecStrategy :: AtomicStrategy :: Nil

  override protected def collectPlaceholders(
      plan: SeccoPlan
  ): Seq[(SeccoPlan, LogicalPlan)] = {
    plan.collect { case placeholder @ PlanLater(logicalPlan) =>
      (placeholder, logicalPlan)
//      case placeholder @ LocalPlanLater(logicalPlan) =>
//        (placeholder, logicalPlan)
    }
  }

  override protected def prunePlans(
      plans: Iterator[SeccoPlan]
  ): Iterator[SeccoPlan] = {
    // TODO: We will need to prune bad plans when we improve plan space exploration
    //       to prevent combinatorial explosion.
    plans
  }

}
