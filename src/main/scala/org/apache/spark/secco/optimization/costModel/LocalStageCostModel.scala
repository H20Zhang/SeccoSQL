package org.apache.spark.secco.optimization.costModel

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.execution.plan.communication.EnumShareComputer
import org.apache.spark.secco.optimization.plan.{
  PairThenCompute,
  Partition,
  Relation
}
import org.apache.spark.secco.optimization.statsEstimation.StatsPlanVisitor
import org.apache.spark.secco.optimization.statsEstimation.histogram.HistogramBasedStatsPlanVisitor

object LocalStageCostModel extends CostModel[PairThenCompute] {

  /** Estimate the communication cost of the plan */
  override def communicationCost(plan: PairThenCompute): Double = ???
//  {
//
//    if (plan.children.forall(_.isInstanceOf[Partition])) {
//
//      val dl = SeccoSession.currentSession.sessionState.conf
//
//      val partitions = plan.children.map(_.asInstanceOf[Partition])
//      val restriction = partitions.head.restriction
//      val cardinalities =
//        partitions.map(StatsPlanVisitor.visit(_).rowCount.toLong)
//      val schemas = partitions.map(_.output)
//      val statisticMap = schemas.zip(cardinalities).toMap
//
//      val shareComputer = new EnumShareComputer(
//        schemas,
//        restriction,
//        dl.numPartition,
//        statisticMap
//      )
//
//      shareComputer.optimalShareWithBudget().communicationCostInTuples
//    } else if (plan.children.forall(_.isInstanceOf[Relation])) {
//      0.0
//    } else {
//      throw new Exception(
//        s"no communication cost estimation available for plan:${plan}"
//      )
//    }
//  }

  /** Estimate the computation cost of the plan */
  override def computationCost(plan: PairThenCompute): Double = ???
}
