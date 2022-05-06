package org.apache.spark.secco.optimization.statsEstimation.histogram

import org.apache.spark.secco.optimization.plan.Aggregate
import org.apache.spark.secco.optimization.statsEstimation.{
  Estimation,
  Statistics
}

object HistogramAggregateEstimation extends Estimation[Aggregate] {
//  override def estimate(agg: Aggregate): Option[Statistics] = {
//    val childStats = agg.child.stats
//    val inputAttrStats = childStats.attributeStats
//
//    //we just left out the column stats of the newly generated column, while maintaining others
//    //in groupingList columns
//    val outputAttrStats =
//      Estimation.getOutputMap(
//        inputAttrStats,
//        agg.groupingExpressions.map(_.toAttribute)
//      )
//    Some(childStats.copy(attributeStats = outputAttrStats))
//  }

  override def estimate(x: Aggregate): Option[Statistics] = ???
}
