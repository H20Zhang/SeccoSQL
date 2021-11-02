package org.apache.spark.secco.optimization.statsEstimation.histogram

import org.apache.spark.secco.optimization.plan.Project
import org.apache.spark.secco.optimization.statsEstimation.{
  Estimation,
  Statistics
}

object HistogramProjectEstimation extends Estimation[Project] {
//  override def estimate(project: Project): Option[Statistics] = {
//    val childStats = project.child.stats
//    val inputAttrStats = childStats.attributeStats
//
//    val outputAttrStats =
//      Estimation.getOutputMap(inputAttrStats, project.outputOld)
//    Some(childStats.copy(attributeStats = outputAttrStats))
//  }

  override def estimate(x: Project): Option[Statistics] = ???
}
