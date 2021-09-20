package org.apache.spark.secco.optimization.support

import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.optimization.statsEstimation.exact.ExactStatsPlanVisitor
import org.apache.spark.secco.optimization.statsEstimation.histogram.HistogramBasedStatsPlanVisitor
import org.apache.spark.secco.optimization.statsEstimation.{
  Statistics,
  StatsPlanVisitor
}

/**
  * A trait to add statistics propagation to [[LogicalPlan]].
  */
trait LogicalPlanStatsSupport {
  self: LogicalPlan =>

  /**
    * Returns the estimated statistics for the current logical plan node. Under the hood, this
    * method caches the return value, which is computed based on the configuration passed in the
    * first time. If the configuration changes, the cache can be invalidated by calling
    * [[invalidateStatsCache()]].
    */
  def stats: Statistics = {

    statsCache.getOrElse {
      statsCache = Option(StatsPlanVisitor.visit(self))
      statsCache.get
    }

  }

  /** A cache for the estimated statistics, such that it will only be computed once. */
  protected var statsCache: Option[Statistics] = None

  /** Invalidates the stats cache. See [[stats]] for more information. */
  final def invalidateStatsCache(): Unit = {
    statsCache = None
    children.foreach(_.invalidateStatsCache())
  }

}
