package org.apache.spark.secco.execution.statsComputation

import org.apache.spark.secco.execution.SeccoPlan
import org.apache.spark.secco.optimization.statsEstimation.Statistics
import org.apache.spark.secco.util.misc.LogAble

/**
  * The keeper for storing the statistic of the plan and compute the statistic of the plan on-demand.
  */
class StatisticKeeper(@transient val plan: SeccoPlan)
    extends Serializable
    with LogAble {

  private var _statistics: Option[Statistics] = None

  /** Get the inner statistic. */
  def statistics(): Statistics = {
    assert(_statistics.nonEmpty)
    _statistics.get
  }

  /** Set the inner statistic. */
  def setStatistic(stats: Statistics): Unit = {
    _statistics = Some(stats)
  }

  /** Return the statistic that at least contains row count information. */
  def rowCountOnlyStatistic(): Statistics = {
    computeRowCountOnlyStatistic()
    statistics()
  }

  /** Return the statistic that at least contains histogram information. */
  def histogramStatistic(): Statistics = {
    computeHistogramStatistic()
    statistics()
  }

  /** Return the statistic that at least contains full cardinality information. */
  def fullCardinalityStatistic(): Statistics = {
    computeFullCardinalityStatistic()
    statistics()
  }

  /**
    * Update the inner statistic with row count information.
    * Note: If inner statistic already contains cardinality information, computation will be bypassed.
    */
  def computeRowCountOnlyStatistic(): Unit = {
    if (_statistics.isEmpty) {
      plan.cacheOutput()
      val result = plan.cachedExecuteResult.get
      val time1 = System.currentTimeMillis()
      logInfo(
        s"computing row count statistic info for ${plan.verboseString}"
      )
      val rowCountStatistic =
        RowCountOnlyStatisticComputer.compute(plan.outputOld, result)
      val time2 = System.currentTimeMillis()
      logInfo(
        s"computed row count statistic info for ${plan.verboseString} in ${time2 - time1}ms"
      )

      _statistics = Some(rowCountStatistic)
    }
  }

  /**
    * Update the inner statistic with histogram information.
    * Note: If inner statistic already contains histogram information, computation will be bypassed.
    */
  def computeHistogramStatistic(): Unit = {
    if (_statistics.isEmpty || _statistics.get.attributeStats.isEmpty) {
      plan.cacheOutput()
      val result = plan.cachedExecuteResult.get
      val time1 = System.currentTimeMillis()
      val histogramStatistic =
        HistogramStatisticComputer.compute(plan.outputOld, result)
      val time2 = System.currentTimeMillis()
      logInfo(
        s"computed histogram statistic info for ${plan.verboseString} in ${time2 - time1}ms"
      )

      _statistics match {
        case Some(statistic) =>
          _statistics = Some(
            statistic.copy(attributeStats = histogramStatistic.attributeStats)
          )
        case None => _statistics = Some(histogramStatistic)
      }
    }
  }

  /**
    * Update the inner statistic with full cardinality information.
    * Note: If inner statistic already contains full cardinality information, computation will be bypassed.
    */
  def computeFullCardinalityStatistic(): Unit = {

    if (_statistics.isEmpty || _statistics.get.fullCardinality.isEmpty) {
      plan.cacheOutput()
      val result = plan.cachedExecuteResult.get
      val time1 = System.currentTimeMillis()
      val fullCardinalityStatistic =
        FullCardinalityStatisticComputer.compute(plan.outputOld, result)
      val time2 = System.currentTimeMillis()
      logInfo(
        s"computed full cardinality statistic info for ${plan.verboseString} in ${time2 - time1}ms"
      )

      _statistics match {
        case Some(statistic) =>
          _statistics = Some(
            statistic.copy(fullCardinality =
              fullCardinalityStatistic.fullCardinality
            )
          )
        case None => _statistics = Some(fullCardinalityStatistic)
      }
    }
  }
}

object StatisticKeeper {
  def apply(plan: SeccoPlan): StatisticKeeper = new StatisticKeeper(plan)
}
