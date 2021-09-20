package org.apache.spark.secco.optimization.statsEstimation.histogram

import org.apache.spark.secco.optimization.plan.Filter
import org.apache.spark.secco.optimization.statsEstimation.{
  Estimation,
  Statistics
}

object HistogramFilterEstimation extends Estimation[Filter] {

  /**
    * Returns an option of Statistics for a Filter logical plan node.
    * For a given compound expression condition, this method computes filter selectivity
    * (or the percentage of rows meeting the filter condition), which
    * is used to compute row count, size in bytes, and the updated statistics after a given
    * predicated is applied.
    *
    * @return Option[Statistics] When there is no statistics collected, it returns None.
    */
  override def estimate(plan: Filter): Option[Statistics] = {
    None
  }
}
