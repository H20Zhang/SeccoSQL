package org.apache.spark.secco.execution.statsComputation

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.{
  InternalBlock,
  InternalDataType,
  RowBlock,
  RowBlockContent
}
import org.apache.spark.secco.optimization.statsEstimation.{
  ColumnStat,
  Histogram,
  HistogramBin,
  Statistics
}
import org.apache.spark.secco.util.`extension`.SeqExtension
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * The trait for class that compute the statistics of the content
  */
trait StatisticComputer {

  /**
    * compute the statistic
    * @param attributes attributes of the content
    * @param content the RDD that contains the relation whose statistic to be computed.
    * @return
    */
  def compute(
      attributes: Seq[String],
      content: RDD[InternalBlock]
  ): Statistics
}
