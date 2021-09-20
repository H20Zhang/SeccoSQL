package org.apache.spark.secco.execution.statsComputation

import org.apache.spark.secco.execution.{InternalBlock, RowBlock}
import org.apache.spark.secco.optimization.statsEstimation.Statistics
import org.apache.spark.rdd.RDD

/**
  * The computer for computing only the row count statistic of the content
  */
object RowCountOnlyStatisticComputer extends StatisticComputer {
  def compute(
      attributes: Seq[String],
      content: RDD[InternalBlock]
  ): Statistics = {

    val rowCount = content
      .map { block =>
        block match {
          case RowBlock(output, blockContent) =>
            blockContent.content.size.toLong
          case b: InternalBlock =>
            throw new Exception(
              s"${this.getClass} does not support compute statistic for ${b.getClass}"
            )
        }
      }
      .sum()
      .toLong

    Statistics(rowCount)
  }
}
