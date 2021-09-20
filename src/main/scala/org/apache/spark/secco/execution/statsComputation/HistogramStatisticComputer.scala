package org.apache.spark.secco.execution.statsComputation

import org.apache.spark.secco.SeccoSession
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
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * The computer for computing the histogram statistic of the content.
  */
object HistogramStatisticComputer {

  def compute(
      attributes: Seq[String],
      content: RDD[InternalBlock]
  ): Statistics = {

    val conf = SeccoSession.currentSession.sessionState.conf

    val num_bin_histogram = conf.numBinHistogram
    //    val num_partition = conf.NUM_PARTITION

    //warning: this practice is just for testing.
    //before compute the statistic, we should gather all partition of content into one partition.
    val contentInOnePartition = content
      .flatMap { block =>
        block match {
          case RowBlock(output, blockContent) =>
            blockContent.content
          case _ => throw new Exception("only RowBlock supports statistic")
        }
      }
      .repartition(1)
      .mapPartitions { rows =>
        Iterator(RowBlock(Seq(), RowBlockContent(rows.toArray)))
      }

    //count distinct numbers of value of attributes and
    // cardinality of the relation.
    val partitionStatistics = contentInOnePartition
      .mapPartitions { blocks =>
        blocks.flatMap { block =>
          block match {
            case RowBlock(output, blockContent) =>
              val array = blockContent.content

              //compute row count
              val rowCount = array.size.toLong

              //compute column statistics
              val colStats = attributes.zipWithIndex.map {
                case (attr, idx) =>
                  val colArr = array.map(f => f(idx)).sorted

                  //compute equi-height histogram
                  val height = rowCount / num_bin_histogram match {
                    case 0 =>
                      1 // this mean each bucket will contain around 1 element
                    case x => x
                  }

                  val percentileArr =
                    new Array[InternalDataType](num_bin_histogram + 1)
                  val binSet =
                    Array.fill(num_bin_histogram)(
                      mutable.HashSet[InternalDataType]()
                    )

                  var i = 0
                  var binId = 0
                  var eleCount = 0
                  var binCount = 1
                  while (i < colArr.size && binId < num_bin_histogram) {
                    val value = colArr(i)
                    eleCount += 1
                    binSet(binId) += value

                    //mark the initial low
                    if (i == 0) {
                      percentileArr(binId) = value
                    }

                    //first bucket is full, move to next bucket
                    if (eleCount == binCount * height) {
                      percentileArr(binId + 1) = value
                      binId += 1
                      binCount += 1
                    }
                    i += 1
                  }

                  percentileArr(num_bin_histogram) =
                    colArr.lastOption.getOrElse(0.0)

                  val bins = binSet.zipWithIndex
                    .map {
                      case (ndvSet, binId) =>
                        HistogramBin(
                          percentileArr(binId),
                          percentileArr(binId + 1),
                          ndvSet.size
                        )
                    }
                    .filter(_.ndv != 0)

                  val histogram = Histogram(height, bins)

                  //compute other statistics
                  val distinctCount = colArr.distinct.size
                  val min = colArr.headOption.getOrElse(0.0)
                  val max = colArr.lastOption.getOrElse(0.0)

                  (
                    attr,
                    ColumnStat(
                      Some(distinctCount),
                      Some(min),
                      Some(max),
                      None,
                      None,
                      None,
                      Some(histogram)
                    )
                  )
              }

              val colStatsMap = mutable.HashMap(colStats: _*)

              Array(Statistics(rowCount, None, colStatsMap))
            case _ =>
              throw new Exception("only RowBlock supports statistic")
          }
        }
      }
      .take(1)

    //assume the table are partitioned by hash partitioning
    //just use the statistic of the first partition as the statistic for the whole table
    val statisticsOfPartition = partitionStatistics.head

    val statistic = Statistics(
      statisticsOfPartition.rowCount,
      statisticsOfPartition.sizeInBytes,
      statisticsOfPartition.attributeStats
    )

    statistic
  }

}
