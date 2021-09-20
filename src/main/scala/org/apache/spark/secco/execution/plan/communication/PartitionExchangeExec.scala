package org.apache.spark.secco.execution.plan.communication

import org.apache.spark.secco.execution._
import org.apache.spark.secco.execution.plan.support.PairExchangeSupport
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper
import org.apache.spark.secco.util.misc.SparkSingle
import org.apache.spark.rdd.RDD

import scala.collection.mutable

case class PartitionExchangeExec(
    child: SeccoPlan,
    sharedShare: SharedParameter[mutable.HashMap[String, Int]]
) extends UnaryExecNode
    with PairExchangeSupport {

  override lazy val statisticKeeper: StatisticKeeper = child.statisticKeeper

  override def outputOld: Seq[String] = child.outputOld

  lazy val sentryRDD: RDD[(InternalRow, Boolean)] =
    sparkContext.parallelize(genSentry(outputOld), 10).cache()

  override protected def doExecute(): RDD[InternalBlock] = {

    // do execute
    val _output = outputOld

    val spark = SparkSingle.getSparkSession()

    val relationRDD = child.execute().flatMap {
      case RowBlock(_, blockContent) =>
        blockContent.content.iterator.map(g => (g, false))
      case _ => throw new Exception(s"child of $this must output RowBlock")
    }

    val partitionedRDD =
      relationRDD.union(sentryRDD).partitionBy(taskPartitioner)

    val indexedBlockRDD = partitionedRDD.mapPartitions { it =>
      var shareVector: Array[Int] = null
      val content = it.toArray
      val array = new Array[InternalRow](content.length - 1)

      var j = 0
      var i = 0
      val contentSize = content.size
      while (j < contentSize) {
        val (tuple, isSentry) = content(j)
        if (isSentry) {
          shareVector = tuple.map(_.asInstanceOf[Int])
        } else {
          array(i) = tuple
          i += 1
        }
        j += 1
      }

      Iterator(
        RowIndexedBlock(_output, shareVector, RowBlockContent(array))
          .asInstanceOf[InternalBlock]
      )
    }

    // increment the counter for benchmark
    val counterManager = dlSession.sessionState.counterManager
    counterManager
      .getOrCreateCounter("benchmark", s"communicationCostInTuples")
      .increment(statisticKeeper.rowCountOnlyStatistic().rowCount.toLong)
    counterManager
      .getOrCreateCounter("benchmark", s"communicationCostInBytes")
      .increment(
        statisticKeeper
          .rowCountOnlyStatistic()
          .rowCount
          .toLong * outputOld.size * 8
      )

    if (conf.recordCommunicationTime) {
      val time1 = System.currentTimeMillis()
      indexedBlockRDD.map(f => 1).sum()
      val time2 = System.currentTimeMillis()
      val communicationTime = time2 - time1

      counterManager
        .getOrCreateCounter("benchmark", "communicationTime(ms)")
        .increment(communicationTime)
    }

    indexedBlockRDD
  }
}
