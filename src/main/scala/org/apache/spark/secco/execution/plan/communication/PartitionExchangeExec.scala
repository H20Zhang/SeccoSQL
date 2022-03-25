package org.apache.spark.secco.execution.plan.communication

import org.apache.spark.secco.execution._
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper
import org.apache.spark.secco.util.misc.SparkSingle
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.execution.storage.block.UnsafeInternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.execution.storage.{
  InternalPartition,
  UnsafeBlockPartition
}
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.types.StructType

import scala.collection.mutable

/** An operator that partitions the rows according to partitioner specified by [[ShareValues]]. */
case class PartitionExchangeExec(
    child: SeccoPlan,
    sharesContext: ShareValuesContext
) extends UnaryExecNode {

  override lazy val statisticKeeper: StatisticKeeper = child.statisticKeeper

  private def share = sharesContext.shares

  override def output: Seq[Attribute] = child.output

  override def taskPartitioner(): PairPartitioner =
    share.genTaskPartitioner(output.toArray)

  /** Generate the sentry row in case some of the coordinate that does not have any rows.
    *
    * Note: In current HyperCube Shuffle implement, it is crucial to gurantee that each coordiante have at least one row.
    * (Empty row is used if there is no row that contains actual data)
    */
  lazy val sentryRowRDD: RDD[(InternalRow, Boolean)] =
    sparkContext.parallelize(share.genSentryRows(output.toArray), 10).cache()

  override protected def doExecute(): RDD[InternalPartition] = {

    val spark = SparkSingle.getSparkSession()

    val rowRDD = child.execute().flatMap {
      case UnsafeBlockPartition(_, data, _, _) =>
        data.head.iterator
          .map(row => (row, true)) //TODO: try directly partition UnsafeBlock
      case _ =>
        throw new Exception(
          s"The input of PartitionExchangeExec must output UnsafeBlockPartition."
        )
    }

    val rawPartitionedRDD =
      rowRDD.union(sentryRowRDD).partitionBy(taskPartitioner)

    val partitionRDD = rawPartitionedRDD.mapPartitions { it =>
      var coordinate: Coordinate = null
      val content = it.toArray
      val array = new Array[InternalRow](content.length - 1)

      var j = 0
      var i = 0
      val contentSize = content.size
      while (j < contentSize) {
        val (tuple, isSentry) = content(j)

        // Derive the coordinate of this partition based on sentry.
        if (isSentry) {
          coordinate = Coordinate(
            output.toArray,
            tuple.toSeq(output.map(_.dataType)).map(_.asInstanceOf[Int]).toArray
          )
        } else {
          array(i) = tuple
          i += 1
        }
        j += 1
      }

      val unsafeBlockPartition = UnsafeBlockPartition(
        output,
        Seq(UnsafeInternalBlock(array, StructType.fromAttributes(output))),
        Some(coordinate),
        Some(taskPartitioner)
      )

      // We assume each RDD partition just stores one InternalPartition.
      Iterator(
        unsafeBlockPartition.asInstanceOf[InternalPartition]
      )
    }

    partitionRDD
  }
}

//// increment the counter for benchmark
//val counterManager = seccoSession.sessionState.counterManager
//counterManager
//.getOrCreateCounter("benchmark", s"communicationCostInTuples")
//.increment(statisticKeeper.rowCountOnlyStatistic().rowCount.toLong)
//counterManager
//.getOrCreateCounter("benchmark", s"communicationCostInBytes")
//.increment(
//statisticKeeper
//.rowCountOnlyStatistic()
//.rowCount
//.toLong * output.size * 8
//)
//
//if (conf.recordCommunicationTime) {
//  val time1 = System.currentTimeMillis()
//  indexedBlockRDD.map(f => 1).sum()
//  val time2 = System.currentTimeMillis()
//  val communicationTime = time2 - time1
//
//  counterManager
//  .getOrCreateCounter("benchmark", "communicationTime(ms)")
//  .increment(communicationTime)
//}
