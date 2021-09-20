package org.apache.spark.secco.execution.plan.communication

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark._
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.plan.communication.utils.{
  EnumShareComputer,
  PairPartitioner,
  ShareResults
}
import org.apache.spark.secco.execution.plan.support.PairExchangeSupport
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper
import org.apache.spark.secco.execution.{
  SeccoPlan,
  InternalBlock,
  MultiTableIndexedBlock,
  SharedParameter
}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.Utils

import scala.collection.mutable

case class PullPairExchangeExec(
    children: Seq[SeccoPlan],
    constraint: Map[String, Int],
    sharedShare: SharedParameter[mutable.HashMap[String, Int]]
) extends SeccoPlan
    with PairExchangeSupport {

  override lazy val statisticKeeper: StatisticKeeper = {
    throw new Exception(
      s"${this.getClass} does not have StatisticKeeper, as it produces multi-table output."
    )
  }

  override def outputOld: Seq[String] = children.flatMap(_.outputOld).distinct

  /** generate the partition for [[PullPairExchangeExec]] */
  def genPullPairPartitions(
      children: Seq[SeccoPlan],
      inputs: Seq[RDD[InternalBlock]]
  ): Array[PullPairRDDPartition] = {

    val attrs = outputOld
    val shareSpaceVector = attrs.map(share).toArray
    val taskPartitioner = new PairPartitioner(shareSpaceVector)
    val partitioners = children.map(child => child.taskPartitioner())
    val shareOfTasks = genShareVectorsForAttrs(attrs)

    val subTaskPartitions = shareOfTasks
      .map { shareVector =>
        val blockIDs = children
          .map { child =>
            val childAttrs = child.outputOld
            val pos = childAttrs.map { attrID =>
              attrs.indexOf(attrID)
            }
            pos.map(shareVector).toArray
          }
          .zipWithIndex
          .map {
            case (subShareVector, relationPos) =>
              partitioners(relationPos).getPartition(subShareVector)
          }

        val taskID = taskPartitioner.getPartition(shareVector)

        new PullPairRDDPartition(taskID, blockIDs, inputs)
      }
      .sortBy(_.index)

    subTaskPartitions
  }

  override protected def doPrepare(): Unit = {
    super.doPrepare()

//    /** make sure all the children are of type [[PartitionExchangeExec]] */
//    assert(children.forall(_.isInstanceOf[PartitionExchangeExec]))

    //compute the share
    if (share.isEmpty) {

      val schemas = children.map(_.outputOld)
      val cardinalities = children.map { child =>
        (
          child.outputOld,
          child.statisticKeeper.rowCountOnlyStatistic().rowCount.toLong
        )
      }.toMap

      val shareComputer = new EnumShareComputer(
        schemas,
        constraint,
        SeccoConfiguration.newDefaultConf().numPartition,
        cardinalities
      )
      val shareResults = shareComputer.optimalShareWithBudget()
      _shareResults = Some(shareResults)
      shareResults.share.foreach { case (key, value) => share(key) = value }
    }

    //cache the output of children to avoid repeated materialization during pairing
    //it is worth noting that we should change the storagelevel to serialization to avoid memory overflow when
    //trying to satisfy multiple get block request
    children.foreach(_.cacheOutput())

  }

  override protected def doCleanUp(): Unit = {
    children.foreach { child =>
      val grandChildren = child.children
      grandChildren.foreach { grandChild =>
        grandChild.foreach { dolpinPlan =>
          dolpinPlan.cachedExecuteResult match {
            case Some(rdd) => rdd.unpersist(false)
            case None      => {}
          }
          dolpinPlan.cachedExecuteResult = None
        }
      }
    }
  }

  /**
    * Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalBlock] = {

    //gen inputRDDs
    val inputRDDs = children.map(_.cachedExecuteResult.get)

    //gen partitions for PullPairRDD
    val subTaskPartitions = genPullPairPartitions(
      children,
      inputRDDs
    )

    //gen PullPairRDD
    val pairedRDD = new PullPairRDD(
      outputOld,
      sparkContext,
      subTaskPartitions,
      taskPartitioner,
      inputRDDs
    )

    // increment the counter for benchmark

    val counterManager = dlSession.sessionState.counterManager
    counterManager
      .getOrCreateCounter("benchmark", s"communicationCostInTuples")
      .increment(_shareResults.map(_.communicationCostInTuples).getOrElse(0))
    counterManager
      .getOrCreateCounter("benchmark", s"communicationCostInBytes")
      .increment(_shareResults.map(_.communicationCostInBytes).getOrElse(0))

    if (conf.recordCommunicationTime) {
      logInfo(
        s"""perform communication only execution of plan:${this}""".stripMargin
      )
      val time1 = System.currentTimeMillis()
      pairedRDD.map(f => 1).sum()
      val time2 = System.currentTimeMillis()
      val communicationTime = time2 - time1

      counterManager
        .getOrCreateCounter("benchmark", "communicationTime(ms)")
        .increment(communicationTime)
    }

    pairedRDD
  }

}

class PullPairRDDPartition(
    idx: Int,
    dependencyBlockID: Seq[Int],
    @transient private val rdds: Seq[RDD[_]]
) extends Partition {
  override val index: Int = idx

  var partitionValues: Seq[(Int, Partition)] = dependencyBlockID.zipWithIndex
    .map(_.swap)
    .map(f => (f._1, rdds(f._1).partitions(f._2)))

  def partitions: Seq[(Int, Partition)] = partitionValues

  //calculate the preferedLocation for this parition, currently it randomly choose an old cached logoblock's location as preferedLocation
  def calculatePreferedLocation: Seq[String] = {
    var prefs = dependencyBlockID.zipWithIndex
      .map(_.swap)
      .map(f => rdds(f._1).preferredLocations(rdds(f._1).partitions(f._2)))
    //The Prefed Location is very important for index reuse, however for some init input rdd's it may not have a prefered location,
    //which can result error.

    //find all blocks location using blockManager.master
    if (prefs.flatten.distinct.forall(f => f == "")) {
      val sparkEnv = SparkEnv.get
      val blockManagerMaster = sparkEnv.blockManager.master
      prefs = dependencyBlockID.zipWithIndex.map(_.swap).map { f =>
        val blockId =
          RDDBlockId(rdds(f._1).id, rdds(f._1).partitions(f._2).index)

        blockManagerMaster
          .getLocations(blockId)
          .map(f => TaskLocation(f.host, f.executorId).toString)

      }
    }

    if (prefs.flatten.distinct.forall(f => f == "")) {
      val sparkEnv = SparkEnv.get
      val blockManagerMaster = sparkEnv.blockManager.master
      prefs = Seq(
        sparkEnv.blockManager.master.getMemoryStatus.keys.toSeq
          .map(f => TaskLocation(f.host, f.executorId).toString)
      )
    }

    val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
    val locs =
      if (exactMatchLocations.nonEmpty) exactMatchLocations
      else prefs.flatten.distinct

    locs
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit =
    Utils.tryOrIOException {
      // Update the reference to parent split at the time of task serialization
      var partitionValues = dependencyBlockID.zipWithIndex
        .map(_.swap)
        .map(f => (f._1, rdds(f._1).partitions(f._2)))
      oos.defaultWriteObject()
    }
}

class PullPairRDD[A <: InternalBlock: Manifest](
    output: Seq[String],
    sc: SparkContext,
    pairedPartitions: Array[PullPairRDDPartition],
    _partitioner: Partitioner,
    var rdds: Seq[RDD[InternalBlock]]
) extends RDD[InternalBlock](sc, rdds.map(x => new OneToOneDependency(x))) {
  override val partitioner: Option[Partitioner] = Some(_partitioner)

  //reorder the subTaskPartitions according to their idx
  override def getPartitions: Array[Partition] = {
    pairedPartitions.asInstanceOf[Array[Partition]]
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[PullPairRDDPartition].calculatePreferedLocation
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }

  override def compute(
      split: Partition,
      context: TaskContext
  ): Iterator[InternalBlock] = {
    val subTaskPartition = split.asInstanceOf[PullPairRDDPartition]
    val blockList = subTaskPartition.partitionValues.par.map { f =>
      val iterator1 = rdds(f._1).iterator(f._2, context)
      val block = iterator1.next()
      iterator1.hasNext
      block
    }.toArray

    val shareVector =
      partitioner.get.asInstanceOf[PairPartitioner].getCoordinate(split.index)

    Iterator(MultiTableIndexedBlock(output, shareVector, blockList))
  }

}
