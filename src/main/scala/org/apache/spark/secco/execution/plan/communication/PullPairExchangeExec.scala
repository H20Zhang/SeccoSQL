package org.apache.spark.secco.execution.plan.communication

import java.io.{IOException, ObjectOutputStream}
import org.apache.spark._
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper
import org.apache.spark.secco.execution.{SeccoPlan, SharedContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.secco.execution.storage.{
  InternalPartition,
  PairedPartition
}
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.Utils

import scala.collection.mutable

/** The operator that pairs partitions from multiple inputs.
  * @param children The input relations
  * @param shareConstraintContext The constraint on the share values an attribute can take
  * @param shareValuesContext The share values of the attributes
  */
case class PullPairExchangeExec(
    children: Seq[SeccoPlan],
    shareConstraintContext: ShareConstraintContext,
    shareValuesContext: ShareValuesContext
) extends SeccoPlan {

  override lazy val statisticKeeper: StatisticKeeper = {
    throw new Exception(
      s"${this.getClass} does not have StatisticKeeper, as it produces multi-table output."
    )
  }

  override def output: Seq[Attribute] = children.flatMap(_.output)

  /** Generate the partition information for [[PullPairExchangeExec]] */
  def genPullPairPartitions(
      children: Seq[SeccoPlan],
      inputs: Seq[RDD[InternalPartition]]
  ): Array[PullPairRDDPartition] = {

    val shareValues = shareValuesContext.shares
    val taskPartitioner = shareValues.genTaskPartitioner(output.toArray)
    val partitioners = children.map(child => child.taskPartitioner())
    val taskCoordinates = shareValues.genHyperCubeCoordinates(output)

    val pairPartitions = taskCoordinates
      .map { coordinate =>
        val partitionIDs = children
          .map { child =>
            shareValues.getSubCoordinate(coordinate, child.output.toArray)
          }
          .zipWithIndex
          .map { case (subCoordinate, idx) =>
            partitioners(idx).getPartition(subCoordinate)
          }

        val taskID = taskPartitioner.getPartition(coordinate)

        new PullPairRDDPartition(taskID, partitionIDs, inputs)
      }
      .sortBy(_.index)

    pairPartitions
  }

  override protected def doPrepare(): Unit = {
    super.doPrepare()

    // Compute the share.
    if (!shareValuesContext.shares.isInitialized()) {

      val schemas = children.map(_.output)
      val cardinalities = children.map { child =>
        (
          child.output,
          child.statisticKeeper.rowCountOnlyStatistic().rowCount.toLong
        )
      }.toMap

      val shareValuesOptimizer = new EnumShareComputer(
        schemas,
        shareConstraintContext.shareConstraint,
        SeccoConfiguration.newDefaultConf().numPartition,
        cardinalities
      )
      val optimizedShareValuesResult =
        shareValuesOptimizer.optimalShareWithBudget()
      shareValuesContext.shares.rawShares = AttributeMap(
        optimizedShareValuesResult.rawShares.toSeq
      )
    }

    // Cache the output of children to avoid repeated materialization during pairing.
    // It is worth noting that we should change the storagelevel to serialization to avoid memory overflow when
    // trying to satisfy multiple get block request.
    children.foreach(_.cacheOutput())

  }

  override protected def doCleanUp(): Unit = {
    children.foreach { child =>
      val grandChildren = child.children
      grandChildren.foreach { grandChild =>
        grandChild.foreach { execPlan =>
          execPlan.cachedExecuteResult match {
            case Some(rdd) => rdd.unpersist(false)
            case None      => {}
          }
          execPlan.cachedExecuteResult = None
        }
      }
    }
  }

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalPartition] = {

    //gen inputRDDs
    val inputRDDs = children.map(_.cachedExecuteResult.get)

    //gen partitions for PullPairRDD
    val subTaskPartitions = genPullPairPartitions(
      children,
      inputRDDs
    )

    //gen PullPairRDD
    val pairedRDD = new PullPairRDD(
      output,
      sparkContext,
      subTaskPartitions,
      taskPartitioner,
      inputRDDs
    )

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

class PullPairRDD[A <: InternalPartition: Manifest](
    output: Seq[Attribute],
    sc: SparkContext,
    pairedPartitions: Array[PullPairRDDPartition],
    _partitioner: Partitioner,
    var rdds: Seq[RDD[InternalPartition]]
) extends RDD[InternalPartition](sc, rdds.map(x => new OneToOneDependency(x))) {
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
  ): Iterator[InternalPartition] = {
    val pairedPartition = split.asInstanceOf[PullPairRDDPartition]
    val partitionList = pairedPartition.partitionValues.par.map { f =>
      val iterator1 = rdds(f._1).iterator(f._2, context)
      val block = iterator1.next()
      iterator1.hasNext
      block
    }.toArray

    Iterator(PairedPartition(output, partitionList, None, None))
  }

}
