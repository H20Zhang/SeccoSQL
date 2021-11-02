//package org.apache.spark.secco.execution.plan.communication
//
//import java.io.{IOException, ObjectOutputStream}
//
//import org.apache.spark._
//import org.apache.spark.secco.config.SeccoConfiguration
//import org.apache.spark.secco.execution.plan.communication.utils.{
//  EnumShareComputer,
//  PairPartitioner
//}
//import org.apache.spark.secco.execution.plan.support.PairExchangeSupport
//import org.apache.spark.secco.execution.{
//  SeccoPlan,
//  InternalBlock,
//  MultiTableIndexedBlock,
//  SharedParameter
//}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.scheduler.TaskLocation
//import org.apache.spark.storage.RDDBlockId
//import org.apache.spark.util.Utils
//
//import scala.collection.mutable
//
//case class IterativePairExchangeExec(
//    children: Seq[SeccoPlan],
//    orderRearrange: Seq[Int],
//    constraint: Map[String, Int],
//    sharedShare: SharedParameter[mutable.HashMap[String, Int]]
//) extends SeccoPlan
//    with PairExchangeSupport {
//
//  override def outputOld: Seq[String] = children.flatMap(_.outputOld).distinct
//
//  /** child that contains cached [[PartitionExchangeExec]] */
//  val cachedChild: SeccoPlan = children.head
//
//  /** child of uncached [[PartitionExchangeExec]] */
//  val uncachedChildren: Seq[SeccoPlan] = children.drop(1)
//
//  /** cached RDD of [[MultiTableIndexedBlock]], which stores block of cached [[PartitionExchangeExec]] */
//  var cachedRDD: Option[RDD[InternalBlock]] = None
//
//  /** make sure the first child is of type [[PullPairExchangeExec]] */
//  assert(cachedChild.isInstanceOf[PullPairExchangeExec])
//
//  /** generate the partition for [[PullPairRDD]] */
//  def genPullPairPartitions(
//      children: Seq[SeccoPlan],
//      inputs: Seq[RDD[InternalBlock]]
//  ): Array[IterativePullPairRDDPartition] = {
//
//    val attrs = outputOld
//    val shareSpaceVector = attrs.map(share).toArray
//    val taskPartitioner = new PairPartitioner(shareSpaceVector)
//    val partitioners = children.map(child => child.taskPartitioner())
//    val shareOfTasks = genShareVectorsForAttrs(attrs)
//
//    val subTaskPartitions = shareOfTasks
//      .map { shareVector =>
//        val blockIDs = children
//          .map { child =>
//            val childAttrs = child.outputOld
//            val pos = childAttrs.map { attrID =>
//              attrs.indexOf(attrID)
//            }
//            pos.map(shareVector).toArray
//          }
//          .zipWithIndex
//          .map {
//            case (subShareVector, relationPos) =>
//              partitioners(relationPos).getPartition(subShareVector)
//          }
//
//        val taskID = taskPartitioner.getPartition(shareVector)
//
//        new IterativePullPairRDDPartition(taskID, blockIDs, inputs)
//      }
//      .sortBy(_.index)
//
//    subTaskPartitions
//  }
//
//  override protected def doPrepare(): Unit = {
//    super.doPrepare()
//
//    //compute the share
//    if (share.isEmpty) {
//
//      // As PullPairExchangeExec does not support statistic computation,
//      // we need to get the children of PullPairExchangeExec to get the whole children involved in communication.
//      val wholeChildren = cachedChild
//        .asInstanceOf[PullPairExchangeExec]
//        .children ++ uncachedChildren
//
//      val cardinalities = wholeChildren.map { child =>
//        (
//          child.outputOld,
//          child.statisticKeeper.rowCountOnlyStatistic().rowCount.toLong
//        )
//      }.toMap
//      val schemas = wholeChildren.map(_.outputOld)
//      val shareComputer = new EnumShareComputer(
//        schemas,
//        constraint,
//        SeccoConfiguration.newDefaultConf().numPartition,
//        cardinalities
//      )
//      val shareResults = shareComputer.optimalShareWithBudget()
//
//      shareResults.share.foreach { case (key, value) => share(key) = value }
//    }
//
//    //cache the output of children to avoid repeated materialization during pairing
//    if (cachedRDD.isEmpty) {
//      children.par.foreach(_.cacheOutput())
//      cachedRDD = cachedChild.cachedExecuteResult
//      cachedChild.cachedExecuteResult = None
//    } else {
//      uncachedChildren.foreach(_.cacheOutput())
//    }
//
//  }
//
//  /**
//    * Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
//    *
//    * Overridden by concrete implementations of SparkPlan.
//    */
//  override protected def doExecute(): RDD[InternalBlock] = {
//
//    //gen inputRDDs
//    val inputRDDs =
//      cachedRDD.get +: uncachedChildren.map(_.execute())
//
//    //gen partitions for PullPairRDD
//    val subTaskPartitions = genPullPairPartitions(
//      children,
//      inputRDDs
//    )
//
//    //gen PullPairRDD
//    val pairedRDD = new IterativePullPairRDD(
//      outputOld,
//      sparkContext,
//      orderRearrange,
//      subTaskPartitions,
//      taskPartitioner,
//      inputRDDs
//    )
//
//    pairedRDD
//  }
//
//  override protected def doCleanUp(): Unit = {
//    uncachedChildren.foreach { child =>
////      child.foreach { dolpinPlan =>
////        dolpinPlan.cachedExecuteResult match {
////          case Some(rdd) => rdd.unpersist(false)
////          case None      =>
////        }
////        dolpinPlan.cachedExecuteResult = None
////      }
//      val grandChildren = child.children
//      grandChildren.foreach { grandChild =>
//        grandChild.foreach { dolpinPlan =>
//          dolpinPlan.cachedExecuteResult match {
//            case Some(rdd) => rdd.unpersist(false)
//            case None      => {}
//          }
//          dolpinPlan.cachedExecuteResult = None
//        }
//      }
//
////      child.cachedExecuteResult match {
////        case Some(rdd) => rdd.unpersist(false)
////        case None      => {}
////      }
////      child.cachedExecuteResult = None
//    }
//  }
//}
//
//class IterativePullPairRDDPartition(
//    idx: Int,
//    dependencyBlockID: Seq[Int],
//    @transient private val rdds: Seq[RDD[_]]
//) extends Partition {
//  override val index: Int = idx
//
//  var partitionValues: Seq[(Int, Partition)] = dependencyBlockID.zipWithIndex
//    .map(_.swap)
//    .map(f => (f._1, rdds(f._1).partitions(f._2)))
//
//  def partitions: Seq[(Int, Partition)] = partitionValues
//
//  //calculate the preferedLocation for this parition, currently it choose the preferred location of first RDD as preferedLocation
//  def calculatePreferedLocation: Seq[String] = {
//
//    var prefs = dependencyBlockID
//      .slice(0, 1)
//      .zipWithIndex
//      .map(_.swap)
//      .map(f => rdds(f._1).preferredLocations(rdds(f._1).partitions(f._2)))
//    //The Prefed Location is very important for index reuse, however for some init input rdd's it may not have a prefered location,
//    //which can result error.
//
//    //find all blocks location using blockManager.master
//    if (prefs.flatten.distinct.forall(f => f == "")) {
//      val sparkEnv = SparkEnv.get
//      val blockManagerMaster = sparkEnv.blockManager.master
//      prefs = dependencyBlockID.slice(0, 1).zipWithIndex.map(_.swap).map { f =>
//        val blockId =
//          RDDBlockId(rdds(f._1).id, rdds(f._1).partitions(f._2).index)
//
//        blockManagerMaster
//          .getLocations(blockId)
//          .map(f => TaskLocation(f.host, f.executorId).toString)
//
//      }
//    }
//
//    //if no preference, then make all servers as preferred
//    if (prefs.flatten.distinct.forall(f => f == "")) {
//      val sparkEnv = SparkEnv.get
//      prefs = Seq(
//        sparkEnv.blockManager.master.getMemoryStatus.keys.toSeq
//          .map(f => TaskLocation(f.host, f.executorId).toString)
//      )
//    }
//
//    val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
//    val locs =
//      if (exactMatchLocations.nonEmpty) exactMatchLocations
//      else prefs.flatten.distinct
//
//    locs
//  }
//
//  @throws(classOf[IOException])
//  private def writeObject(oos: ObjectOutputStream): Unit =
//    Utils.tryOrIOException {
//      // Update the reference to parent split at the time of task serialization
//      var partitionValues = dependencyBlockID.zipWithIndex
//        .map(_.swap)
//        .map(f => (f._1, rdds(f._1).partitions(f._2)))
//      oos.defaultWriteObject()
//    }
//}
//
//class IterativePullPairRDD[A <: InternalBlock: Manifest](
//    output: Seq[String],
//    sc: SparkContext,
//    orderRearrange: Seq[Int],
//    pairedPartitions: Array[IterativePullPairRDDPartition],
//    _partitioner: Partitioner,
//    var rdds: Seq[RDD[InternalBlock]]
//) extends RDD[InternalBlock](sc, rdds.map(x => new OneToOneDependency(x))) {
//  override val partitioner: Option[Partitioner] = Some(_partitioner)
//
//  //reorder the subTaskPartitions according to their idx
//  override def getPartitions: Array[Partition] = {
//    pairedPartitions.asInstanceOf[Array[Partition]]
//  }
//
//  override def getPreferredLocations(s: Partition): Seq[String] = {
//    s.asInstanceOf[IterativePullPairRDDPartition].calculatePreferedLocation
//  }
//
//  override def clearDependencies(): Unit = {
//    super.clearDependencies()
//    rdds = null
//  }
//
//  override def compute(
//      split: Partition,
//      context: TaskContext
//  ): Iterator[InternalBlock] = {
//    val subTaskPartition = split.asInstanceOf[IterativePullPairRDDPartition]
//    val blockList = subTaskPartition.partitionValues.par
//      .map { f =>
//        val iterator1 = rdds(f._1).iterator(f._2, context)
//        val block = iterator1.next()
//        iterator1.hasNext
//        block
//      }
//      .toArray
//      .flatMap {
//        case mthb: MultiTableIndexedBlock => mthb.subBlocks
//        case b: InternalBlock             => Seq(b)
//      }
//
//    //we need to rearrange the position of block list based on orderRearrange
//    val rearrangedBlockList = orderRearrange.map(blockList)
//
//    val shareVector =
//      partitioner.get.asInstanceOf[PairPartitioner].getCoordinate(split.index)
//
//    Iterator(MultiTableIndexedBlock(output, shareVector, rearrangedBlockList))
//  }
//
//}
