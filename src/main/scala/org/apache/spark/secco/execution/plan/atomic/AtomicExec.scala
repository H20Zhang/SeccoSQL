package org.apache.spark.secco.execution.plan.atomic

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark._
import org.apache.spark.secco.catalog._
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class CacheExec(child: SeccoPlan) extends UnaryExecNode {

//  val view: CatalogView = CatalogView(output.map(CatalogColumn(_)))

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalBlock] = {
    cachedExecuteResult match {
      case Some(rdd) => rdd
      case None =>
        val rdd =
          child.execute().persist(seccoSession.sessionState.conf.rddCacheLevel)
        rdd.count()
        cachedExecuteResult = Some(rdd)
        rdd
    }
  }

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld
}

case class AssignExec(child: SeccoPlan, tableIdentifier: String)
    extends UnaryExecNode {

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalBlock] = ???
  //TODO: refactor below
//  {
//
//    dataManager(tableIdentifier) match {
//      case Some(childRDD) =>
//        val childRDD =
//          child.execute().persist(seccoSession.sessionState.conf.rddCacheLevel)
//        childRDD.count()
//
//        dataManager.storeRelation(tableIdentifier, childRDD)
//        childRDD
//
////        childRDD.asInstanceOf[RDD[InternalBlock]]
//      case None =>
//        val childRDD =
//          child.execute().persist(seccoSession.sessionState.conf.rddCacheLevel)
//        childRDD.count()
//
//        dataManager.storeRelation(tableIdentifier, childRDD)
//        childRDD
//    }
//
//  }

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld
}

case class RenameExec(child: SeccoPlan, attrRenameMap: Map[String, String])
    extends UnaryExecNode {

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalBlock] = {
    child.execute()
  }

  /** The output attributes */
  override def outputOld: Seq[String] = {
    child.outputOld.map(f => attrRenameMap.getOrElse(f, f))
  }
}

case class IterativeExec(
    child: SeccoPlan,
    returnTableIdentifier: String,
    numRun: Int = 10
) extends UnaryExecNode {

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalBlock] = {

    var i = 0
    var isChanged = true

    while (i < numRun && isChanged) {
      logInfo(s"do iteration-$i")
      val deltaRDD = child.execute()
      isChanged = !deltaRDD.map(f => f.isEmpty).collect().forall(identity)

      //clean up the cache
      children.foreach { child =>
        child.foreach { dolpinPlan =>
          dolpinPlan.cachedExecuteResult match {
            case Some(rdd) => rdd.unpersist(false)
            case None      => {}
          }
          dolpinPlan.cachedExecuteResult = None
        }
      }

      i += 1
    }

    //finalize the table of updateExec
    child.foreach {
      case child: UpdateExec => child.genFinalTable()
      case _                 =>
    }

    dataManager(returnTableIdentifier) match {
      case Some(childRDD) => childRDD.asInstanceOf[RDD[InternalBlock]]
      case None =>
        new Exception(s"No such returnTable:$returnTableIdentifier")
        sparkContext.emptyRDD
    }

  }

  /** The output attributes */
  override def outputOld: Seq[String] = {
    val catalog = seccoSession.sessionState.catalog
    catalog.getTable(returnTableIdentifier) match {
      case Some(table) => table.schema.map(_.columnName)
      case None =>
        throw new Exception(
          s"No such returnTableIdentifier:$returnTableIdentifier"
        )
    }
  }
}

case class UpdateExec(
    child: SeccoPlan,
    tableIdentifier: String,
    deltaTableName: String,
    key: Seq[String]
) extends UnaryExecNode {

  lazy val updateOp
      : (OldInternalDataType, OldInternalDataType) => OldInternalDataType =
    SeccoConfiguration.newDefaultConf().updateOp match {
      case "min" => Math.min
      case "max" => Math.max
    }

  private var hashMapRDDOption: Option[RDD[InternalBlock]] = None

  /** we assume first k attributes are key attributes */
  lazy val keyPos: Array[Int] = key.map(child.outputOld.indexOf).toArray

  /** we assume the attribute that follows key attributes is value attribute */
  lazy val valuePos: Int = keyPos.length
  lazy val partitioner =
    new KeyHashPartitioner(keyPos, outputOld.size, conf.numPartition)
  lazy val sentryRDD: RDD[(OldInternalRow, Boolean)] = genSentryRDD(
    conf.numPartition
  )

  def genSentryRDD(numPartitions: Int): RDD[(OldInternalRow, Boolean)] = {
    val sentry = Range(0, numPartitions).map { sentryId =>
      val arr = new Array[Double](outputOld.size + 1)
      val sentryPos = outputOld.size
      arr(sentryPos) = sentryId
      (arr, false)
    }.toArray
    val sentryRDD = sparkContext.parallelize(sentry)
    sentryRDD.cache().count()

    sentryRDD
  }

  def genPartitionRDD(
      rdd: RDD[InternalBlock],
      sentryRDD: RDD[(OldInternalRow, Boolean)]
  ): RDD[InternalBlock] = {
    val rowRDD = rdd.flatMap {
      case r: RowBlock => r.blockContent.content.map(f => (f, true))
      case _ =>
        throw new Exception("blockType not supported.")
    }

    val partitionedRDD =
      sparkContext
        .union(rowRDD, sentryRDD)
        .partitionBy(partitioner)
        .mapPartitions { it =>
          val content = it.filter(_._2).map(_._1).toArray
          Iterator(
            RowBlock(outputOld, RowBlockContent(content))
              .asInstanceOf[InternalBlock]
          )
        }

    partitionedRDD
  }

  def genHashMapRDD(rdd: RDD[InternalBlock]): RDD[InternalBlock] = {
    rdd.map {
      case r: RowBlock =>
        val content = r.blockContent.content
        val hashMap =
          mutable
            .HashMap[mutable.WrappedArray[
              OldInternalDataType
            ], OldInternalDataType]()
        content.foreach { row =>
          val keyArr = new Array[OldInternalDataType](keyPos.length)
          val keySize = keyPos.length
          var i = 0
          while (i < keySize) {
            keyArr(i) = row(keyPos(i))
            i += 1
          }
          val key = mutable.WrappedArray.make[OldInternalDataType](keyArr)
          val value = row(valuePos)
          hashMap(key) = value
        }
        GeneralBlock(outputOld, GeneralBlockContent(hashMap))
      case _ => throw new Exception("blockType not supported.")
    }
  }

  def genZippedRDD(
      rdd1: RDD[InternalBlock],
      rdd2: RDD[InternalBlock]
  ): RDD[InternalBlock] = {
    val zippedRDD = new UpdateRDD(sparkContext, Seq(rdd1, rdd2))
    zippedRDD
  }

  def update(
      zippedRDD: RDD[InternalBlock]
  ): (RDD[InternalBlock], RDD[InternalBlock]) = {

    /** cache zippedRDD */
//    zippedRDD.cache().count()

    val diffRDD = zippedRDD.map {
      case m: MultiBlock =>
        val block0 = m
          .subBlocks(0)
          .asInstanceOf[GeneralBlock[
            mutable.HashMap[mutable.WrappedArray[
              OldInternalDataType
            ], OldInternalDataType]
          ]]
        val block1 = m.subBlocks(1).asInstanceOf[RowBlock]

        val hashMap = block0.blockContent.content
        val rows = block1.blockContent.content

        val diffRows = rows.filter { row =>
          val keyArr = new Array[OldInternalDataType](keyPos.size)
          val keyPosSize = keyPos.length
          var i = 0
          while (i < keyPosSize) {
            keyArr(i) = row(keyPos(i))
            i += 1
          }
          val key = mutable.WrappedArray.make[OldInternalDataType](keyArr)
          hashMap.get(key) match {
            case Some(value) =>
              if (updateOp(value, row(valuePos)) != value) {
                true
              } else {
                false
              }
//              Math.abs(value - row(valuePos)) > conf.SMALL_CHANGE
            case None => true
          }
        }

        RowBlock(outputOld, RowBlockContent(diffRows))
          .asInstanceOf[InternalBlock]
      case _ => throw new Exception("blockType not supported")
    }

    val newHashMapRDD = zippedRDD.map {
      case m: MultiBlock =>
        val block0 = m
          .subBlocks(0)
          .asInstanceOf[GeneralBlock[
            mutable.HashMap[mutable.WrappedArray[
              OldInternalDataType
            ], OldInternalDataType]
          ]]
        val block1 = m.subBlocks(1).asInstanceOf[RowBlock]

        val hashMap = block0.blockContent.content
        val rows = block1.blockContent.content
        val keyPosSize = keyPos.length

        rows.foreach { row =>
          val keyArr = new Array[OldInternalDataType](keyPosSize)

          var i = 0
          while (i < keyPosSize) {
            keyArr(i) = row(keyPos(i))
            i += 1
          }
          val key = mutable.WrappedArray.make[OldInternalDataType](keyArr)

          hashMap.get(key) match {
            case Some(value) =>
              if (updateOp(value, row(valuePos)) != value) {
                hashMap(key) = row(valuePos)
              }
            case None => hashMap(key) = row(valuePos)
          }
        }

        block0.asInstanceOf[InternalBlock]
      case _ => throw new Exception("blockType not supported")
    }

    /** uncache zippedRDD */
//    zippedRDD.unpersist(false)

    (newHashMapRDD, diffRDD)
  }

  def genFinalTable(): Unit = ???
//  TODO: refactor below
//  {
//    val outputRDD = hashMapRDDOption match {
//      case Some(hashMapRDD) =>
//        hashMapRDD.map {
//          case b: GeneralBlock[
//                mutable.HashMap[mutable.WrappedArray[
//                  OldInternalDataType
//                ], OldInternalDataType]
//              ] =>
//            val hashMap = b.blockContent.content
//            val mutableArray = ArrayBuffer[OldInternalRow]()
//            val keyPosSize = keyPos.length
//            hashMap.foreach { case (key, value) =>
//              val res = new Array[OldInternalDataType](outputOld.size)
//              var i = 0
//              while (i < keyPosSize) {
//                res(i) = key(i)
//                i += 1
//              }
//              res(keyPosSize) = value
//              mutableArray += res
//            }
//            val content = mutableArray.toArray
//            RowBlock(outputOld, RowBlockContent(content))
//              .asInstanceOf[InternalBlock]
//          case _ => throw new Exception("blockType not supported")
//        }
//      case None =>
//        throw new Exception(
//          "hashMapRDD shouldn't be empty when calling genFinalTable"
//        )
//    }
//
////    outputRDD.cache().count()
//    dataManager.storeRelation(tableIdentifier, outputRDD)
//  }

  private var iter = 0
  private var checkpointRDDOption: Option[RDD[InternalBlock]] = None

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalBlock] = ???
  //TODO: refactor below
//  {
//    val childRDD = child.execute()
//
//    //TODO: find out if caching is needed
//
////      .cache()
//    childRDD.count()
//
//    val diffRDD = hashMapRDDOption match {
//      case Some(hashMapRDD) =>
//        val partitionRDD = genPartitionRDD(childRDD, sentryRDD)
//        val zippedRDD = genZippedRDD(hashMapRDD, partitionRDD)
//        val (newHashMapRDD, diffRDD) = update(zippedRDD)
//
////        iter += 1
////        if (iter % 5 == 0) {
//////          newHashMapRDD.cache().count()
////          val oldCheckpointRDDOption = checkpointRDDOption
////
////          checkpointRDDOption = Some(newHashMapRDD.localCheckpoint())
////          checkpointRDDOption.get.count()
////
////          oldCheckpointRDDOption match {
////            case Some(x) =>
////              checkpointRDDOption.get.unpersist(false)
////            case None =>
////          }
////        } else {
//
////        diffRDD.cache().count()
////        }
//
//        //store the diffRDD in deltaTable
//        dataManager.storeRelation(deltaTableName, diffRDD)
//
//        newHashMapRDD.cache().count()
//        hashMapRDDOption = Some(newHashMapRDD)
//        hashMapRDD.unpersist(false)
//
//        diffRDD
//      case None =>
//        val partitionRDD = genPartitionRDD(childRDD, sentryRDD)
//        val hashMapRDD = genHashMapRDD(partitionRDD)
//
//        hashMapRDD.cache().count()
//        hashMapRDDOption = Some(hashMapRDD)
//        val diffRDD = childRDD
//
//        //store the diffRDD in deltaTable
//        dataManager.storeRelation(deltaTableName, diffRDD)
//
//        diffRDD
//
//    }
//
//    childRDD.unpersist(false)
//
//    diffRDD
//  }

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld
}

/** partition the [[OldInternalRow]] based on [[keyPos]] */
class KeyHashPartitioner(keyPos: Seq[Int], sentryPos: Int, _numPartitions: Int)
    extends Partitioner {

  val hashPartitioner = new HashPartitioner(numPartitions)
  val underlyingArray = new Array[OldInternalDataType](keyPos.size)
  val wrappedArray: mutable.WrappedArray[OldInternalDataType] =
    mutable.WrappedArray.make[OldInternalDataType](underlyingArray)
  val keyPosArray: Array[Int] = keyPos.toArray
  val keyPosArraySize: Int = keyPos.size

  override def numPartitions: Int = _numPartitions

  override def getPartition(key: Any): Int = {
    key match {
      case row: OldInternalRow if row.length <= sentryPos =>
        var i = 0
        while (i < keyPosArraySize) {
          underlyingArray(i) = row(i)
          i += 1
        }
        hashPartitioner.getPartition(wrappedArray)
      case row: OldInternalRow if row.length == (sentryPos + 1) =>
        row(sentryPos).toInt
    }
  }

}

class UpdateRDDPartition(
    idx: Int,
    @transient private val rdds: Seq[RDD[_]]
) extends Partition {
  override val index: Int = idx
  var partitionValues: Seq[Partition] = rdds.map(rdd => rdd.partitions(idx))
  def partitions: Seq[Partition] = partitionValues
  val dependencyBlockID: Seq[Int] = rdds.map(f => idx)

  //calculate the preferredLocation for this parition, currently it choose the preferred location of first RDD as preferedLocation
  def calculatePreferedLocation = {

    var prefs = dependencyBlockID
      .slice(0, 1)
      .zipWithIndex
      .map(_.swap)
      .map(f => rdds(f._1).preferredLocations(rdds(f._1).partitions(f._2)))
    //The Preferred Location is very important for index reuse, however for some init input rdd's it may not have a prefered location,
    //which can result error.

    //find all blocks location using blockManager.master
    if (prefs.flatten.distinct.forall(f => f == "")) {
      val sparkEnv = SparkEnv.get
      val blockManagerMaster = sparkEnv.blockManager.master
      prefs = dependencyBlockID.slice(0, 1).zipWithIndex.map(_.swap).map { f =>
        val blockId =
          RDDBlockId(rdds(f._1).id, rdds(f._1).partitions(f._2).index)

        blockManagerMaster
          .getLocations(blockId)
          .map(f => TaskLocation(f.host, f.executorId).toString)

      }
    }

    //if no preference, then make all servers as preferred
    if (prefs.flatten.distinct.forall(f => f == "")) {
      val sparkEnv = SparkEnv.get
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
      partitionValues = rdds.map(rdd => rdd.partitions(idx))
      oos.defaultWriteObject()
    }
}

class UpdateRDD(
    sc: SparkContext,
    var rdds: Seq[RDD[InternalBlock]]
) extends RDD[InternalBlock](sc, rdds.map(x => new OneToOneDependency(x))) {
  override val partitioner: Option[Partitioner] = rdds(0).partitioner

  assert(rdds.size == 2, "size of rdds should be 2 in UpdateRDD")

  //reorder the subTaskPartitions according to their idx
  override def getPartitions: Array[Partition] = {
    val numParts = rdds.head.partitions.length
    if (!rdds.forall(rdd => rdd.partitions.length == numParts)) {
      throw new IllegalArgumentException(
        s"Can't zip RDDs with unequal numbers of partitions: ${rdds.map(_.partitions.length)}"
      )
    }
    Array.tabulate[Partition](numParts) { i =>
      new UpdateRDDPartition(i, rdds)
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[UpdateRDDPartition].calculatePreferedLocation
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }

  override def compute(
      split: Partition,
      context: TaskContext
  ): Iterator[MultiBlock] = {
    val partitions = split.asInstanceOf[UpdateRDDPartition].partitions
    val rdd1 = rdds(0)
    val rdd2 = rdds(1)

    val it1 = rdd1.iterator(partitions(0), context)
    val it2 = rdd2.iterator(partitions(1), context)

    it1.zip(it2).map { case (block1, block2) =>
      MultiBlock(block1.output, Seq(block1, block2))
    }
  }

}
