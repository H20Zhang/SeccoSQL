package org.apache.spark.secco.execution.plan.computation

import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.optimization.plan.JoinType
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper
import org.apache.spark.secco.execution.SeccoPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.execution.storage.{
  InternalPartition,
  PairedPartition
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

import scala.collection.mutable

/** A local computation physical operator that performs a sequence of local computations in a stage.
  * @param child child physical operator
  * @param localExec local computations to be performed in this stage
  */
case class LocalStageExec(
    child: SeccoPlan,
    var localExec: LocalProcessingExec
) extends SeccoPlan {

  override def output: Seq[Attribute] = localExec.output

  override protected def doPrepare(): Unit = {}

  override protected def doExecute(): RDD[InternalPartition] = {

//    logInfo(s"""
//         |== Begin Local Computation Stage ==
//         | ${simpleString}
//         | """.stripMargin)
    child.execute().map { partition =>
      localExec.foreach { exec =>
        if (exec.isInstanceOf[LocalInputExec]) {
          exec
            .asInstanceOf[LocalInputExec]
            .setLocalPartition(partition)
        }
      }

      //non-codegen mode
      InternalPartition.fromInternalBlock(
        localExec.output,
        localExec.result(),
        partition.coordinate,
        partition.partitioner
      )

    //TODO: implement codegen mode
    }
  }

  override protected def doRDD(): RDD[InternalRow] = {

    execute().flatMap { p =>
      p match {
        case p: PairedPartition =>
          throw new Exception(
            s"${p.getClass} cannot be serialized to InternalRow."
          )
        case t: InternalPartition => t.data.head.iterator
      }
    }

  }

  override def children: Seq[SeccoPlan] = Seq(child)

  override def argString: String = {
    s"[${localExec.relationalString}]"
  }
}

//TODO: add optimization rules to optimize Local Stage.
///** The attribute order computer that compute the attribute order that minimize the computation cost for localPlan. */
//@transient private lazy val attributeOrderComputer =
//new AttributeOrderComputer(
//localExec,
//sharedAttributeOrder.res,
//getStatisticKeeperOfPlaceHolder()
//)
//
///** The localPlan optimized by [[AttributeOrderComputer]]. */
//private var processedLocalPlan: Option[LocalProcessingExec] = None
//
//  /** The underlying shared attribute order. */
//  def attributeOrder: mutable.ArrayBuffer[String] = sharedAttributeOrder.res
//
//  /** Get the statistics of the place holder based on children's statistic */
//  def getStatisticKeeperOfPlaceHolder(): Seq[StatisticKeeper] = {
//  child match {
//  case s: InMemoryScanExec =>
//  Seq(s.statisticKeeper)
//  case s: DiskScanExec =>
//  Seq(s.statisticKeeper)
//  case c: LocalPreparationExec =>
//  Seq(c.statisticKeeper)
//  case pExec: PullPairExchangeExec =>
//  pExec.children.map(_.statisticKeeper)
//  case ipExec: IterativePairExchangeExec =>
//  val unarrangedStatistics = ipExec.children
//  .flatMap { f =>
//  f match {
//  case pExec: PullPairExchangeExec =>
//  pExec.children
//  case p: PartitionExchangeExec => Seq(p)
//  case c: LocalPreparationExec  => Seq(c)
//  }
//  }
//  .map(_.statisticKeeper)
//  val reArrangedStatistics =
//  ipExec.orderRearrange.map(unarrangedStatistics)
//  reArrangedStatistics
//  }
//  }
//
//  /** Assign Preprocessing based on `processedLocalPlan`. */
//  def assignPreprocessingTasks(): Unit = {
//  val _processedLocalPlan = processedLocalPlan.get
//
//  // all execWithPreprocessing
//  val execWithPreprocessing = _processedLocalPlan.collect {
//  case j @ LocalJoinExec(
//    _,
//    JoinType.GHDFKFK,
//    _
//  ) =>
//  j
//  case p: LocalProjectExec =>
//  p
//  case a: LocalSemiringAggregateExec =>
//  a
//  }
//
//  // functions for checking if a FKFKJoin is the below some FKFKJoin
//  def isChildOfExecWithPreprocessing(j: LocalProcessingExec): Boolean = {
//  if (execWithPreprocessing.diff(Seq(j)).exists(_.find(_ == j).nonEmpty)) {
//  true
//  } else {
//  false
//  }
//  }
//
//  // determine the preprocessing to perform for binary join and GHD join.
//  _processedLocalPlan.foreach { plan =>
//  plan match {
//  case j @ LocalJoinExec(
//    children,
//    _,
//    _
//  )
//  if j.joinType == JoinType.GHDFKFK || j.joinType == JoinType.PKFK =>
//  val base = children(0)
//  val index = children(1)
//  base match {
//  case exec: LocalPlaceHolderExec
//  if !isChildOfExecWithPreprocessing(j) =>
//  val pos = exec.pos
//  if (
//  SeccoConfiguration
//  .newDefaultConf()
//  .enableLocalPreprocessingOptimization
//  ) {
//  sharedPreparationTasks(
//  pos
//  ).res += PreparationTask.ConstructTrie
//  } else {
//  sharedPreparationTasks(
//  pos
//  ).res += PreparationTask.ConstructTrie
//  }
//  case exec: LocalPlaceHolderExec
//  if isChildOfExecWithPreprocessing(j) =>
//  val pos = exec.pos
//  if (
//  SeccoConfiguration
//  .newDefaultConf()
//  .enableLocalPreprocessingOptimization
//  ) {
//  sharedPreparationTasks(
//  pos
//  ).res += PreparationTask.ConstructTrie
//  } else {
//  sharedPreparationTasks(
//  pos
//  ).res += PreparationTask.ConstructTrie
//  }
//  case _ =>
//  }
//  index match {
//  case exec: LocalPlaceHolderExec =>
//  val pos = exec.pos
//  val joinAttributes =
//  base.outputOld.intersect(index.outputOld).toArray
//  if (
//  SeccoConfiguration
//  .newDefaultConf()
//  .enableLocalPreprocessingOptimization
//  ) {
//  //                sharedPreparationTasks(
//  //                  pos
//  //                ).res += PreparationTask.ConstructHashMap(
//  //                  joinAttributes
//  //                )
//  sharedPreparationTasks(
//  pos
//  ).res += PreparationTask.ConstructTrie
//  } else {
//  sharedPreparationTasks(
//  pos
//  ).res += PreparationTask.ConstructTrie
//  }
//  case _ =>
//  }
//  case LocalJoinExec(children, JoinType.GHD, sharedAttributeOrder) =>
//  children.foreach { child =>
//  val pos = child.asInstanceOf[LocalPlaceHolderExec].pos
//  sharedPreparationTasks(pos).res += PreparationTask.ConstructTrie
//  }
//  case placeHolder: LocalPlaceHolderExec
//  if isChildOfExecWithPreprocessing(placeHolder) =>
//  val pos = placeHolder.pos
//
//  if (sharedPreparationTasks(pos).res.isEmpty) {
//  sharedPreparationTasks(pos).res += PreparationTask.ConstructTrie
//  }
//
//  //          println(s"[debug] localPlaceExec:${placeHolder}")
//
//  //          children.foreach { child =>
//  //            if (child.isInstanceOf[LocalPlaceHolderExec]) {
//  //              val pos = child.asInstanceOf[LocalPlaceHolderExec].pos
//  //
//  //            }
//  //          }
//  case _ =>
//  }
//  }
//
//  // set the default preprocessing tasks.
//  sharedPreparationTasks.foreach { sharedPreparationTask =>
//  if (sharedPreparationTask.res.isEmpty) {
//  if (
//  SeccoConfiguration
//  .newDefaultConf()
//  .enableLocalPreprocessingOptimization
//  ) {
//  sharedPreparationTask.res += PreparationTask.Sort
//  } else {
//  sharedPreparationTask.res += PreparationTask.ConstructTrie
//  }
//  }
//  }
//  }
//
//  /** Compute the `processedLocalPlan` */
//  def computeProcessedLocalPlan(): Unit = {
//  if (processedLocalPlan.isEmpty) {
//  processedLocalPlan = Some(
//  attributeOrderComputer.genProcessedLocalPlan()
//  )
//  }
//  }
