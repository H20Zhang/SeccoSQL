package org.apache.spark.secco.execution.plan.computation

import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.optimization.plan.JoinType
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper
import org.apache.spark.secco.execution.SeccoPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.codegen.{
  CodeAndComment,
  CodeFormatter,
  CodeGenerator,
  CodegenContext,
  ExprCode
}
import org.apache.spark.secco.execution.plan.computation.localExec.LocalInputExec
import org.apache.spark.secco.execution.plan.computation.utils.BufferedRowIterator
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.{
  InternalPartition,
  PairedPartition
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{Attribute, BoundReference}
import org.apache.spark.secco.types.StructType
import org.apache.spark.secco.util.counter.Counter

import scala.collection.mutable

/** A local computation physical operator that performs a sequence of local computations in a stage.
  *
  * There are two mode for performing a local stage:
  *   a. Iterator Mode (Pull-Based Computation)
  *   a. Compilation Mode (Push-Based Computation)
  *
  * For Pull-Based Computation: XXX
  *
  * For Push-Based Computation:
  * It compiles a subtree of plans that support codegen together into single Java
  * function.
  *
  * `doCodeGen()` will create a `CodeGenContext`, which will hold a list of variables for input,
  * used to generated code for [[BoundReference]].
  *
  * Here is the call graph of to generate Java source
  *
  *   LocalStageExec       Plan A               InputPlan B
  * =============================================================
  *
  * -> doCodegen()
  *     |
  *     +----------------->   produce()
  *                             |
  *                          doProduce()  -------> produce()
  *                                                   |
  *                                                doProduce()
  *                                                   |
  *                         doConsume() <--------- consume()
  *                             |
  *  doConsume()  <--------  consume()
  *
  * SeccoPlan A and B should override `doProduce()` and `doConsume()`.
  *
  * @param child child physical operator
  * @param localExec local computations to be performed in this stage
  */
case class LocalStageExec(
    child: SeccoPlan,
    var localExec: LocalProcessingExec
)(val codegenStageId: Int)
    extends SeccoPlan
    with PushBasedCodegen {

  private val doPushBasedCodegen: Boolean = true

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

      if (!doPushBasedCodegen) {
        //iterator mode
        InternalPartition.fromInternalBlock(
          localExec.output,
          localExec.iteratorResults(),
          partition.coordinate,
          partition.partitioner
        )
      } else {
        // push-based codegen mode
        InternalPartition.fromInternalBlock(
          localExec.output,
          InternalBlock(
            executeWithCodeGen().toArray,
            StructType.fromAttributes(localExec.output)
          ),
          partition.coordinate,
          partition.partitioner
        )
      }

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

  /** Generates code for this subtree.
    *
    * @return the tuple of the codegen context and the actual generated source.
    */
  def doCodeGen(): (CodegenContext, CodeAndComment) = {
    val startTime = System.nanoTime()
    val ctx = new CodegenContext
    val code = localExec.asInstanceOf[PushBasedCodegen].produce(ctx, this)

    // main next function.
    ctx.addNewFunction(
      "processNext",
      s"""
        protected void processNext() throws java.io.IOException {
          System.out.println("in PushBasedCodegenExec, beginning...");
          ${code.trim}
        }
       """,
      inlineToOuterClass = true
    )

    val className = "GeneratedIterator"

    val source = s"""
      public Object generate(Object[] references) {
        return new $className(references);
      }

      ${ctx.registerComment(
      s"""Codegened pipeline for stage (id=$codegenStageId)
         |${this.treeString.trim}""".stripMargin,
      "wsc_codegenPipeline"
    )}
      ${ctx.registerComment(
      s"codegenStageId=$codegenStageId",
      "wsc_codegenStageId",
      true
    )}
      final class $className extends ${classOf[BufferedRowIterator].getName} {

        private Object[] references;
        private scala.collection.Iterator[] inputs;
        ${ctx.declareMutableStates()}

        public $className(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator[] inputs) {
          // partitionIndex = index;
          this.inputs = inputs;
          ${ctx.initMutableStates()}
          ${ctx.initPartition()}
          System.out.println("in PushBasedCodegenExec, init finished");
        }

        ${ctx.emitExtraCode()}

        ${ctx.declareAddedFunctions()}
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(
        CodeFormatter.stripExtraNewLines(source),
        ctx.getPlaceHolderToComments()
      )
    )

    val duration = System.nanoTime() - startTime
    PushBasedCodegenExec.increaseCodeGenTime(duration)

    logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    (ctx, cleanedSource)
  }

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }

  override def doConsume(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      row: ExprCode
  ): String = {
    val doCopy = if (needCopyResult) {
      ".copy()"
    } else {
      ""
    }
    s"""
       |${row.code}
       |System.out.println("in PushBasedCodegenExec, will append a row");
       |append(${row.value}.copy());
     """.stripMargin.trim
  }

  def executeWithCodeGen(): Iterator[InternalRow] = {
    val (ctx, cleanedSource) = doCodeGen()

    val (clazz, _) = CodeGenerator.compile(cleanedSource)

    val references = ctx.references.toArray
    val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
//    println("in executeWithCodeGen(), before calling inputRowIterators()")
    val rowIters = child.asInstanceOf[PushBasedCodegen].inputRowIterators()
//    println("in executeWithCodeGen(), before init")
    buffer.init(0, rowIters.toArray)
//    println("in executeWithCodeGen(), after init")
//    println(s"rowIters: $rowIters")
    //    var iterCount = 0
    //    for(iter <- rowIters){
    //      println(s"iter $iterCount in rowIters:")
    //      while(iter.hasNext)
    //        {
    //          println(s"iter $iterCount.next():" + iter.next())
    //        }
    //      iterCount += 1
    //    }
    new Iterator[InternalRow] {
      override def hasNext: Boolean = buffer.hasNext
      override def next: InternalRow = buffer.next
    }
  }

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      builder: StringBuilder,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false
  ): StringBuilder = {
    child.generateTreeString(depth, lastChildren, builder, verbose, "*")
  }

  override def needStopCheck: Boolean = true

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(
    codegenStageId.asInstanceOf[Integer]
  )

  protected def withNewChildInternal(
      newChild: SeccoPlan
  ): LocalStageExec =
    copy(child = newChild)(codegenStageId)

  override def inputRowIterators(): Seq[Iterator[InternalRow]] = {
    throw new UnsupportedOperationException
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
