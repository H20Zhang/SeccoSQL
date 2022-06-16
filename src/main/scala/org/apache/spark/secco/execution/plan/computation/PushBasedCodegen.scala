package org.apache.spark.secco.execution.plan.computation

import org.apache.spark.rdd.RDD
import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen._
import org.apache.spark.secco.execution.plan.computation.deprecated.{
  HashJoinExec,
  LeapFrogJoinExec
}
import org.apache.spark.secco.execution.plan.computation.newIter.SeccoIterator
import org.apache.spark.secco.execution.plan.computation.utils.BufferedRowIterator
import org.apache.spark.secco.execution.storage.InternalPartition
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.{
  InternalRow,
  UnsafeInternalRow
}
import org.apache.spark.secco.execution.{SeccoPlan, UnaryExecNode}
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.codegen.GenerateUnsafeProjection
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.types._

import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

trait PushBasedCodegen extends SeccoPlan {

//  def variablePrefix: String
  //  /** Prefix used in the current operator's variable names. */
  protected def variablePrefix: String = this match {
    case _: PushBasedCodegenExec => "wholestagecodegen"
    case _                       => nodeName.toLowerCase(Locale.ROOT)
  }

//  def inputRowIterator(): Iterator[InternalRow]
  def inputRowIterators(): Seq[Iterator[InternalRow]]

  /** Whether this SparkPlan supports whole stage codegen or not.
    */
  def supportCodegen: Boolean = true

  /** Which SparkPlan is calling produce() of this one. It's itself for the first SparkPlan.
    */
  protected var parent: PushBasedCodegen = null

  /** Returns Java source code to process the rows from input RDD.
    */
  final def produce(ctx: CodegenContext, parent: PushBasedCodegen): String = {
    this.parent = parent
    ctx.freshNamePrefix = variablePrefix
    s"""
       |${ctx.registerComment(s"PRODUCE: ${this.simpleString}")}
       |${doProduce(ctx)}
     """.stripMargin
  }

  /** Generate the Java source code to process, should be overridden by subclass to support codegen.
    *
    * doProduce() usually generate the framework, for example, aggregation could generate this:
    *
    *   if (!initialized) {
    *     # create a hash map, then build the aggregation hash map
    *     # call child.produce()
    *     initialized = true;
    *   }
    *   while (hashmap.hasNext()) {
    *     row = hashmap.next();
    *     # build the aggregation results
    *     # create variables for results
    *     # call consume(), which will call parent.doConsume()
    *      if (shouldStop()) return;
    *   }
    */
  protected def doProduce(ctx: CodegenContext): String

  private def prepareRowVar(
      ctx: CodegenContext,
      row: String,
      colVars: Seq[ExprCode]
  ): ExprCode = {
    if (row != null) {
      ExprCode.forNonNullValue(
        JavaCode.variable(row, classOf[UnsafeInternalRow])
      )
    } else {
      if (colVars.nonEmpty) {
        val colExprs = output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable)
        }
        val evaluateInputs = evaluateVariables(colVars)
        // generate the code to create a UnsafeInternalRow
        ctx.INPUT_ROW = row
        ctx.currentVars = colVars
        val ev = GenerateUnsafeProjection.createCode(ctx, colExprs, false)
        // lgh: finished: Import GenerateUnsafeProjection to our project.
        val code = code"""
                         |$evaluateInputs
                         |${ev.code}
         """.stripMargin
        ExprCode(code, FalseLiteralValue, ev.value)
      } else {
        // There are no columns
        ExprCode.forNonNullValue(
          JavaCode.variable("UnsafeInternalRow", classOf[UnsafeInternalRow])
        )
      }
    }
  }

  /** Consume the generated columns or row from current SparkPlan, call its parent's `doConsume()`.
    *
    * Note that `outputVars` and `row` can't both be null.
    */
  final def consume(
      ctx: CodegenContext,
      outputVars: Seq[ExprCode],
      row: String = null
  ): String = {
    val inputVarsCandidate =
      if (outputVars != null) {
        assert(outputVars.length == output.length)
        // outputVars will be used to generate the code for UnsafeInternalRow, so we should copy them
        outputVars.map(_.copy())
      } else {
        assert(row != null, "outputVars and row cannot both be null.")
        ctx.currentVars = null
        ctx.INPUT_ROW = row
        output.zipWithIndex.map { case (attr, i) =>
          BoundReference(i, attr.dataType, attr.nullable).genCode(ctx)
        }
      }

    val inputVars = inputVarsCandidate match {
      case stream: Stream[ExprCode] => stream.force
      case other                    => other
    }

    val rowVar = prepareRowVar(ctx, row, outputVars)

    // Set up the `currentVars` in the codegen context, as we generate the code of `inputVars`
    // before calling `parent.doConsume`. We can't set up `INPUT_ROW`, because parent needs to
    // generate code of `rowVar` manually.
    ctx.currentVars = inputVars
    ctx.INPUT_ROW = null
    ctx.freshNamePrefix = parent.variablePrefix
    val evaluated =
      evaluateRequiredVariables(output, inputVars, parent.usedInputs)

    s"""
       |${ctx.registerComment(s"CONSUME: ${parent.simpleString}")}
       |$evaluated
       |${parent.doConsume(ctx, inputVars, rowVar)}
     """.stripMargin
  }

  /** Returns source code to evaluate all the variables, and clear the code of them, to prevent
    * them to be evaluated twice.
    */
  protected def evaluateVariables(variables: Seq[ExprCode]): String = {
    val evaluate =
      variables.filter(_.code.nonEmpty).map(_.code.toString).mkString("\n")
    variables.foreach(_.code = EmptyBlock)
    evaluate
  }

  /** Returns source code to evaluate the variables for required attributes, and clear the code
    * of evaluated variables, to prevent them to be evaluated twice.
    */
  protected def evaluateRequiredVariables(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      required: AttributeSet
  ): String = {
    val evaluateVars = new StringBuilder
    variables.zipWithIndex.foreach { case (ev, i) =>
      if (ev.code.nonEmpty && required.contains(attributes(i))) {
        evaluateVars.append(ev.code.toString + "\n")
        ev.code = EmptyBlock
      }
    }
    evaluateVars.toString()
  }

  /** Returns source code to evaluate the variables for non-deterministic expressions, and clear the
    * code of evaluated variables, to prevent them to be evaluated twice.
    */
  protected def evaluateNondeterministicVariables(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode],
      expressions: Seq[NamedExpression]
  ): String = {
    val nondeterministicAttrs =
      expressions.filterNot(_.deterministic).map(_.toAttribute)
    evaluateRequiredVariables(
      attributes,
      variables,
      AttributeSet(nondeterministicAttrs)
    )
  }

  /** The subset of inputSet those should be evaluated before this plan.
    *
    * We will use this to insert some code to access those columns that are actually used by current
    * plan before calling doConsume().
    */
  def usedInputs: AttributeSet = references

  /** Generate the Java source code to process the rows from child SparkPlan. This should only be
    * called from `consume`.
    *
    * This should be override by subclass to support codegen.
    *
    * Note: The operator should not assume the existence of an outer processing loop,
    *       which it can jump from with "continue;"!
    *
    * For example, filter could generate this:
    *   # code to evaluate the predicate expression, result is isNull1 and value2
    *   if (!isNull1 && value2) {
    *     # call consume(), which will call parent.doConsume()
    *   }
    *
    * Note: A plan can either consume the rows as UnsafeInternalRow (row), or a list of variables (input).
    *       When consuming as a listing of variables, the code to produce the input is already
    *       generated and `CodegenContext.currentVars` is already set. When consuming as UnsafeInternalRow,
    *       implementations need to put `row.code` in the generated code and set
    *       `CodegenContext.INPUT_ROW` manually. Some plans may need more tweaks as they have
    *       different inputs(join build side, aggregate buffer, etc.), or other special cases.
    */
  def doConsume(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      row: ExprCode
  ): String = {
    throw new UnsupportedOperationException
  }

  /** Whether or not the result rows of this operator should be copied before putting into a buffer.
    *
    * If any operator inside WholeStageCodegen generate multiple rows from a single row (for
    * example, Join), this should be true.
    *
    * If an operator starts a new pipeline, this should be false.
    */
  def needCopyResult: Boolean = {
    if (children.isEmpty) {
      false
    } else if (this.isInstanceOf[LeapFrogJoinExec]) { // Added by lgh
      true
    } else if (this.isInstanceOf[HashJoinExec]) { // Added by lgh
      true
    } else if (children.length == 1) {
      children.head.asInstanceOf[PushBasedCodegen].needCopyResult
    } else {
      throw new UnsupportedOperationException
    }
  }

  /** Whether or not the children of this operator should generate a stop check when consuming input
    * rows. This is used to suppress shouldStop() in a loop of WholeStageCodegen.
    *
    * This should be false if an operator starts a new pipeline, which means it consumes all rows
    * produced by children but doesn't output row to buffer by calling append(),  so the children
    * don't require shouldStop() in the loop of producing rows.
    */
  def needStopCheck: Boolean = parent.needStopCheck

  /** Helper default should stop check code.
    */
  def shouldStopCheckCode: String = if (needStopCheck) {
    "if (shouldStop()) return;"
  } else {
    "// shouldStop check is eliminated"
  }
}

trait BuildExecPushBasedCodegen extends UnaryExecNode with PushBasedCodegen {

  override def output: Seq[Attribute] = child.output

  override def doProduce(ctx: CodegenContext): String = {
    throw new UnsupportedOperationException
  }

  /** Returns Java source code to process the rows from input,
    * and the variable term that stores the result.
    */
  final def produceBulk(
      ctx: CodegenContext,
      parent: PushBasedCodegen
  ): (String, String) = {
    this.parent = parent
    ctx.freshNamePrefix = variablePrefix
    var resultTerm: String = null
    val codeStr =
      s"""
       |${ctx.registerComment(s"PRODUCE_BULK: ${this.simpleString}")}
       |${
        val (codeStrInner, resultTermInner) = doProduceBulk(ctx);
        resultTerm = resultTermInner; codeStrInner
      }
     """.stripMargin
    (codeStr, resultTerm)
  }

  protected def doProduceBulk(context: CodegenContext): (String, String)

}

object PushBasedCodegenExec {
  val PIPELINE_DURATION_METRIC = "duration"

  private def numOfNestedFields(dataType: DataType): Int = dataType match {
    case dt: StructType => dt.fields.map(f => numOfNestedFields(f.dataType)).sum
    case _              => 1
  }

  // The whole-stage codegen generates Java code on the driver side and sends it to the Executors
  // for compilation and execution. The whole-stage codegen can bring significant performance
  // improvements with large dataset in distributed environments. However, in the test environment,
  // due to the small amount of data, the time to generate Java code takes up a major part of the
  // entire runtime. So we summarize the total code generation time and output it to the execution
  // log for easy analysis and view.
  private val _codeGenTime = new AtomicLong

  // Increase the total generation time of Java source code in nanoseconds.
  // Visible for testing
  def increaseCodeGenTime(time: Long): Unit = _codeGenTime.addAndGet(time)

  // Returns the total generation time of Java source code in nanoseconds.
  // Visible for testing
  def codeGenTime: Long = _codeGenTime.get

  // Reset generation time of Java source code.
  // Visible for testing
  def resetCodeGenTime(): Unit = _codeGenTime.set(0L)
}

/** WholeStageCodegen compiles a subtree of plans that support codegen together into single Java
  * function.
  *
  * `doCodeGen()` will create a `CodeGenContext`, which will hold a list of variables for input,
  * used to generated code for [[BoundReference]].
  *
  * Here is the call graph of to generate Java source
  *
  *   WholeStageCodegen       Plan A               InputPlan B
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
  */
case class PushBasedCodegenExec(child: SeccoPlan)(val codegenStageId: Int)
    extends UnaryExecNode
    with PushBasedCodegen {

  override def output: Seq[Attribute] = child.output

  override def nodeName: String = s"WholeStageCodegen (${codegenStageId})"

  /** Generates code for this subtree.
    *
    * @return the tuple of the codegen context and the actual generated source.
    */
  def doCodeGen(): (CodegenContext, CodeAndComment) = {
    val startTime = System.nanoTime()
    val ctx = new CodegenContext
    val code = child.asInstanceOf[PushBasedCodegen].produce(ctx, this)

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
    println("in executeWithCodeGen(), before calling inputRowIterators()")
    val rowIters = child.asInstanceOf[PushBasedCodegen].inputRowIterators()
    println("in executeWithCodeGen(), before init")
    buffer.init(0, rowIters.toArray)
    println("in executeWithCodeGen(), after init")
    println(s"rowIters: $rowIters")
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
  ): PushBasedCodegenExec =
    copy(child = newChild)(codegenStageId)

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalPartition] = ???

  override def inputRowIterators(): Seq[Iterator[InternalRow]] = {
    throw new UnsupportedOperationException
  }
}
