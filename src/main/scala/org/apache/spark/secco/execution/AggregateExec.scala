package org.apache.spark.secco.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen._
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.BindReferences.bindReferences
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.aggregate._
import org.apache.spark.secco.expression.codegen.GenerateUnsafeProjection
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.types.StructType

import java.util
import scala.collection.mutable

/**
  * Hash-based aggregate operator.
  */
case class AggregateExec(groupingExpressions: Seq[NamedExpression],
                         aggregateFunctions: Array[AggregateFunction], /* Only support DeclarativeAggregate */
                         child: SeccoPlan)
  extends PushBasedCodegen {

//  require(HashAggregateExec.supportsAggregate(aggregateBufferAttributes))

  private val aggregateAttributes = aggregateFunctions.map(item => AttributeReference(item.prettyName, item.dataType)())
  private val aggregateBufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes).toSeq

  lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
      aggregateFunctions.flatMap(_.inputAggBufferAttributes)

  override lazy val output: Seq[Attribute] =
    aggregateFunctions.map { func =>
      val expr = func.asInstanceOf[DeclarativeAggregate].evaluateExpression
      AttributeReference(expr.prettyName, expr.dataType, expr.nullable)()
    }

  protected override def doExecute(): RDD[OldInternalBlock] = ???

  // all the mode of aggregate expressions
//  private val modes = aggregateExpressions.map(_.mode).distinct

  override def usedInputs: AttributeSet = inputSet

  override def supportCodegen: Boolean = {
    // ImperativeAggregate are not supported right now
    !aggregateFunctions.exists(_.isInstanceOf[ImperativeAggregate])
  }

//  override def inputRDDs(): Seq[RDD[InternalRow]] = {
//    child.asInstanceOf[PushBasedCodegen].inputRDDs()
//  }

  /**
    * Generate the code for output.
    * @return function name for the result code.
    */
  private def generateResultFunction(ctx: CodegenContext): String = {
    val funcName = ctx.freshName("doAggregateWithKeysOutput")
    val keyTerm = ctx.freshName("keyTerm")
    val bufferTerm = ctx.freshName("bufferTerm")

    val body =
      if (aggregateFunctions.nonEmpty) {
        // generate output using resultExpressions
        ctx.currentVars = null
        ctx.INPUT_ROW = keyTerm
        val keyVars = groupingExpressions.zipWithIndex.map { case (e, i) =>
          BoundReference(i, e.dataType, e.nullable).genCode(ctx)
        }
        val evaluateKeyVars = evaluateVariables(keyVars)
        ctx.INPUT_ROW = bufferTerm
        val bufferVars = aggregateBufferAttributes.zipWithIndex.map { case (e, i) =>
          BoundReference(i, e.dataType, e.nullable).genCode(ctx)
        }
        val evaluateBufferVars = evaluateVariables(bufferVars)
        // evaluate the aggregation result
        ctx.currentVars = bufferVars
        val aggResults = bindReferences(
          declFunctions.map(_.evaluateExpression),
          aggregateBufferAttributes).map(_.genCode(ctx))
        val evaluateAggResults = evaluateVariables(aggResults)
//        // generate the final result
//        ctx.currentVars = keyVars ++ aggResults
//        val inputAttrs = groupingAttributes ++ aggregateAttributes
//        val resultVars = bindReferences[Expression](
//          resultExpressions,
//          inputAttrs).map(_.genCode(ctx))
//        val evaluateNondeterministicResults =
//          evaluateNondeterministicVariables(output, resultVars, resultExpressions)
        s"""
           |$evaluateKeyVars
           |$evaluateBufferVars
           |$evaluateAggResults
           |${consume(ctx, aggResults)}
       """.stripMargin
//        |$evaluateNondeterministicResults
//        ${consume(ctx, resultVars)}
      }else {
//        // generate result based on grouping key
//        ctx.INPUT_ROW = keyTerm
//        ctx.currentVars = null
//        val resultVars = bindReferences[Expression](
//          resultExpressions,
//          groupingAttributes).map(_.genCode(ctx))
//        val evaluateNondeterministicResults =
//          evaluateNondeterministicVariables(output, resultVars, resultExpressions)
//        s"""
//           |$evaluateNondeterministicResults
//           |${consume(ctx, resultVars)}
//       """.stripMargin
        s"""
           |${consume(ctx, Seq[ExprCode]())}
       """.stripMargin
      }
    ctx.addNewFunction(funcName,
      s"""
         |private void $funcName(UnsafeInternalRow $keyTerm, UnsafeInternalRow $bufferTerm)
         |    throws java.io.IOException {
         |  $body
         |}
       """.stripMargin)
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    if (groupingExpressions.isEmpty) {
      doProduceWithoutKeys(ctx)
    } else {
      doProduceWithKeys(ctx)
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    if (groupingExpressions.isEmpty) {
      doConsumeWithoutKeys(ctx, input)
    } else {
      doConsumeWithKeys(ctx, input)
    }
  }

  // The variables are used as aggregation buffers and each aggregate function has one or more
  // ExprCode to initialize its buffer slots. Only used for aggregation without keys.
  private var bufVars: Seq[Seq[ExprCode]] = _

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initAgg")
    // The generated function doesn't have input row in the code context.
    ctx.INPUT_ROW = null

    // generate variables for aggregation buffer
    val functions = aggregateFunctions.map(_.asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.map(f => f.initialValues)
    bufVars = initExpr.map { exprs =>
      exprs.map { e =>
        val isNull = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "bufIsNull")
        val value = ctx.addMutableState(CodeGenerator.javaType(e.dataType), "bufValue")
        // The initial expression should not access any column
        val ev = e.genCode(ctx)
        val initVars = code"""
                             |$isNull = ${ev.isNull};
                             |$value = ${ev.value};
         """.stripMargin
        ExprCode(
          ev.code + initVars,
          JavaCode.isNullGlobal(isNull),
          JavaCode.global(value, e.dataType))
      }
    }
    val flatBufVars = bufVars.flatten
    val initBufVar = evaluateVariables(flatBufVars)

    // generate variables for output
    val (resultVars, genResult) = {
      // evaluate aggregate results
      ctx.currentVars = flatBufVars
      val aggResults = bindReferences(
        functions.map(_.evaluateExpression),
        aggregateBufferAttributes.toSeq).map(_.genCode(ctx))
      val evaluateAggResults = evaluateVariables(aggResults)
      // evaluate result expressions
      ctx.currentVars = aggResults
//      val resultVars = bindReferences(resultExpressions, aggregateAttributes).map(_.genCode(ctx))
//      (resultVars, s"""
//                      |$evaluateAggResults
//                      |${evaluateVariables(resultVars)}
//       """.stripMargin)
      (aggResults, s"$evaluateAggResults")
    }

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    val doAggFuncName = ctx.addNewFunction(doAgg,
      s"""
         |private void $doAgg() throws java.io.IOException {
         |  // initialize aggregation buffer
         |  $initBufVar
         |
         |  ${child.asInstanceOf[PushBasedCodegen].produce(ctx, this)}
         |}
       """.stripMargin)

    s"""
       |while (!$initAgg) {
       |  $initAgg = true;
       |  $doAggFuncName();
       |
       |  // output the result
       |  ${genResult.trim}
       |
       |  ${consume(ctx, resultVars).trim}
       |}
     """.stripMargin
  }

  private def doConsumeWithoutKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // only have DeclarativeAggregate
    val functions = aggregateFunctions.map(_.asInstanceOf[DeclarativeAggregate])
    val inputAttrs = functions.flatMap(_.aggBufferAttributes).toSeq ++ child.output  // TODO: check this --lgh
    // To individually generate code for each aggregate function, an element in `updateExprs` holds
    // all the expressions for the buffer of an aggregation function.
    val updateExprs = aggregateFunctions.map { aggFun =>
      aggFun.asInstanceOf[DeclarativeAggregate].updateExpressions
    }
    ctx.currentVars = bufVars.flatten ++ input
    val boundUpdateExprs = updateExprs.map { updateExprsForOneFunc =>
      bindReferences(updateExprsForOneFunc, inputAttrs)
    }
//    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExprs.flatten)
//    val effectiveCodes = ctx.evaluateSubExprEliminationState(subExprs.states.values)
//    val bufferEvals = boundUpdateExprs.map { boundUpdateExprsForOneFunc =>
//      ctx.withSubExprEliminationExprs(subExprs.states) {
//        boundUpdateExprsForOneFunc.map(_.genCode(ctx))
//      }
//    }

    val bufferEvals = boundUpdateExprs.map { boundUpdateExprsForOneFunc =>
      boundUpdateExprsForOneFunc.map(_.genCode(ctx))
    }

    val aggNames = functions.map(_.prettyName)
    val aggCodeBlocks = bufferEvals.zipWithIndex.map { case (bufferEvalsForOneFunc, i) =>
      val bufVarsForOneFunc = bufVars(i)
      // All the update code for aggregation buffers should be placed in the end
      // of each aggregation function code.
      val updates = bufferEvalsForOneFunc.zip(bufVarsForOneFunc).map { case (ev, bufVar) =>
        s"""
           |${bufVar.isNull} = ${ev.isNull};
           |${bufVar.value} = ${ev.value};
         """.stripMargin
      }
      code"""
            |${ctx.registerComment(s"do aggregate for ${aggNames(i)}")}
            |${ctx.registerComment("evaluate aggregate function")}
            |${evaluateVariables(bufferEvalsForOneFunc)}
            |${ctx.registerComment("update aggregation buffers")}
            |${updates.mkString("\n").trim}
       """.stripMargin
    }

    val codeToEvalAggFuncs = aggCodeBlocks.map(_.code).mkString("\n")
    s"""
       |// do aggregate
       |// common sub-expressions
       |// evaluate aggregate functions and update aggregation buffers
       |$codeToEvalAggFuncs
     """.stripMargin
  }

  private def doProduceWithKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initAgg")

    val thisPlan = ctx.addReferenceObj("plan", this)

    // Create a name for the iterator from the regular hash map.
    // Inline mutable state since not many aggregation operations in a task
//    val iterTerm = ctx.addMutableState(classOf[KVIterator[UnsafeInternalRow, UnsafeInternalRow]].getName,
//      "mapIter", forceInline = true)
    val iterClassName = classOf[java.util.Iterator[java.util.Map.Entry[InternalRow, InternalRow]]].getName
    val iterTerm = ctx.addMutableState(iterClassName, "mapIter", forceInline = true)
//    // create hashMap
//    val hashMapClassName = classOf[UnsafeFixedWidthAggregationMap].getName
//    hashMapTerm = ctx.addMutableState(hashMapClassName, "hashMap", forceInline = true)
    val hashMapClassName =
          classOf[java.util.LinkedHashMap[InternalRow, InternalRow]].getName + "<UnsafeInternalRow, UnsafeInternalRow>"
    hashMapTerm = ctx.addMutableState(hashMapClassName,
      "hashMap", v => s"$v = new $hashMapClassName();", forceInline = true)
    val initExpr = declFunctions.flatMap(f => f.initialValues)
    initialBuffer = GenerateUnsafeProjection.generate(initExpr)(EmptyRow)
    val a = new util.LinkedHashMap[InternalRow, InternalRow]()
    val b: java.util.Iterator[java.util.Map.Entry[InternalRow, InternalRow]] = a.entrySet().iterator()
    initialBufferTerm = ctx.addMutableState("UnsafeInternalRow", "initialBuffer",
      v => s"$v = $thisPlan.getInitialBuffer();", forceInline = true)
//    sorterTerm = ctx.addMutableState(classOf[UnsafeKVExternalSorter].getName, "sorter",
//      forceInline = true)

    val doAgg = ctx.freshName("doAggregateWithKeys")

    val finishHashMap = s"$iterTerm = $hashMapTerm.entrySet().iterator();"

    val doAggFuncName = ctx.addNewFunction(doAgg,
      s"""
         |private void $doAgg() throws java.io.IOException {
         |  ${child.asInstanceOf[PushBasedCodegen].produce(ctx, this)}
         |  $finishHashMap
         |}
       """.stripMargin)

    // generate code for output
    val keyTerm = ctx.freshName("aggKey")
    val bufferTerm = ctx.freshName("aggBuffer")
    val iterItemTerm = ctx.freshName("iterItem")
    val outputFunc = generateResultFunction(ctx)

    def outputFromRegularHashMap: String = {
      s"""
         |while ($iterTerm.hasNext()) {
         |  java.util.Map.Entry<InternalRow, InternalRow> $iterItemTerm = (java.util.Map.Entry<InternalRow, InternalRow>) $iterTerm.next();
         |  UnsafeInternalRow $keyTerm = (UnsafeInternalRow) $iterItemTerm.getKey();
         |  UnsafeInternalRow $bufferTerm = (UnsafeInternalRow) $iterItemTerm.getValue();
         |  $outputFunc($keyTerm, $bufferTerm);
         |  if (shouldStop()) return;
         |}
       """.stripMargin
    }
//    $iterTerm.close();
//    if ($sorterTerm == null) {
//      $hashMapTerm.free();
//    }

    s"""
       |if (!$initAgg) {
       |  $initAgg = true;
       |  // $hashMapTerm = $thisPlan.createHashMap();
       |  $doAggFuncName();
       |}
       |// output the result
       |$outputFromRegularHashMap
     """.stripMargin
  }

  def doConsumeWithKeys(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // create grouping key
    val unsafeRowKeyCode = GenerateUnsafeProjection.createCode(
      ctx, bindReferences[Expression](groupingExpressions, child.output))
    val fastRowKeys = ctx.generateExpressions(
      bindReferences[Expression](groupingExpressions, child.output))
    val unsafeRowKeys = unsafeRowKeyCode.value
    val unsafeRowBuffer = ctx.freshName("unsafeRowAggBuffer")

    // To individually generate code for each aggregate function, an element in `updateExprs` holds
    // all the expressions for the buffer of an aggregation function.
    val updateExprs = aggregateFunctions.map(_.asInstanceOf[DeclarativeAggregate].updateExpressions)

//    val (checkFallbackForBytesToBytesMap, resetCounter, incCounter) = testFallbackStartsAt match {
//      case Some((_, regularMapCounter)) =>
//        val countTerm = ctx.addMutableState(CodeGenerator.JAVA_INT, "fallbackCounter")
//        (s"$countTerm < $regularMapCounter", s"$countTerm = 0;", s"$countTerm += 1;")
//      case _ => ("true", "", "")
//    }

//    val oomeClassName = classOf[SparkOutOfMemoryError].getName

//    |int $unsafeRowKeyHash = ${unsafeRowKeyCode.value}.hashCode();
    val findOrInsertHashMap: String =
    s"""
         |// generate grouping key
         |${unsafeRowKeyCode.code}
         |  // try to get the buffer from hash map
         |$unsafeRowBuffer =
         |    (UnsafeInternalRow) $hashMapTerm.get($unsafeRowKeys);
         |// Can't allocate buffer from the hash map. Spill the map and fallback to sort-based
         |// aggregation after processing all input rows.
         |if ($unsafeRowBuffer == null) {
         |  $unsafeRowBuffer = (UnsafeInternalRow) $initialBufferTerm.copy();
         |  $hashMapTerm.put($unsafeRowKeys.copy(), $unsafeRowBuffer);
         |}
       """.stripMargin

    val inputAttrs = aggregateBufferAttributes ++ child.output
    // Here we set `currentVars(0)` to `currentVars(numBufferSlots)` to null, so that when
    // generating code for buffer columns, we use `INPUT_ROW`(will be the buffer row), while
    // generating input columns, we use `currentVars`.
    ctx.currentVars = new Array[ExprCode](aggregateBufferAttributes.length) ++ input

    val aggNames = aggregateFunctions.map(_.prettyName)
    // Computes start offsets for each aggregation function code
    // in the underlying buffer row.
    val bufferStartOffsets = {
      val offsets = mutable.ArrayBuffer[Int]()
      var curOffset = 0
      updateExprs.foreach { exprsForOneFunc =>
        offsets += curOffset
        curOffset += exprsForOneFunc.length
      }
      offsets.toArray
    }

    val updateRowInHashMap: String = {
      ctx.INPUT_ROW = unsafeRowBuffer
      val boundUpdateExprs = updateExprs.map { updateExprsForOneFunc =>
        bindReferences(updateExprsForOneFunc, inputAttrs)
      }
      val unsafeRowBufferEvals = boundUpdateExprs.map { boundUpdateExprsForOneFunc =>
          boundUpdateExprsForOneFunc.map(_.genCode(ctx))
      }

      val aggCodeBlocks = updateExprs.indices.map { i =>
        val rowBufferEvalsForOneFunc = unsafeRowBufferEvals(i)
        val boundUpdateExprsForOneFunc = boundUpdateExprs(i)
        val bufferOffset = bufferStartOffsets(i)

        // All the update code for aggregation buffers should be placed in the end
        // of each aggregation function code.
        val updateRowBuffers = rowBufferEvalsForOneFunc.zipWithIndex.map { case (ev, j) =>
          val updateExpr = boundUpdateExprsForOneFunc(j)
          val dt = updateExpr.dataType
          val nullable = updateExpr.nullable
          CodeGenerator.updateColumn(unsafeRowBuffer, dt, bufferOffset + j, ev, nullable)
        }
        code"""
              |${ctx.registerComment(s"evaluate aggregate function for ${aggNames(i)}")}
              |${evaluateVariables(rowBufferEvalsForOneFunc)}
              |${ctx.registerComment("update unsafe row buffer")}
              |${updateRowBuffers.mkString("\n").trim}
         """.stripMargin
      }

      val codeToEvalAggFuncs = aggCodeBlocks.map(_.code).mkString("\n")
      s"""
         |// evaluate aggregate functions and update aggregation buffers
         |$codeToEvalAggFuncs
       """.stripMargin
    }

    val declareRowBuffer: String = s"UnsafeInternalRow $unsafeRowBuffer = null;"

    // We try to do hash map based in-memory aggregation first. If there is not enough memory (the
    // hash map will return null for new key), we spill the hash map to disk to free memory, then
    // continue to do in-memory aggregation and spilling until all the rows had been processed.
    // Finally, sort the spilled aggregate buffers by key, and merge them together for same key.
    s"""
       |$declareRowBuffer
       |$findOrInsertHashMap
       |$updateRowInHashMap
     """.stripMargin
  }

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
  private val declFunctions =
    aggregateFunctions.filter(_.isInstanceOf[DeclarativeAggregate]).map(_.asInstanceOf[DeclarativeAggregate])
  private val bufferSchema = StructType.fromAttributes(aggregateBufferAttributes)


  // The name for UnsafeInternalRow HashMap
  private var hashMapTerm: String = _
  private var sorterTerm: String = _
  private var initialBuffer: UnsafeInternalRow = _
  def getInitialBuffer: UnsafeInternalRow = initialBuffer
  private var initialBufferTerm: String = _

  protected def withNewChildInternal(newChild: SeccoPlan): AggregateExec =
    copy(child = newChild)

  override def children: Seq[SeccoPlan] = child :: Nil

  override def inputRowIterators(): Seq[Iterator[InternalRow]] =
    child.asInstanceOf[PushBasedCodegen].inputRowIterators()
}