
package org.apache.spark.secco.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen._
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.BindReferences.bindReferences
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.types.StructType
import org.apache.spark.secco.util.misc.LogAble

import scala.collection.mutable


/**
  * Leaf codegen node reading from a single table.
  */
case class BlockInputExec(override val output: Seq[Attribute],
                          block: InternalBlock) extends PushBasedCodegen with LeafExecNode with LogAble{

  // If the input can be InternalRows, an UnsafeProjection needs to be created.
  private val createUnsafeProjection: Boolean = false

  private lazy val unsafeRowIterator: Iterator[InternalRow] = {
    if (block.isEmpty()) {
      Iterator.empty
    } else {
      val rowArray = block.toArray()
      logTrace("in unsafeRowIterator: after rowArray")
      val resultRowArray = rowArray.map(r => UnsafeInternalRow.fromInternalRow(StructType.fromAttributes(output), r))
      logTrace("in unsafeRowIterator: after resultRowArray")
      resultRowArray.iterator
    }
  }

  override def inputRowIterators(): Seq[Iterator[InternalRow]] = {
    logTrace("in BlockInputExec.inputRowIterators(), before unsafeRowIterator")
    val out = Seq(unsafeRowIterator)
    logTrace("in BlockInputExec.inputRowIterators(), after unsafeRowIterator")
    out
  }

  override def doProduce(ctx: CodegenContext): String = {
    // Inline mutable state since an InputRDDCodegen is used once in a task for WholeStageCodegen
    val input = ctx.addMutableState("scala.collection.Iterator",
      "input", v => s"$v = inputs[${ctx.getCurInputIndex}];", forceInline = true)
    val row = ctx.freshName("row")

    val outputVars = if (createUnsafeProjection) {
      // creating the vars will make the parent consume add an unsafe projection.
      ctx.INPUT_ROW = row
      ctx.currentVars = null
      output.zipWithIndex.map { case (a, i) =>
        BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      }
    } else {
      null
    }

    s"""
       | System.out.println("in BlockInputExec before hasNext()");
       | while ($input.hasNext()) {
       |   System.out.println("in BlockInputExec before next(): $input = " + $input);
       |   InternalRow $row = (InternalRow) $input.next();
       |   System.out.println("in BlockInputExec after next(): $row = " + $row);
       |   ${consume(ctx, outputVars, if (createUnsafeProjection) null else row).trim}
       | }
     """.stripMargin
  }

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[OldInternalBlock] = ???
}


trait GeneratePredicateHelper extends PredicateHelper {
  self: PushBasedCodegen =>

  protected def generatePredicateCode(
                                       ctx: CodegenContext,
                                       condition: Expression,
                                       inputAttrs: Seq[Attribute],
                                       inputExprCode: Seq[ExprCode]): String = {
    val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
      case IsNotNull(a) => a.references.subsetOf(AttributeSet(inputAttrs))
      case _ => false
    }
    val nonNullAttrExprIds = notNullPreds.flatMap(_.references).distinct.map(_.exprId)
    val outputAttrs = outputWithNonNullability(inputAttrs, nonNullAttrExprIds)
    generatePredicateCode(
      ctx, inputAttrs, inputExprCode, outputAttrs, notNullPreds, otherPreds,
      nonNullAttrExprIds)
  }

  protected def generatePredicateCode(
                                       ctx: CodegenContext,
                                       inputAttrs: Seq[Attribute],
                                       inputExprCode: Seq[ExprCode],
                                       outputAttrs: Seq[Attribute],
                                       notNullPreds: Seq[Expression],
                                       otherPreds: Seq[Expression],
                                       nonNullAttrExprIds: Seq[ExprId]): String = {
    /**
      * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
      */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      val evaluated = evaluateRequiredVariables(inputAttrs, in, c.references)

      // Generate the code for the predicate.
//      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
      val ev = bound.genCode(ctx)
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        s""
      }

      s"""
         |$evaluated
         |${ev.code}
         |if (${nullCheck}!${ev.value}) continue;
       """.stripMargin
    }

    // To generate the predicates we will follow this algorithm.
    // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
    // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
    // generate that check *before* the predicate. After all of these predicates, we will generate
    // the remaining IsNotNull checks that were not part of other predicates.
    // This has the property of not doing redundant IsNotNull checks and taking better advantage of
    // short-circuiting, not loading attributes until they are needed.
    // This is very perf sensitive.
    // TODO: revisit this. We can consider reordering predicates as well.
    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
    val extraIsNotNullAttrs = mutable.Set[Attribute]()
    val generated = otherPreds.map { c =>
      val nullChecks = c.references.map { r =>
        val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r)}
        if (idx != -1 && !generatedIsNotNullChecks(idx)) {
          generatedIsNotNullChecks(idx) = true
          // Use the child's output. The nullability is what the child produced.
          genPredicate(notNullPreds(idx), inputExprCode, inputAttrs)
        } else if (nonNullAttrExprIds.contains(r.exprId) && !extraIsNotNullAttrs.contains(r)) {
          extraIsNotNullAttrs += r
          genPredicate(IsNotNull(r), inputExprCode, inputAttrs)
        } else {
          ""
        }
      }.mkString("\n").trim

      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |$nullChecks
         |${genPredicate(c, inputExprCode, outputAttrs)}
       """.stripMargin.trim
    }.mkString("\n")

    val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedIsNotNullChecks(idx)) {
        genPredicate(c, inputExprCode, inputAttrs)
      } else {
        ""
      }
    }.mkString("\n")

    s"""
       |$generated
       |$nullChecks
     """.stripMargin
  }
}


/** Physical plan for Filter. */
case class FilterExec(condition: Expression, child: SeccoPlan)
  extends UnaryExecNode with PushBasedCodegen with GeneratePredicateHelper {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = outputWithNonNullability(child.output, notNullAttributes)

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[PushBasedCodegen].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
//    val numOutput = metricTerm(ctx, "numOutputRows")

    val predicateCode = generatePredicateCode(
      ctx, child.output, input, output, notNullPreds, otherPreds, notNullAttributes)

    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = FalseLiteralValue
      }
      ev
    }

    // Note: wrap in "do { } while(false);", so the generated checks can jump out with "continue;"
    s"""
       |do {
       |  $predicateCode
       |  ${consume(ctx, resultVars)}
       |} while(false);
     """.stripMargin
  }
  //  |  $numOutput.add(1);

  protected override def doExecute(): RDD[OldInternalBlock] = ???

  def verboseStringWithOperatorId(): String = {
    s"""
       |$nodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |Condition : ${condition}
       |""".stripMargin
  }

  protected def withNewChildInternal(newChild: SeccoPlan): FilterExec =
    copy(child = newChild)

  override def inputRowIterators(): Seq[Iterator[InternalRow]] =
    child.asInstanceOf[PushBasedCodegen].inputRowIterators()
}

/** Physical plan for Project. */
case class ProjectExec(projectList: Seq[NamedExpression], child: SeccoPlan)
  extends UnaryExecNode
    with PushBasedCodegen {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[PushBasedCodegen].produce(ctx, this)
  }

  override def usedInputs: AttributeSet = {
    // only the attributes those are used at least twice should be evaluated before this plan,
    // otherwise we could defer the evaluation until output attribute is actually used.
    val usedExprIds = projectList.flatMap(_.collect {
      case a: Attribute => a.exprId
    })
    val usedMoreThanOnce = usedExprIds.groupBy(id => id).filter(_._2.size > 1).keySet
    references.filter(a => usedMoreThanOnce.contains(a.exprId))
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val exprs = bindReferences[Expression](projectList, child.output)
    val resultVars = exprs.map(_.genCode(ctx))

    // Evaluation of non-deterministic expressions can't be deferred.
    val nonDeterministicAttrs = projectList.filterNot(_.deterministic).map(_.toAttribute)
    s"""
       |${evaluateRequiredVariables(output, resultVars, AttributeSet(nonDeterministicAttrs))}
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[OldInternalBlock] = ???

  def verboseStringWithOperatorId(): String = {
    s"""
       |$nodeName
       |${ExplainUtils.generateFieldString("Output", projectList)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |""".stripMargin
  }

  protected def withNewChildInternal(newChild: SeccoPlan): ProjectExec =
    copy(child = newChild)

  override def inputRowIterators(): Seq[Iterator[InternalRow]] =
    child.asInstanceOf[PushBasedCodegen].inputRowIterators()
}


object ExplainUtils {

  /**
    * Generate detailed field string with different format based on type of input value
    */
  def generateFieldString(fieldName: String, values: Any): String = values match {
    case iter: Iterable[_] if (iter.size == 0) => s"${fieldName}: []"
    case iter: Iterable[_] => s"${fieldName} [${iter.size}]: ${iter.mkString("[", ", ", "]")}"
    case str: String if (str == null || str.isEmpty) => s"${fieldName}: None"
    case str: String => s"${fieldName}: ${str}"
    case _ => throw new IllegalArgumentException(s"Unsupported type for argument values: $values")
  }
}


// TODO: 1. doExecute 2.SubExpressionElimination (common subexpression)