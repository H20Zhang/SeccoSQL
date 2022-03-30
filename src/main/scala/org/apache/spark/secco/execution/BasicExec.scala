
package org.apache.spark.secco.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen._
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.BindReferences.bindReferences
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.codegen._
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.types.StructType
//import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}

import scala.collection.mutable


/**
  * Leaf codegen node reading from a single table.
  */
case class BlockInputExec(override val output: Seq[Attribute],
                          block: InternalBlock) extends PushBasedCodegen with LeafExecNode {

  // If the input can be InternalRows, an UnsafeProjection needs to be created.
  private val createUnsafeProjection: Boolean = false

  private lazy val unsafeRowIterator: Iterator[InternalRow] = {
    if (block.isEmpty()) {
      Iterator.empty
    } else {
//      println("in unsafeRowIterator: before generate")
//      val proj = GenerateUnsafeProjection.generate(output, output)
//      println("in unsafeRowIterator: after generate")
      val rowArray = block.toArray()
      println("in unsafeRowIterator: after rowArray")
//      val resultRowArray = rowArray.map(r => proj(r).copy())
      val resultRowArray = rowArray.map(r => UnsafeInternalRow.fromInternalRow(StructType.fromAttributes(output), r))
      println("in unsafeRowIterator: after resultRowArray")
      resultRowArray.iterator
    }
  }
//  private lazy val rowIterator: Iterator[InternalRow] = {
//    if (block.isEmpty()) {
//      Iterator.empty
//    } else {
//      val proj = GenerateUnsafeProjection.generate(output, output)
//      block.toArray().iterator
//    }
//  }

//  override def inputRowIterator(): Iterator[InternalRow] = unsafeRowIterator
  override def inputRowIterators(): Seq[Iterator[InternalRow]] = {
    println("in BlockInputExec.inputRowIterators(), before unsafeRowIterator")
    val out = Seq(unsafeRowIterator)
    println("in BlockInputExec.inputRowIterators(), after unsafeRowIterator")
    out
  }
//  override def inputRowIterators(): Seq[Iterator[InternalRow]] = rowIterator :: Nil

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
//  protected override def doExecute(): RDD[InternalRow] = {
////    val numOutputRows = longMetric("numOutputRows")
//    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
//      val predicate = GeneratePredicate.generate(condition, child.output)
//      predicate.initialize(0)
//      iter.filter { row => predicate.eval(row)}
//    }
//  }

  def verboseStringWithOperatorId(): String = {
    s"""
       |$nodeName
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |Condition : ${condition}
       |""".stripMargin
  }

  protected def withNewChildInternal(newChild: SeccoPlan): FilterExec =
    copy(child = newChild)

//  override def inputRowIterator(): Iterator[InternalRow] = child.asInstanceOf[PushBasedCodegen].inputRowIterator()
  override def inputRowIterators(): Seq[Iterator[InternalRow]] =
    child.asInstanceOf[PushBasedCodegen].inputRowIterators()
}

sealed abstract class BuildSide

case object BuildRight extends BuildSide

case object BuildLeft extends BuildSide
//
///**
//  * @param relationTerm variable name for HashedRelation
//  * @param keyIsUnique  indicate whether keys of HashedRelation known to be unique in code-gen time
//  * @param isEmpty indicate whether it known to be EmptyHashedRelation in code-gen time
//  */
//private[execution] case class HashedRelationInfo(
//                                              relationTerm: String,
//                                              keyIsUnique: Boolean,
//                                              isEmpty: Boolean)

/** Physical plan for Hash Join. */
case class HashJoinExec(
  left: SeccoPlan,
  //    right: BaseBuildIndexIterator,
  right: SeccoPlan,
  leftKeys: Seq[Expression],
  rightKeys: Seq[Expression],
  joinCondition: Option[Expression]
  )
  extends BinaryExecNode
    with PushBasedCodegen {

  var thisPlan: String = _
  var relationTerm: String = _
  var keyIsUnique: Boolean = false
  var isEmptyHashedRelation: Boolean = false

  def buildSide: BuildSide = BuildLeft

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = {
    require(leftKeys.length == rightKeys.length &&
      leftKeys.map(_.dataType)
        .zip(rightKeys.map(_.dataType))
        .forall(types => types._1.sameType(types._2)),
      "Join keys from two sides should have same length and types")
    buildSide match {
      case BuildLeft => (leftKeys, rightKeys)
      case BuildRight => (rightKeys, leftKeys)
    }
  }

  @transient protected lazy val (buildOutput, streamedOutput) = {
    buildSide match {
      case BuildLeft => (left.output, right.output)
      case BuildRight => (right.output, left.output)
    }
  }

  @transient protected lazy val buildBoundKeys: Seq[Expression] =
    bindReferences(buildKeys, buildOutput)

  @transient protected lazy val streamedBoundKeys: Seq[Expression] =
    bindReferences(streamedKeys, streamedOutput)

  override def output: Seq[Attribute] = left.output ++ right.output

  /**
    * Generates the code for variables of one child side of join.
    */
  protected def genOneSideJoinVars(
                                    ctx: CodegenContext,
                                    row: String,
                                    plan: SeccoPlan,
                                    setDefaultValue: Boolean): Seq[ExprCode] = {
    ctx.currentVars = null
    ctx.INPUT_ROW = row
    plan.output.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      if (setDefaultValue) {
        // the variables are needed even there is no matched rows
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val javaType = CodeGenerator.javaType(a.dataType)
        val code = code"""
                         |boolean $isNull = true;
                         |$javaType $value = ${CodeGenerator.defaultValue(a.dataType)};
                         |if ($row != null) {
                         |  ${ev.code}
                         |  $isNull = ${ev.isNull};
                         |  $value = ${ev.value};
                         |}
          """.stripMargin
        ExprCode(code, JavaCode.isNullVariable(isNull), JavaCode.variable(value, a.dataType))
      } else {
        ev
      }
    }
  }

  /**
    * Generate the (non-equi) condition used to filter joined rows.
    * This is used in Inner, Left Semi, Left Anti and Full Outer joins.
    *
    * @return Tuple of variable name for row of build side, generated code for condition,
    *         and generated code for variables of build side.
    */
  protected def getJoinCondition(
                                  ctx: CodegenContext,
                                  streamVars: Seq[ExprCode],
                                  streamPlan: SeccoPlan,
                                  buildPlan: SeccoPlan,
                                  buildRow: Option[String] = None): (String, String, Seq[ExprCode]) = {
    val buildSideRow = buildRow.getOrElse(ctx.freshName("buildRow"))
    val buildVars = genOneSideJoinVars(ctx, buildSideRow, buildPlan, setDefaultValue = false)
    val checkCondition = if (joinCondition.isDefined) {
      val expr = joinCondition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)

      // filter the output via condition
      ctx.currentVars = streamVars ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamPlan.output ++ buildPlan.output).genCode(ctx)
      val skipRow = s"${ev.isNull} || !${ev.value}"
      s"""
         |$eval
         |${ev.code}
         |if (!($skipRow))
       """.stripMargin
    } else {
      ""
    }
    (buildSideRow, checkCondition, buildVars)
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    thisPlan = ctx.addReferenceObj("plan", this)
    val clsName = classOf[HashedRelation].getName

    // Inline mutable state since not many join operations in a task
    relationTerm = ctx.addMutableState(clsName, "relation",
      v => s"$v = $thisPlan.buildHashedRelation(inputs[${ctx.getCurInputIndex}]);", forceInline = true)
    ctx.incrementCurInputIndex()
    streamedPlan.asInstanceOf[PushBasedCodegen].produce(ctx, this)
  }

  /**
    * This is called by generated Java class, should be public.
    */
  def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = HashedRelation(iter, buildBoundKeys)

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {

    // generate the join key as UnsafeInternalRow
    ctx.currentVars = input
    val ev = GenerateUnsafeProjection.createCode(ctx, streamedBoundKeys)
    val (keyEv, anyNull) = (ev, s"${ev.value}.anyNull()")

    val (matched, checkCondition, buildVars) = getJoinCondition(ctx, input, streamedPlan, buildPlan)

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }


    if (isEmptyHashedRelation) {
      """
        |// If HashedRelation is empty, hash inner join simply returns nothing.
      """.stripMargin
    } else if (keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashedRelation
         |UnsafeInternalRow $matched = $anyNull ? null: (UnsafeInternalRow)$relationTerm.getValue(${keyEv.value});
         |System.out.println("$relationTerm: " + ${relationTerm}.toString());
         |if ($matched != null) {
         |  $checkCondition {
         |    System.out.println("find a match: " + $matched);
         |    ${consume(ctx, resultVars)}
         |  }
         |}
       """.stripMargin
//      |    $numOutput.add(1);
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeInternalRow]].getName

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ?
         |  null : ($iteratorCls)$relationTerm.get(${keyEv.value});
         |System.out.println("$relationTerm: " + ${relationTerm}.toString());
         |if ($matches != null) {
         |  System.out.println("find matches, matches.hasNext(): " + $matches.hasNext());
         |  while ($matches.hasNext()) {
         |    UnsafeInternalRow $matched = (UnsafeInternalRow) $matches.next();
         |    $checkCondition {
         |    System.out.println("find a match: " + $matched);
         |      ${consume(ctx, resultVars)}
         |    }
         |  }
         |}
       """.stripMargin
//      |      $numOutput.add(1);
    }
  }

  protected override def doExecute(): RDD[OldInternalBlock] = ???
//  protected override def doExecute(): RDD[InternalRow] = {
//    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
//      val project = UnsafeProjection.create(projectList, child.output)
//      project.initialize(index)
//      iter.map(project)
//    }
//  }

  def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (joinCondition.isDefined) {
      s"${joinCondition.get}"
    } else "None"
    if (leftKeys.nonEmpty || rightKeys.nonEmpty) {
      s"""
         |$nodeName
         |${ExplainUtils.generateFieldString("Left keys", leftKeys)}
         |${ExplainUtils.generateFieldString("Right keys", rightKeys)}
         |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
         |""".stripMargin
    } else {
      s"""
         |$nodeName
         |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
         |""".stripMargin
    }
  }

  protected def withNewChildrenInternal(newLeft: SeccoPlan, newRight: SeccoPlan): HashJoinExec =
    copy(left = newLeft, right = newRight)

//  override def inputRowIterator(): Iterator[InternalRow] =
//    streamedPlan.asInstanceOf[PushBasedCodegen].inputRowIterator()
  override def inputRowIterators(): Seq[Iterator[InternalRow]] =
    buildPlan.asInstanceOf[PushBasedCodegen].inputRowIterators() ++
      streamedPlan.asInstanceOf[PushBasedCodegen].inputRowIterators()
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
  //  protected override def doExecute(): RDD[InternalRow] = {
  //    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
  //      val project = UnsafeProjection.create(projectList, child.output)
  //      project.initialize(index)
  //      iter.map(project)
  //    }
  //  }

  def verboseStringWithOperatorId(): String = {
    s"""
       |$nodeName
       |${ExplainUtils.generateFieldString("Output", projectList)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |""".stripMargin
  }

  protected def withNewChildInternal(newChild: SeccoPlan): ProjectExec =
    copy(child = newChild)

//  override def inputRowIterator(): Iterator[InternalRow] = child.asInstanceOf[PushBasedCodegen].inputRowIterator()
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