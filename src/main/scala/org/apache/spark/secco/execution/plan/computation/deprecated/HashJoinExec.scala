package org.apache.spark.secco.execution.plan.computation.deprecated

import org.apache.spark.rdd.RDD
import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen._
import org.apache.spark.secco.execution.plan.computation.{
  BuildExecPushBasedCodegen,
  PushBasedCodegen
}
import org.apache.spark.secco.execution.storage.InternalPartition
import org.apache.spark.secco.execution.storage.block._
import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.execution.{BinaryExecNode, SeccoPlan}
import org.apache.spark.secco.expression.BindReferences.bindReferences
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.codegen._
import org.apache.spark.secco.util.misc.LogAble

sealed abstract class BuildSide

case object BuildRight extends BuildSide

case object BuildLeft extends BuildSide

/** Physical plan for Build HashMap. */
case class BuildHashMapExec(
    child: SeccoPlan,
    keys: Seq[Attribute]
) extends BuildExecPushBasedCodegen {

  private var hashMapBuilderTerm: String = _

  override protected def doProduceBulk(
      ctx: CodegenContext
  ): (String, String) = {
    val builderClassName = classOf[HashMapInternalBlockBuilder].getName
    hashMapBuilderTerm = ctx.addMutableState(builderClassName, "hashMapBuilder")
    val blockClassName = classOf[HashMapInternalBlock].getName
    val hashMapBlockTerm = ctx.addMutableState(blockClassName, "hashMapBlock")
    val codeStr = {
      s"""
         |$hashMapBuilderTerm = ($builderClassName) ${ctx.addReferenceObj(
        s"HashMapBuilder",
        new HashMapInternalBlockBuilder(child.output, keys)
      )};
         |${child.asInstanceOf[PushBasedCodegen].produce(ctx, this)}
         |$hashMapBlockTerm = $hashMapBuilderTerm.build();
         |""".stripMargin
    }
    (codeStr, hashMapBlockTerm)
  }

  override def doConsume(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      row: ExprCode
  ): String = {
    s"""
       |${row.code}
       |$hashMapBuilderTerm.add(${row.value}.copy());
       |""".stripMargin
  }

  override protected def doExecute(): RDD[InternalPartition] = ???

  override def inputRowIterators(): Seq[Iterator[InternalRow]] =
    child.asInstanceOf[PushBasedCodegen].inputRowIterators()
}

/** Physical plan for Hash Join. */
case class HashJoinExec(
    left: SeccoPlan,
    right: SeccoPlan,
    streamedKeys: Seq[Expression],
    joinCondition: Option[Expression]
) extends BinaryExecNode
    with PushBasedCodegen
    with LogAble {

  var thisPlan: String = _
  var relationTerm: String = _
  var keyIsUnique: Boolean = false
  var isEmptyHashedRelation: Boolean = false

  def buildSide: BuildSide = BuildRight

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft  => (left, right)
    case BuildRight => (right, left)
  }

  @transient protected lazy val (buildOutput, streamedOutput) = {
    buildSide match {
      case BuildLeft  => (left.output, right.output)
      case BuildRight => (right.output, left.output)
    }
  }

  @transient protected lazy val streamedBoundKeys: Seq[Expression] =
    bindReferences(streamedKeys, streamedOutput)

  override def output: Seq[Attribute] = left.output ++ right.output

  /** Generates the code for variables of one child side of join.
    */
  protected def genOneSideJoinVars(
      ctx: CodegenContext,
      row: String,
      plan: SeccoPlan,
      setDefaultValue: Boolean
  ): Seq[ExprCode] = {
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
                         |$javaType $value = ${CodeGenerator.defaultValue(
          a.dataType
        )};
                         |if ($row != null) {
                         |  ${ev.code}
                         |  $isNull = ${ev.isNull};
                         |  $value = ${ev.value};
                         |}
          """.stripMargin
        ExprCode(
          code,
          JavaCode.isNullVariable(isNull),
          JavaCode.variable(value, a.dataType)
        )
      } else {
        ev
      }
    }
  }

  /** Generate the (non-equi) condition used to filter joined rows.
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
      buildRow: Option[String] = None
  ): (String, String, Seq[ExprCode]) = {
    val buildSideRow = buildRow.getOrElse(ctx.freshName("buildRow"))
    val buildVars =
      genOneSideJoinVars(ctx, buildSideRow, buildPlan, setDefaultValue = false)
    logTrace(s"streamVars: ${streamVars.mkString(", ")}")
    logTrace(s"buildVars: ${buildVars.mkString(", ")}")
    val checkCondition = if (joinCondition.isDefined) {
      val expr = joinCondition.get
      // evaluate the variables from build side that used by condition
      val eval =
        evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)

      // filter the output via condition
      ctx.currentVars = streamVars ++ buildVars

      val ev =
        BindReferences
          .bindReference(expr, streamPlan.output ++ buildPlan.output)
          .genCode(ctx)
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

    s"""
       |${
      val (codeStr, hashMapTerm) =
        buildPlan.asInstanceOf[BuildHashMapExec].produceBulk(ctx, this)
      relationTerm = hashMapTerm
      ctx.incrementCurInputIndex()
      codeStr
    }
       |${streamedPlan.asInstanceOf[PushBasedCodegen].produce(ctx, this)}
       |""".stripMargin
  }

  override def doConsume(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      row: ExprCode
  ): String = {

    // generate the join key as UnsafeInternalRow
    ctx.currentVars = input
    val ev = GenerateUnsafeProjection.createCode(ctx, streamedBoundKeys)
    val (keyEv, anyNull) = (ev, s"${ev.value}.anyNull()")

    val (matched, checkCondition, buildVars) =
      getJoinCondition(ctx, input, streamedPlan, buildPlan)

    val resultVars = buildSide match {
      case BuildLeft  => buildVars ++ input
      case BuildRight => input ++ buildVars
    }

    if (isEmptyHashedRelation) {
      """
        |// If HashedRelation is empty, hash inner join simply returns nothing.
      """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val arrayCls = classOf[InternalRow].getName + "[]"

      s"""
         |// generate join key for stream side
         |${keyEv.code}
         |// find matches from HashRelation
         |$arrayCls $matches = $anyNull ?
         |  null : $relationTerm.get(${keyEv.value});
         |System.out.println("$relationTerm: " + ${relationTerm}.toString());
         |if ($matches != null) {
         |  System.out.println("find matches, matches.length: " + $matches.length);
         |  for (InternalRow $matched:$matches) {
         |    $checkCondition {
         |    System.out.println("find a match: " + $matched);
         |      ${consume(ctx, resultVars)}
         |    }
         |  }
         |}
       """.stripMargin
    }
  }

  override protected def doExecute(): RDD[InternalPartition] = ???

  def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (joinCondition.isDefined) {
      s"${joinCondition.get}"
    } else "None"
    if (streamedKeys.nonEmpty) {
      s"""
         |$nodeName
         |${ExplainUtils.generateFieldString("Streamed keys", streamedKeys)}
         |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
         |""".stripMargin
    } else {
      s"""
         |$nodeName
         |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
         |""".stripMargin
    }
  }

  protected def withNewChildrenInternal(
      newLeft: SeccoPlan,
      newRight: SeccoPlan
  ): HashJoinExec =
    copy(left = newLeft, right = newRight)

  override def inputRowIterators(): Seq[Iterator[InternalRow]] =
    buildPlan.asInstanceOf[PushBasedCodegen].inputRowIterators() ++
      streamedPlan.asInstanceOf[PushBasedCodegen].inputRowIterators()
}
