package org.apache.spark.secco.expression

import org.apache.spark.secco.codegen._
import org.apache.spark.secco.codegen.Block._
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.types._

case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends ComplexTypeMergingExpression {

  @transient
  override lazy val inputTypesForMerging: Seq[DataType] = {
    Seq(trueValue.dataType, falseValue.dataType)
  }

  def first: Expression = predicate
  def second: Expression = trueValue
  def third: Expression = falseValue
  override final lazy val children: Seq[Expression] = IndexedSeq(first, second, third)

//  final override val nodePatterns : Seq[TreePattern] = Seq(IF)

//  override def checkInputDataTypes(): TypeCheckResult = {
//    if (predicate.dataType != BooleanType) {
//      TypeCheckResult.TypeCheckFailure(
//        "type of predicate expression in If should be boolean, " +
//          s"not ${predicate.dataType.catalogString}")
//    } else if (!TypeCoercion.haveSameType(inputTypesForMerging)) {
//      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
//        s"(${trueValue.dataType.catalogString} and ${falseValue.dataType.catalogString}).")
//    } else {
//      TypeCheckResult.TypeCheckSuccess
//    }
//  }

  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  override def eval(input: InternalRow): Any = {
    if (java.lang.Boolean.TRUE.equals(predicate.eval(input))) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val condEval = predicate.genCode(ctx)
    val trueEval = trueValue.genCode(ctx)
    val falseEval = falseValue.genCode(ctx)

    val code =
      code"""
            |${condEval.code}
            |boolean ${ev.isNull} = false;
            |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            |if (!${condEval.isNull} && ${condEval.value}) {
            |  ${trueEval.code}
            |  ${ev.isNull} = ${trueEval.isNull};
            |  ${ev.value} = ${trueEval.value};
            |} else {
            |  ${falseEval.code}
            |  ${ev.isNull} = ${falseEval.isNull};
            |  ${ev.value} = ${falseEval.value};
            |}
       """.stripMargin
    ev.copy(code = code)
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"

//  override protected def withNewChildrenInternal(
//                                                  newFirst: Expression, newSecond: Expression, newThird: Expression): Expression = copy(
//    predicate = newFirst,
//    trueValue = newSecond,
//    falseValue = newThird
//  )

  override def sql: String = s"(IF(${predicate.sql}, ${trueValue.sql}, ${falseValue.sql}))"
}