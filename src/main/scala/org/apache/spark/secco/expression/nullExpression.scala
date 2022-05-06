package org.apache.spark.secco.expression

import org.apache.spark.secco.analysis.TypeCheckResult
import org.apache.spark.secco.codegen._
import org.apache.spark.secco.codegen.Block._
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.util.TypeUtils

/**
  * An expression that is evaluated to the first non-null input.
  *
  * {{{
  *   coalesce(1, 2) => 1
  *   coalesce(null, 1, 2) => 1
  *   coalesce(null, null, 2) => 2
  *   coalesce(null, null, null) => null
  * }}}
  */
// scalastyle:off line.size.limit
// scalastyle:on line.size.limit
case class Coalesce(children: Seq[Expression]) extends ComplexTypeMergingExpression {

  /** Coalesce is nullable if all of its children are nullable, or if it has no children. */
  override def nullable: Boolean = children.forall(_.nullable)

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = children.forall(_.foldable)

//  final override val nodePatterns: Seq[TreePattern] = Seq(COALESCE)

//  override def checkInputDataTypes(): TypeCheckResult = {
//    if (children.length < 1) {
//      TypeCheckResult.TypeCheckFailure(
//        s"input to function $prettyName requires at least one argument")
//    } else {
//      TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), s"function $prettyName")
//    }
//  }

  override def eval(input: InternalRow): Any = {
    var result: Any = null
    val childIterator = children.iterator
    while (childIterator.hasNext && result == null) {
      result = childIterator.next().eval(input)
    }
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ev.isNull = JavaCode.isNullGlobal(ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, ev.isNull))

    // all the evals are meant to be in a do { ... } while (false); loop
    val evals = children.map { e =>
      val eval = e.genCode(ctx)
      s"""
         |${eval.code}
         |if (!${eval.isNull}) {
         |  ${ev.isNull} = false;
         |  ${ev.value} = ${eval.value};
         |  continue;
         |}
       """.stripMargin
    }

    val resultType = CodeGenerator.javaType(dataType)
    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = evals,
      funcName = "coalesce",
      returnType = resultType,
      makeSplitFunction = func =>
        s"""
           |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
           |do {
           |  $func
           |} while (false);
           |return ${ev.value};
         """.stripMargin,
      foldFunctions = _.map { funcCall =>
        s"""
           |${ev.value} = $funcCall;
           |if (!${ev.isNull}) {
           |  continue;
           |}
         """.stripMargin
      }.mkString)


    ev.copy(code =
      code"""
            |${ev.isNull} = true;
            |$resultType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
            |do {
            |  $codes
            |} while (false);
       """.stripMargin)
  }

//  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Coalesce =
//    copy(children = newChildren)
}