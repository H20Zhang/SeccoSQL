package org.apache.spark.secco.expression

import org.apache.spark.secco.codegen._
import org.apache.spark.secco.codegen.Block._
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.types._

/** An expression that is evaluated to true if the input is null.
  */
case class IsNull(child: Expression) extends UnaryExpression with Predicate {

  override def nullable: Boolean = false

//  final override val nodePatterns: Seq[TreePattern] = Seq(NULL_CHECK)

  override def eval(input: InternalRow): Any = {
    child.eval(input) == null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    ExprCode(code = eval.code, isNull = FalseLiteralValue, value = eval.isNull)
  }

  override def sql: String = s"(${child.sql} IS NULL)"

//  override protected def withNewChildInternal(newChild: Expression): IsNull = copy(child = newChild)
}

/** An expression that is evaluated to true if the input is not null.
  */
case class IsNotNull(child: Expression) extends UnaryExpression with Predicate {
  override def nullable: Boolean = false

//  final override val nodePatterns: Seq[TreePattern] = Seq(NULL_CHECK)

  override def eval(input: InternalRow): Any = {
    child.eval(input) != null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val (value, newCode) = eval.isNull match {
      case TrueLiteralValue  => (FalseLiteralValue, EmptyBlock)
      case FalseLiteralValue => (TrueLiteralValue, EmptyBlock)
      case v =>
        val value = ctx.freshName("value")
        (JavaCode.variable(value, BooleanType), code"boolean $value = !$v;")
    }
    ExprCode(
      code = eval.code + newCode,
      isNull = FalseLiteralValue,
      value = value
    )
  }

  override def sql: String = s"(${child.sql} IS NOT NULL)"

//  override protected def withNewChildInternal(newChild: Expression): IsNotNull =
//    copy(child = newChild)
}
