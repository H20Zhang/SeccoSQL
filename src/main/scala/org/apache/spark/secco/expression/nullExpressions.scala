package org.apache.spark.secco.expression

import org.apache.spark.secco.codegen.{CodegenContext, ExprCode}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.types.DataType
import org.apache.spark.sql.catalyst.expressions.Expression

/** Returns the first non-null argument if exists. Otherwise, null. */
case class Coalesce(children: Seq[Expression]) extends Expression {
  override def nullable: Boolean = children.exists(_.nullable)

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = ???

  /** Returns Java source code that can be compiled to evaluate this expression.
    * The default behavior is to call the eval method of the expression. Concrete expression
    * implementations should override this to do actual code generation.
    *
    * @param ctx a [[CodegenContext]]
    * @param ev  an [[ExprCode]] with unique terms， which is to be assigned value of the expression.
    * @return an [[ExprCode]] containing the Java source code to generate the given expression
    */
  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = ???

  /** Returns the [[DataType]] of the result of evaluating this expression.  It is
    * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
    */
  override def dataType: DataType = children.head.dataType
}
