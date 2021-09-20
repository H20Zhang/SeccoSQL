package org.apache.spark.secco.expression.aggregate

import org.apache.spark.secco.codegen.{CodegenContext, ExprCode}
import org.apache.spark.secco.execution.storage.row
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Expression
import org.apache.spark.secco.types.DataType

/** Return the minimum value of `expr`
  *
  * @param child the child `expr`
  */
case class Min(child: Expression) extends AggregateFunction {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = ???

  override def dataType: DataType = child.dataType

  override def children: Seq[Expression] = child :: Nil

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = ???
}

/** Return the max value of `expr`
  * @param child the child `expr`
  */
case class Max(child: Expression) extends AggregateFunction {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = ???

  override def dataType: DataType = child.dataType

  override def children: Seq[Expression] = child :: Nil

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = ???
}

/** Return the numbers of value of `expr`
  * @param child the child `expr`
  */
case class Count(child: Expression) extends AggregateFunction {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = ???

  override def dataType: DataType = child.dataType

  override def children: Seq[Expression] = child :: Nil

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = ???
}

/** Return the average value of `expr`
  * @param child the child `expr`
  */
case class Average(child: Expression) extends AggregateFunction {
  override def nullable: Boolean = true

  override def eval(input: row.InternalRow): Any = ???

  override def dataType: DataType = child.dataType

  override def children: Seq[Expression] = child :: Nil

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
}

/** Return the sum value of `expr`
  * @param child the child `expr`
  */
case class Sum(child: Expression) extends AggregateFunction {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = ???

  override def dataType: DataType = child.dataType

  override def children: Seq[Expression] = child :: Nil

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = ???
}
