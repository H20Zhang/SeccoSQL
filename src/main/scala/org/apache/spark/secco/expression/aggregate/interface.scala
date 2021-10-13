package org.apache.spark.secco.expression.aggregate

import org.apache.spark.secco.expression.{
  ExprId,
  Expression,
  NamedExpression,
  Unevaluable
}
import org.apache.spark.secco.types.DataType

//TODO: implement this
abstract class AggregateFunction extends Expression {

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false

  def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }

  /** String representation used in explain plans. */
  def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + flatArguments.mkString(start, ", ", ")")
  }
}

object AggregateExpression {

  def apply(
      aggregateFunction: AggregateFunction,
      isDistinct: Boolean
  ): AggregateExpression = {
    AggregateExpression(
      aggregateFunction,
      isDistinct,
      NamedExpression.newExprId
    )
  }

}

/** A container for an [[AggregateFunction]] with  a field
  * (`isDistinct`) indicating if DISTINCT keyword is specified for this function.
  */
case class AggregateExpression(
    aggregateFunction: AggregateFunction,
    isDistinct: Boolean,
    resultId: ExprId
) extends Expression
    with Unevaluable {

  override def children: Seq[Expression] = aggregateFunction :: Nil
  override def dataType: DataType = aggregateFunction.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = aggregateFunction.nullable

}
