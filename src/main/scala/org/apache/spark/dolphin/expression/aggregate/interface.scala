package org.apache.spark.dolphin.expression.aggregate

import org.apache.spark.dolphin.expression.Expression

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
