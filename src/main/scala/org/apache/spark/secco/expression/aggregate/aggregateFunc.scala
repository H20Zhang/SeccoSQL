package org.apache.spark.secco.expression.aggregate

import org.apache.spark.secco.codegen.{CodegenContext, ExprCode}
import org.apache.spark.secco.execution.storage.row
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression._
import org.apache.spark.secco.types._
import org.apache.spark.secco.dsl.expression._

/** Return the minimum value of `expr`
  *
  * @param child the child `expr`
  */
case class Min(child: Expression) extends DeclarativeAggregate {

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def children: Seq[Expression] = child :: Nil

  private lazy val min = AttributeReference("min", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = min :: Nil

  override val initialValues: Seq[Expression] = Seq(
    /* min = */ Literal(null, child.dataType)
  )

  override val updateExpressions: Seq[Expression] = Seq(
    /* min = */ least(min, child)
  )

  override val mergeExpressions: Seq[Expression] = Seq(
    /* min = */ least(min.left, min.right)
  )

  override val evaluateExpression: Expression = min
}

/** Return the max value of `expr`
  * @param child the child `expr`
  */
case class Max(child: Expression) extends DeclarativeAggregate {

  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def children: Seq[Expression] = child :: Nil

  private lazy val max = AttributeReference("max", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = max :: Nil

  override val initialValues: Seq[Expression] = Seq(
    /* max = */ Literal(null, child.dataType)
  )

  override val updateExpressions: Seq[Expression] = Seq(
    /* max = */ greatest(max, child)
  )

  override val mergeExpressions: Seq[Expression] = {
    Seq(
      /* max = */ greatest(max.left, max.right)
    )
  }

  override val evaluateExpression: Expression = max

}

/** Return the numbers of value of `expr`
  * @param child the child `expr`
  */
case class Count(child: Expression) extends DeclarativeAggregate {

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  override def children: Seq[Expression] = child :: Nil

  private lazy val count =
    AttributeReference("count", LongType, nullable = false)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = count :: Nil

  override val initialValues: Seq[Expression] = Seq(
    /* count = */ Literal(0L)
  )

  override val updateExpressions: Seq[Expression] = {
    val nullableChildren = children.filter(_.nullable)
    if (nullableChildren.isEmpty) {
      Seq(
        /* count = */ count + 1L
      )
    } else {
      Seq(
        /* count = */ If(
          nullableChildren.map(IsNull).reduce(Or),
          count,
          count + 1L
        )
      )
    }
  }
//    Seq(
//    /* count = */ count + 1L
//  )

  override val mergeExpressions: Seq[Expression] = Seq(
    /* count = */ count.left + count.right
  )

  override val evaluateExpression: Expression = count
}

/** Return the average value of `expr`
  * @param child the child `expr`
  */
case class Average(child: Expression)
    extends DeclarativeAggregate
    with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def prettyName: String = "avg"

  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  private lazy val resultType = DoubleType

  override def children: Seq[Expression] = child :: Nil

  private lazy val sum = AttributeReference("sum", resultType)()

  private lazy val count = AttributeReference("count", LongType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    Seq(sum, count)

  override val initialValues: Seq[Expression] = Seq(
    /* sum = */ Literal.default(resultType),
    /* count = */ Literal(0L)
  )

  override val updateExpressions: Seq[Expression] = Seq(
    /* sum = */ Add(
      sum,
      coalesce(child.cast(resultType), Literal.default(resultType))
    ),
    /* count = */ If(child.isNull, count, count + 1L)
  )

  override val mergeExpressions: Seq[Expression] = Seq(
    /* sum = */ sum.left + sum.right,
    /* count = */ count.left + count.right
  )

  override val evaluateExpression: Expression =
    Divide(sum.cast(resultType), count.cast(resultType))
}

/** Return the sum value of `expr`
  * @param child the child `expr`
  */
case class Sum(child: Expression) extends DeclarativeAggregate {

  override def nullable: Boolean = true

  override def dataType: DataType = resultType

//  private lazy val resultType = child.dataType match {
//    case DecimalType.Fixed(precision, scale) =>
//      DecimalType.bounded(precision + 10, scale)
//    case _: IntegralType => LongType
//    case it: YearMonthIntervalType => it
//    case it: DayTimeIntervalType => it
//    case _ => DoubleType
//  }

  private lazy val resultType = child.dataType

  override def children: Seq[Expression] = child :: Nil

  private lazy val sum = AttributeReference("sum", resultType)()

  private lazy val zero = Literal.default(resultType)

  override lazy val aggBufferAttributes: Seq[AttributeReference] = sum :: Nil

  override val initialValues: Seq[Expression] = Seq(
    /* sum = */ Literal(null, resultType)
  )

  override val updateExpressions: Seq[Expression] =
    // For non-decimal type, the initial value of `sum` is null, which indicates no value.
    // We need `coalesce(sum, zero)` to start summing values. And we need an outer `coalesce`
    // in case the input is nullable. The `sum` can only be null if there is no value, as
    // non-decimal type can produce overflowed value under non-ansi mode.
    if (child.nullable) {
      Seq(coalesce(coalesce(sum, zero) + child, sum)) // edited by lgh
    } else {
      Seq(coalesce(sum, zero) + child) // edited by lgh
    }

  override val mergeExpressions: Seq[Expression] = Seq(
    coalesce(coalesce(sum.left, zero) + sum.right, sum.left) // edited by lgh
  )

  override val evaluateExpression: Expression = sum
}
