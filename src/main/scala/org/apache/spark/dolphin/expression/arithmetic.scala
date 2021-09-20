package org.apache.spark.dolphin.expression

import org.apache.spark.dolphin.codegen.Block.BlockHelper
import org.apache.spark.dolphin.codegen.{
  CodeGenerator,
  CodegenContext,
  ExprCode
}
import org.apache.spark.dolphin.execution.storage.row.InternalRow
import org.apache.spark.dolphin.types.{
  AbstractDataType,
  DataType,
  DoubleType,
  FloatType,
  FractionalType,
  IntegralType,
  NumericType
}

case class UnaryPositive(child: Expression) extends UnaryExpression {
  override def prettyName: String = "positive"

  override def dataType: DataType = child.dataType

  override def sql: String = s"(+ ${child.sql})"

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = {
    defineCodeGen(ctx, ev, c => c)
  }

  override protected def nullSafeEval(input: Any): Any = input
}

case class UnaryMinus(child: Expression) extends UnaryExpression {

  override def toString: String = s"-$child"

  override def dataType: DataType = child.dataType

  override def sql: String = s"(- ${child.sql})"

  private lazy val numeric =
    dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    dataType match {
      case dt: NumericType =>
        nullSafeCodeGen(
          ctx,
          ev,
          eval => {
            val originValue = ctx.freshName("origin")
            // codegen would fail to compile if we just write (-($c))
            // for example, we could not write --9223372036854775808L in code
            s"""
        ${CodeGenerator.javaType(dt)} $originValue = (${CodeGenerator.javaType(
              dt
            )})($eval);
        ${ev.value} = (${CodeGenerator.javaType(dt)})(-($originValue));
      """
          }
        )
    }

  protected override def nullSafeEval(input: Any): Any = {
    numeric.negate(input)
  }
}

abstract class BinaryArithmetic extends BinaryOperator {
  override def dataType: DataType = left.dataType

  override lazy val resolved: Boolean = childrenResolved

//  /** Name of the function for this expression on a [[Decimal]] type. */
//  def decimalMethod: String =
//    sys.error("BinaryArithmetics must override either decimalMethod or genCode")
//
//  /** Name of the function for this expression on a [[CalendarInterval]] type. */
//  def calendarIntervalMethod: String =
//    sys.error("BinaryArithmetics must override either calendarIntervalMethod or genCode")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    dataType match {
      case _ =>
        defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
    }
}

object BinaryArithmetic {
  def unapply(e: BinaryArithmetic): Option[(Expression, Expression)] =
    Some((e.left, e.right))
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "+"

  override def inputType: AbstractDataType = NumericType

  private lazy val numeric =
    dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    numeric.plus(input1, input2)
  }
}

case class Subtract(left: Expression, right: Expression)
    extends BinaryArithmetic {
  override def symbol: String = "-"

  override def inputType: AbstractDataType = left.dataType

  private lazy val numeric =
    dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    numeric.minus(input1, input2)
  }
}

case class Multiply(left: Expression, right: Expression)
    extends BinaryArithmetic {
  override def symbol: String = "*"

  override def inputType: AbstractDataType = NumericType

  private lazy val numeric =
    dataType.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]

  override protected def nullSafeEval(input1: Any, input2: Any): Any = {
    numeric.times(input1, input2)
  }
}

// Common base trait for Divide and Remainder, since these two classes are almost identical
trait DivModLike extends BinaryArithmetic {

  override def nullable: Boolean = true

  final override def eval(input: InternalRow): Any = {
    val input2 = right.eval(input)
    if (input2 == null || input2 == 0) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        evalOperation(input1, input2)
      }
    }
  }

  def evalOperation(left: Any, right: Any): Any

  /**
    * Special case handling due to division/remainder by 0 => null.
    */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val isZero =
//      if (dataType.isInstanceOf[DecimalType]) {
//      s"${eval2.value}.isZero()"
//    } else {
      s"${eval2.value} == 0"
//    }

    val javaType = CodeGenerator.javaType(dataType)
    val operation =
//      if (dataType.isInstanceOf[DecimalType]) {
//      s"${eval1.value}.$decimalMethod(${eval2.value})"
//    } else {
      s"($javaType)(${eval1.value} $symbol ${eval2.value})"
//    }

    if (!left.nullable && !right.nullable) {
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if ($isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          ${ev.value} = $operation;
        }""")
    } else {
      ev.copy(code = code"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        if (${eval2.isNull} || $isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          if (${eval1.isNull}) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = $operation;
          }
        }""")
    }
  }
}

case class Divide(left: Expression, right: Expression) extends DivModLike {
  override def symbol: String = "/"

  override def inputType: AbstractDataType = NumericType

  private lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
  }

  override def evalOperation(left: Any, right: Any): Any = div(left, right)
}

case class Remainder(left: Expression, right: Expression) extends DivModLike {
  override def symbol: String = "%"

  override def inputType: AbstractDataType = NumericType

  private lazy val mod: (Any, Any) => Any = dataType match {
    // special cases to make float/double primitive types faster
    case DoubleType =>
      (left, right) => left.asInstanceOf[Double] % right.asInstanceOf[Double]
    case FloatType =>
      (left, right) => left.asInstanceOf[Float] % right.asInstanceOf[Float]

    // catch-all cases
    case i: IntegralType =>
      val integral = i.integral.asInstanceOf[Integral[Any]]
      (left, right) => integral.rem(left, right)
    case i: FractionalType => // should only be DecimalType for now
      val integral = i.asIntegral.asInstanceOf[Integral[Any]]
      (left, right) => integral.rem(left, right)
  }

  override def evalOperation(left: Any, right: Any): Any = mod(left, right)
}
