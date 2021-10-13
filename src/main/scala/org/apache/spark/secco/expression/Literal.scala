package org.apache.spark.secco.expression

import org.apache.spark.secco.types._

import java.util.Objects
import org.apache.spark.secco.codegen.{
  CodeGenerator,
  CodegenContext,
  ExprCode,
  JavaCode
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.types.TypeConverters.convertToSecco

case class Literal(value: Any, dataType: DataType) extends LeafExpression {

  override def foldable: Boolean = true

  override def nullable: Boolean = value == null

  override def eval(input: InternalRow): Any = value

  override def toString: String =
    value match {
      case null  => "null"
      case other => other.toString
    }

  override def hashCode(): Int = {
    val valueHashCode = value match {
      case null  => 0
      case other => other.hashCode()
    }
    31 * Objects.hashCode(dataType) + valueHashCode
  }

  override def equals(other: Any): Boolean =
    other match {
      case o: Literal if !dataType.equals(o.dataType) => false
      case o: Literal =>
        (value, o.value) match {
          case (null, null) => true
          case (a, b)       => a != null && a.equals(b)
        }
      case _ => false
    }

  override def sql: String =
    (value, dataType) match {
      case v: (Int, IntegerType)   => v._1.toString
      case v: (Long, LongType)     => v._2 + "L"
      case v: (Float, FloatType)   => v._2 + "f"
      case v: (Double, DoubleType) => v._2.toString
      case v: (String, StringType) => v._1
    }

  /** Returns Java source code that can be compiled to evaluate this expression.
    * The default behavior is to call the eval method of the expression. Concrete expression
    * implementations should override this to do actual code generation.
    *
    * @param ctx a [[CodegenContext]]
    * @param ev  an [[ExprCode]] with unique terms.
    * @return an [[ExprCode]] containing the Java source code to generate the given expression
    */
  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = {
    {
      val javaType = CodeGenerator.javaType(dataType)
      if (value == null) {
        ExprCode.forNullValue(dataType)
      } else {
        def toExprCode(code: String): ExprCode = {
          ExprCode.forNonNullValue(JavaCode.literal(code, dataType))
        }
        dataType match {
          case BooleanType | IntegerType =>
            toExprCode(value.toString)
          case FloatType =>
            value.asInstanceOf[Float] match {
              case v if v.isNaN =>
                toExprCode("Float.NaN")
              case Float.PositiveInfinity =>
                toExprCode("Float.POSITIVE_INFINITY")
              case Float.NegativeInfinity =>
                toExprCode("Float.NEGATIVE_INFINITY")
              case _ =>
                toExprCode(s"${value}F")
            }
          case DoubleType =>
            value.asInstanceOf[Double] match {
              case v if v.isNaN =>
                toExprCode("Double.NaN")
              case Double.PositiveInfinity =>
                toExprCode("Double.POSITIVE_INFINITY")
              case Double.NegativeInfinity =>
                toExprCode("Double.NEGATIVE_INFINITY")
              case _ =>
                toExprCode(s"${value}D")
            }
          case _ =>
            val constRef = ctx.addReferenceObj("literal", value, javaType)
            ExprCode.forNonNullValue(JavaCode.global(constRef, dataType))
        }
      }
    }
  }
}

object Literal {
  val TrueLiteral: Literal = Literal(true, BooleanType)

  val FalseLiteral: Literal = Literal(false, BooleanType)

  def apply(v: Any): Literal =
    v match {
      case i: Int     => Literal(i, IntegerType)
      case l: Long    => Literal(l, LongType)
      case d: Double  => Literal(d, DoubleType)
      case f: Float   => Literal(f, FloatType)
      case s: String  => Literal(s, StringType)
      case b: Boolean => Literal(b, BooleanType)
      case v: Literal => v
      case _ =>
        throw new RuntimeException(
          "Unsupported literal type " + v.getClass + " " + v
        )
    }

  def create(v: Any, dataType: DataType): Literal = {

    Literal(TypeConverters.convertToSecco(v), dataType)
  }

  def default(dataType: DataType): Literal = dataType match {
    case IntegerType => Literal(0)
    case LongType    => Literal(0L)
    case DoubleType  => Literal(0.0)
    case FloatType   => Literal(0.0f)
    case StringType  => Literal("")
    case BooleanType => Literal(false)
    case _ =>
      throw new RuntimeException(s"no default for type $dataType")
  }
}
