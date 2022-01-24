package org.apache.spark.secco

import org.apache.spark.secco.expression._
import org.apache.spark.secco.types._

/**
  * A collection of implicit conversions that create a DSL for constructing catalyst data structures.
  *
  * {{{
  *  scala> import org.apache.spark.sql.catalyst.dsl.expressions._
  *
  *  // Standard operators are added to expressions.
  *  scala> import org.apache.spark.sql.catalyst.expressions.Literal
  *  scala> Literal(1) + Literal(1)
  *  res0: org.apache.spark.sql.catalyst.expressions.Add = (1 + 1)
  *
  *  // There is a conversion from 'symbols to unresolved attributes.
  *  scala> 'a.attr
  *  res1: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute = 'a
  *
  *  // These unresolved attributes can be used to create more complicated expressions.
  *  scala> 'a === 'b
  *  res2: org.apache.spark.sql.catalyst.expressions.EqualTo = ('a = 'b)
  *
  *  // SQL verbs can be used to construct logical query plans.
  *  scala> import org.apache.spark.sql.catalyst.plans.logical._
  *  scala> import org.apache.spark.sql.catalyst.dsl.plans._
  *  scala> LocalRelation('key.int, 'value.string).where('key === 1).select('value).analyze
  *  res3: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
  *  Project [value#3]
  *   Filter (key#2 = 1)
  *    LocalRelation [key#2,value#3], []
  * }}}
  */
package object dsl {
  trait ImplicitOperators {
    def expr: Expression

    def unary_+ : Expression = UnaryPositive(expr)
    def unary_- : Expression = UnaryMinus(expr)
    def unary_! : Expression = Not(expr)
//    def unary_~ : Expression = BitwiseNot(expr)

    def + (other: Expression): Expression = Add(expr, other)
    def - (other: Expression): Expression = Subtract(expr, other)
    def * (other: Expression): Expression = Multiply(expr, other)
    def / (other: Expression): Expression = Divide(expr, other)
//    def div (other: Expression): Expression = IntegralDivide(expr, other)

    def && (other: Expression): Predicate = And(expr, other)
    def || (other: Expression): Predicate = Or(expr, other)

    def < (other: Expression): Predicate = LessThan(expr, other)
    def <= (other: Expression): Predicate = LessThanOrEqual(expr, other)
    def > (other: Expression): Predicate = GreaterThan(expr, other)
    def >= (other: Expression): Predicate = GreaterThanOrEqual(expr, other)
    def === (other: Expression): Predicate = EqualTo(expr, other)

    def isNull: Predicate = IsNull(expr)
    def isNotNull: Predicate = IsNotNull(expr)

    def cast(to: DataType): Expression = {
      if (expr.resolved && expr.dataType.sameType(to)) {
        expr
      } else {
        val cast = Cast(expr, to)
//        cast.setTagValue(Cast.USER_SPECIFIED_CAST, true)
        cast
      }
    }
  }

  trait ExpressionConversions {
    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      def expr: Expression = e
    }

    implicit def booleanToLiteral(b: Boolean): Literal = Literal(b)
    implicit def intToLiteral(i: Int): Literal = Literal(i)
    implicit def longToLiteral(l: Long): Literal = Literal(l)
    implicit def doubleToLiteral(d: Double): Literal = Literal(d)
    implicit def floatToLiteral(f: Float): Literal = Literal(f)
    implicit def stringToLiteral(s: String): Literal = Literal(s, StringType)

    def greatest(args: Expression*): Expression = Greatest(args)
    def least(args: Expression*): Expression = Least(args)

    def coalesce(args: Expression*): Expression = Coalesce(args)
  }

  object expression extends ExpressionConversions
}
