package org.apache.spark.secco.parsing
import scala.util.parsing.input.{NoPosition, Position, Positional, Reader}

sealed trait Token extends Positional

sealed trait Literal extends Token
case class StringLit(d: String) extends Literal
case class BooleanLit(d: Boolean) extends Literal
case class FloatLit(d: Float) extends Literal
case class DoubleLit(d: Double) extends Literal
case class LongLit(d: Long) extends Literal
case class IntLit(d: Int) extends Literal
case object NullLit extends Literal

sealed trait Operator extends Token
case object Add extends Operator
case object Sub extends Operator
case object Mul extends Operator
case object Div extends Operator
case object Mod extends Operator
case object Eq extends Operator
case object NotEq extends Operator
case object Leq extends Operator
case object Le extends Operator
case object Geq extends Operator
case object Ge extends Operator
case object Lp extends Operator
case object Rp extends Operator
case object Sim extends Operator
case object Dot extends Operator
case object Com extends Operator

case class Identifier(d: String) extends Token

sealed trait Keyword extends Token
case object With extends Keyword
case object Recursive extends Keyword
case object As extends Keyword
case object Select extends Keyword
case object Distinct extends Keyword
case object All extends Keyword
case object From extends Keyword
case object Where extends Keyword
case object GroupBy extends Keyword
case object Having extends Keyword
case object And extends Keyword
case object Or extends Keyword
case object Any extends Keyword
case object Exists extends Keyword
case object Inner extends Keyword
case object In extends Keyword
case object Not extends Keyword
case object IsNull extends Keyword
case object IsNotNull extends Keyword
case object Case extends Keyword
case object When extends Keyword
case object Then extends Keyword
case object Else extends Keyword
case object Join extends Keyword
case object On extends Keyword
case object Using extends Keyword
case object LeftOuter extends Keyword
case object RightOuter extends Keyword
case object FullOuter extends Keyword
case object Natural extends Keyword
case object Union extends Keyword
case object Intersect extends Keyword
case object Except extends Keyword
case object ByUpdate extends Keyword
case object OrderBy extends Keyword
case object Limit extends Keyword
case object Asc extends Keyword
case object Desc extends Keyword

case class TokenReader(tokens: Seq[Token]) extends Reader[Token] {
  override def first: Token = tokens.head
  override def atEnd: Boolean = tokens.isEmpty
  override def pos: Position =
    tokens.headOption
      .map { x =>
        x.pos
      }
      .getOrElse(NoPosition)
  override def rest: Reader[Token] = new TokenReader(tokens.tail)
}
