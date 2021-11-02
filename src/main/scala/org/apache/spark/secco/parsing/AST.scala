package org.apache.spark.secco.parsing
import scala.util.parsing.input.Positional

/* The file that defines the AST for parsing. */

sealed trait AST extends Positional

sealed trait Expr extends AST
case class ColumnRef(qualifier: Option[Identifier], name: Identifier)
    extends Expr {
  def nameParts: Seq[String] = {
    if (qualifier.isEmpty) {
      name.d :: Nil
    } else {
      qualifier.get.d :: name.d :: Nil
    }
  }
}
case class Star(qualifier: Option[Identifier]) extends Expr
case class AppExpr(fun: Identifier, args: Seq[Expr]) extends Expr
case class LiteralExpr(literal: Literal) extends Expr
case class QueryExpr(query: Query) extends Expr
case class MulExpr(lhs: Expr, rhs: Expr) extends Expr
case class DivExpr(lhs: Expr, rhs: Expr) extends Expr
case class ModExpr(lhs: Expr, rhs: Expr) extends Expr
case class PosExpr(expr: Expr) extends Expr
case class NegExpr(expr: Expr) extends Expr
case class AddExpr(lhs: Expr, rhs: Expr) extends Expr
case class SubExpr(lhs: Expr, rhs: Expr) extends Expr
case class IsNotNullExpr(expr: Expr) extends Expr
case class IsNullExpr(expr: Expr) extends Expr
case class ExistsExpr(query: Query) extends Expr
case class EqExpr(lhs: Expr, rhs: Expr) extends Expr
case class NotEqExpr(lhs: Expr, rhs: Expr) extends Expr
case class LeExpr(lhs: Expr, rhs: Expr) extends Expr
case class LeqExpr(lhs: Expr, rhs: Expr) extends Expr
case class GeExpr(lhs: Expr, rhs: Expr) extends Expr
case class GeqExpr(lhs: Expr, rhs: Expr) extends Expr
case class InExpr(lhs: Expr, rhs: Expr) extends Expr
case class NotExpr(expr: Expr) extends Expr
case class AndExpr(lhs: Expr, rhs: Expr) extends Expr
case class OrExpr(lhs: Expr, rhs: Expr) extends Expr

case class Projection(expression: Expr, alias: Option[Identifier]) extends AST

sealed trait TableRef extends AST
case class StoredTable(name: Identifier, alias: Option[Identifier])
    extends TableRef
case class DerivedTable(query: Query, alias: Identifier) extends TableRef
case class JoinedTable(
    lhs: TableRef,
    rhs: TableRef,
    joinType: JoinType,
    joinCondition: Option[JoinCondition]
) extends TableRef

sealed trait JoinType extends AST
case object NaturalJoin extends JoinType
case object InnerJoin extends JoinType
case object LeftOuterJoin extends JoinType
case object RightOuterJoin extends JoinType
case object FullOuterJoin extends JoinType

sealed trait JoinCondition extends AST
case class JoinOn(condition: Expr) extends JoinCondition
case class JoinUsing(columns: Seq[Identifier]) extends JoinCondition

sealed trait Query extends AST

/* Table operations */
case class SelectStmt(
    distinct: Boolean,
    projections: Seq[Projection],
    from: Seq[TableRef],
    where: Option[Expr],
    groupBy: Option[Seq[ColumnRef]],
    having: Option[Expr],
    orderBy: Option[Seq[(ColumnRef, Boolean)]],
    limit: Option[Int]
) extends Query

/* Set operations */
case class UnionStmt(lhs: Query, rhs: Query, all: Boolean) extends Query
case class UnionByUpdateStmt(lhs: Query, rhs: Query, columns: Seq[Identifier])
    extends Query
case class IntersectStmt(lhs: Query, rhs: Query) extends Query
case class ExceptStmt(lhs: Query, rhs: Query) extends Query

/* CTE operations */
case class WithStmt(
    recursive: Option[Option[Int]],
    withList: Seq[WithElem],
    query: Query
) extends Query

case class WithElem(
    name: Identifier,
    columns: Option[Seq[Identifier]],
    query: Query
) extends AST
