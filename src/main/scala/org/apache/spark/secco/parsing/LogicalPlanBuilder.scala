package org.apache.spark.secco.parsing

import org.apache.spark.secco.expression.Literal
import org.apache.spark.secco.optimization.plan.{
  JoinType,
  UnionByUpdate,
  Update
}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan, plan => L}
import org.apache.spark.secco.types.{
  BooleanType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType
}
import org.apache.spark.secco.{expression => E}
import org.apache.spark.secco.{analysis => A}

/**
  * The builder for building the logical operator tree based on abstract syntax tree obtained by parser.
  * It is worth noting that currently, we only support natural join
  */
object LogicalPlanBuilder {

  def fromJoinType(joinType: JoinType) =
    joinType match {
      case InnerJoin      => JoinType.Inner
      case LeftOuterJoin  => JoinType.LeftOuter
      case RightOuterJoin => JoinType.RightOuter
      case FullOuterJoin  => JoinType.FullOuter
    }

  def fromTableRef(ref: TableRef): LogicalPlan = {
    ref match {
      case Table(Identifier(name), alias) =>
        val tab = A.UnresolvedRelation(name)
        alias
          .map { case Identifier(alias) => L.SubqueryAlias(alias, tab) }
          .getOrElse(tab)
      case DerivedTable(query, Identifier(alias)) =>
        L.SubqueryAlias(alias, fromQuery(query))
      case JoinedTable(lhs, rhs, joinType, Some(JoinOn(cond))) =>
        throw new Exception(
          s"joinType:${joinType} is not supported, we only support natural join now."
        )
      case JoinedTable(lhs, rhs, joinType @ NaturalJoin, None) =>
        //warning: try to remove subquery alias without checking by assuming subquery is non-correlated subquery
        val leftTable = fromTableRef(lhs) match {
          case L.SubqueryAlias(_, subquery, _) => subquery
          case other: LogicalPlan              => other
        }

        //warning: try to remove subquery alias without checking by assuming subquery is non-correlated subquery
        val rightTable = fromTableRef(rhs) match {
          case L.SubqueryAlias(_, subquery, _) => subquery
          case other: LogicalPlan              => other
        }

        L.Join(
          Seq(leftTable, rightTable),
          JoinType.Natural,
          ExecMode.Coupled,
          Seq()
        )
    }
  }

  //  import ParserUtils._
  def fromQuery(query: Query): LogicalPlan = {
    query match {
      case SelectStmt(
            distinct,
            projections,
            from,
            where,
            groupBy,
            having,
            sortBy,
            limit
          ) => {

        /** from R1, R2, R3, .... */
        val cartesianProductPlan = from.size match {
          case x if x == 1 => fromTableRef(from.head)
          case x if x > 1 =>
            L.CartesianProduct(from.map(fromTableRef), ExecMode.Coupled)
          case _ => throw new Exception("numbers of table in where should >= 1")
        }

        /** from R1, R2, R3, ....
          * [where cond1 [and|or] cond2 ...]
          */
        val filterPlan = where
          .map { cond =>
            L.Filter(
              cartesianProductPlan,
              Seq(),
              ExecMode.Coupled,
              Some(fromExpression(cond))
            )
          }
          .getOrElse(cartesianProductPlan)

        /** select a1 as b1, ...
          * from R1, R2, R3, ....
          * [where cond1 [and|or] cond2 ...]
          * [group by c1, c2, c3]
          */
        val projectionOrAggregatePlan = {

          val projectionList = projections.map {
            case Projection(expr, None) =>
              A.UnresolvedAlias(fromExpression(expr))
            case Projection(expr, Some(Identifier(name))) =>
              E.Alias(fromExpression(expr), name)()
          }

          if (having.isEmpty && groupBy.isEmpty) {
            L.Project(filterPlan, Seq(), ExecMode.Coupled, projectionList)
          } else {
            //TODO: support having clause
            val groups = groupBy.getOrElse(Seq()).map(fromExpression(_))
            L.Aggregate(
              filterPlan,
              groups.map(_.toString),
              (projectionList.toString(), "error"),
              Seq(),
              ExecMode.Coupled,
              groups,
              projectionList
            )
          }
        }

        /** select [distinct] a1 as b1, ...
          * from R1, R2, R3, ....
          * [where cond1 [and|or] cond2 ...]
          * [group by c1, c2, c3]
          */
        val distinctPlan = if (distinct) {
          L.Distinct(projectionOrAggregatePlan)
        } else {
          projectionOrAggregatePlan
        }

        /** select [distinct] a1 as b1, ...
          * from R1, R2, R3, ....
          * [where cond1 [and|or] cond2 ...]
          * [group by c1, c2, c3]
          * [sort by d1 desc ...]
          */
        val sortPlan = sortBy
          .map(cols =>
            L.Sort(
              distinctPlan,
              cols.map(f =>
                (
                  A.UnresolvedAttribute(
                    f._1.qualifier.map(_.d).toList :+ f._1.name.d
                  ),
                  f._2
                )
              )
            )
          )
          .getOrElse(distinctPlan)

        /** select [distinct] a1 as b1, ...
          * from R1, R2, R3, ....
          * [where cond1 [and|or] cond2 ...]
          * [group by c1, c2, c3]
          * [sort by d1 desc ...]
          * [limit 100]
          */
        val limitPlan = limit
          .map(maxNumRow => L.Limit(sortPlan, maxNumRow))
          .getOrElse(sortPlan)

        limitPlan
      }
      case UnionByUpdateStmt(lhs, rhs, columns) =>
        UnionByUpdate(fromQuery(lhs), fromQuery(rhs), columns.map(_.d), false)
      case UnionStmt(lhs, rhs, true) =>
        L.Union(Seq(fromQuery(lhs), fromQuery(rhs)), ExecMode.Coupled)
      case UnionStmt(lhs, rhs, false) =>
        L.Distinct(
          L.Union(Seq(fromQuery(lhs), fromQuery(rhs)), ExecMode.Coupled)
        )
      case WithStmt(recursive, withList, query) =>
        L.With(
          recursive,
          fromQuery(query),
          withList.map {
            case WithElem(Identifier(name), columns, query) =>
              (name, columns.map(_.map(_.d)))
          },
          withList.map {
            case WithElem(Identifier(name), columns, query) => fromQuery(query)
          }
        )
    }
  }

  def fromExpression(expr: Expr): E.Expression = {
    expr match {
      case ColumnRef(qualifier, Identifier(name)) =>
        qualifier match {
          case Some(Identifier(qualifier)) =>
            A.UnresolvedAttribute(Seq(qualifier, name))
          case None => A.UnresolvedAttribute(Seq(name))
        }
      case Star(qualifier) => A.UnresolvedStar(qualifier.map(x => Seq(x.d)))
      case AppExpr(Identifier(fun), args) =>
        A.UnresolvedFunction(fun, args.map(fromExpression(_)), false)
      case LiteralExpr(literal) => {
        literal match {
          case StringLit(d)  => E.Literal(d, StringType)
          case BooleanLit(d) => E.Literal(d, BooleanType)
          case FloatLit(d)   => E.Literal(d, FloatType)
          case DoubleLit(d)  => E.Literal(d, DoubleType)
          case LongLit(d)    => E.Literal(d, LongType)
          case IntLit(d)     => E.Literal(d, IntegerType)
        }
      }
      case QueryExpr(query) => E.ScalarSubquery(fromQuery(query))
      case MulExpr(a, b) =>
        E.Multiply(fromExpression(a), fromExpression(b))
      case DivExpr(a, b) =>
        E.Divide(fromExpression(a), fromExpression(b))
      case ModExpr(a, b) =>
        E.Remainder(fromExpression(a), fromExpression(b))
      case PosExpr(a)    => E.UnaryPositive(fromExpression(a))
      case NegExpr(a)    => E.UnaryMinus(fromExpression(a))
      case AddExpr(a, b) => E.Add(fromExpression(a), fromExpression(b))
      case SubExpr(a, b) =>
        E.Subtract(fromExpression(a), fromExpression(b))
      case EqExpr(a, b) =>
        E.EqualTo(fromExpression(a), fromExpression(b))
      case NotEqExpr(a, b) =>
        E.Not(E.EqualTo(fromExpression(a), fromExpression(b)))
      case LeExpr(a, b) =>
        E.LessThan(fromExpression(a), fromExpression(b))
      case LeqExpr(a, b) =>
        E.LessThanOrEqual(fromExpression(a), fromExpression(b))
      case GeExpr(a, b) =>
        E.GreaterThan(fromExpression(a), fromExpression(b))
      case GeqExpr(a, b) =>
        E.GreaterThanOrEqual(fromExpression(a), fromExpression(b))
      case InExpr(a, b) => {
        b match {
          case QueryExpr(b) =>
            E.In(fromExpression(a), E.ListQuery(fromQuery(b)) :: Nil)
          case _ => throw new Exception("only `expr in (query)` is supported")
        }
      }
      case ExistsExpr(query) => E.Exists(fromQuery(query))
      case NotExpr(a)        => E.Not(fromExpression(a))
      case AndExpr(a, b)     => E.And(fromExpression(a), fromExpression(b))
      case OrExpr(a, b)      => E.Or(fromExpression(a), fromExpression(b))
    }
  }
  def fromTableIdentifier(iden: Identifier) = iden.d
}
