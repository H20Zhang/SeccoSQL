package org.apache.spark.secco.parsing
import org.apache.spark.secco.catalog.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.types.{DataType, StructType}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.Position

case class ParserError(p: Position, msg: String)

object SQLParser extends Parsers with ParserInterface {
  override type Elem = Token

  //----------------------------------------------functions to build the parser----------------------------------------------
  def identifier: Parser[Identifier] =
    positioned {
      accept("identifier", { case d: Identifier => d })
    }
  def literal: Parser[Literal] =
    positioned {
      accept("literal", { case d: Literal => d })
    }
  def intLit: Parser[IntLit] =
    positioned {
      accept("int literal", { case d: IntLit => d })
    }

  //TODO handle case when then else
  def star: Parser[Star] =
    positioned {
      opt(identifier <~ Dot) <~ Mul ^^ { case qualifier => Star(qualifier) }
    }
  def expr_1 =
    positioned {
      identifier ~ (Lp ~> repsep(expression | star, Com) <~ Rp) ^^ {
        case fun ~ args => AppExpr(fun, args)
      } |
        opt(identifier <~ Dot) ~ identifier ^^ { case qualifier ~ name =>
          ColumnRef(qualifier, name)
        } |
        literal ^^ { case literal => LiteralExpr(literal) } |
        Lp ~> expression <~ Rp ^^ { case expr => expr } |
        Lp ~> query <~ Rp ^^ { case query => QueryExpr(query) }
    }
  def expr_2 =
    positioned {
      expr_1 * (Mul ^^^ { (lhs: Expr, rhs: Expr) =>
        MulExpr(lhs, rhs)
      } |
        Div ^^^ { (lhs: Expr, rhs: Expr) =>
          DivExpr(lhs, rhs)
        } |
        Mod ^^^ { (lhs: Expr, rhs: Expr) =>
          ModExpr(lhs, rhs)
        })
    }
  def expr_3 =
    positioned {
      (rep(Add | Sub) ~ expr_2 ^^ { case lst ~ expr =>
        lst.foldRight(expr) { (op: Token, expr: Expr) =>
          op match {
            case Add => PosExpr(expr)
            case Sub => NegExpr(expr)
            case _   => throw new Exception("unreachable")
          }
        }
      }) * (Add ^^^ { (lhs: Expr, rhs: Expr) =>
        AddExpr(lhs, rhs)
      } |
        Sub ^^^ { (lhs: Expr, rhs: Expr) =>
          SubExpr(lhs, rhs)
        })
    }
  def expr_4 =
    positioned {
      expr_3 <~ IsNull ^^ { case expr => IsNullExpr(expr) } |
        expr_3 <~ IsNotNull ^^ { case expr => IsNotNullExpr(expr) } |
        Exists ~> Lp ~> query <~ Rp ^^ { case query => ExistsExpr(query) } |
        expr_3 * (Eq ^^^ { (lhs: Expr, rhs: Expr) =>
          EqExpr(lhs, rhs)
        } |
          NotEq ^^^ { (lhs: Expr, rhs: Expr) =>
            NotEqExpr(lhs, rhs)
          } |
          Le ^^^ { (lhs: Expr, rhs: Expr) =>
            LeExpr(lhs, rhs)
          } |
          Leq ^^^ { (lhs: Expr, rhs: Expr) =>
            LeqExpr(lhs, rhs)
          } |
          Ge ^^^ { (lhs: Expr, rhs: Expr) =>
            GeExpr(lhs, rhs)
          } |
          Geq ^^^ { (lhs: Expr, rhs: Expr) =>
            GeqExpr(lhs, rhs)
          } |
          In ^^^ { (lhs: Expr, rhs: Expr) =>
            InExpr(lhs, rhs)
          } |
          Not ~ In ^^^ { (lhs: Expr, rhs: Expr) =>
            NotExpr(InExpr(lhs, rhs))
          })
    }
  def expr_5 =
    positioned {
      Not ~> expr_4 ^^ { case expr => NotExpr(expr) } |
        expr_4
    }
  def expr_6 =
    positioned {
      expr_5 * (And ^^^ { (lhs: Expr, rhs: Expr) =>
        AndExpr(lhs, rhs)
      })
    }
  //TODO handle all/any/in
  def expr_7 =
    positioned {
      expr_6 * (Or ^^^ { (lhs: Expr, rhs: Expr) =>
        OrExpr(lhs, rhs)
      })
    }
  def expression: Parser[Expr] = positioned { expr_7 }

  def tableRef: Parser[TableRef] = positioned { join }
  def tablePrimary: Parser[TableRef] =
    positioned {
      table | derivedTable | Lp ~> join <~ Rp
    }
  def table =
    positioned {
      (identifier <~ opt(As)) ~ opt(identifier) ^^ { case name ~ alias =>
        StoredTable(name, alias)
      }
    }
  def derivedTable =
    positioned {
      ((Lp ~> query <~ Rp) <~ opt(As)) ~ identifier ^^ { case query ~ alias =>
        DerivedTable(query, alias)
      }
    }
  def join =
    positioned {
      tablePrimary ~ rep(
        (joinType <~ Join) ~ tablePrimary ~ opt(joinCondition) ^^ {
          case joinType ~ query ~ joinCondition =>
            (joinType, query, joinCondition)
        }
      ) ^^ { case x ~ xs =>
        xs.foldLeft(x) { (x, y) =>
          y match {
            case (joinType, query, joinCondition) =>
              JoinedTable(x, query, joinType, joinCondition)
          }
        }
      }
    }
  def joinType =
    positioned {
      Inner ^^^ InnerJoin |
        Natural ^^^ NaturalJoin |
        LeftOuter ^^^ LeftOuterJoin |
        RightOuter ^^^ RightOuterJoin |
        FullOuter ^^^ FullOuterJoin |
        success(InnerJoin)
    }
  def joinCondition =
    positioned {
      On ~> expression ^^ { case cond => JoinOn(cond) } |
        Using ~> Lp ~> rep1sep(identifier, Com) <~ Rp ^^ { case col =>
          JoinUsing(col)
        }
    }

  def query: Parser[Query] =
    positioned {
      selectStmt | exceptStmt | intersectStmt | unionByUpdateStmt | unionStmt | withStmt
    }
  def selectStmt =
    positioned {
      Select ~> (opt(Distinct) ~
        projections <~
        From) ~ rep1sep(tableRef, Com) ~
        opt(Where ~> expression) ~
        opt(
          GroupBy ~> repsep(
            opt(identifier <~ Dot) ~ identifier ^^ { case qualifier ~ name =>
              ColumnRef(qualifier, name)
            },
            Com
          )
        ) ~
        opt(Having ~> expression) ~
        opt(
          OrderBy ~> repsep(
            opt(identifier <~ Dot) ~ identifier ~ opt(Asc | Desc) ^^ {
              case qualifier ~ name ~ order =>
                val isAsc = order.forall {
                  case Asc  => true
                  case Desc => false
                }
                (ColumnRef(qualifier, name), isAsc)
            },
            Com
          )
        ) ~
        opt(Limit ~> intLit) ^^ {
          case distinct ~ projections ~ from ~ where ~ groupBy ~ having ~ orderBy ~ limit => {
            SelectStmt(
              distinct.isDefined,
              projections,
              from,
              where,
              groupBy,
              having,
              orderBy,
              limit.map(_.d)
            )
          }
        }
    }
  def unionByUpdateStmt =
    positioned {
      ((Lp ~> query <~ Rp) <~ Union <~ ByUpdate) ~ rep1sep(
        identifier,
        Com
      ) ~ (Lp ~> query <~ Rp) ^^ { case lhs ~ columns ~ rhs =>
        UnionByUpdateStmt(lhs, rhs, columns)
      }
    }
  def unionStmt =
    positioned {
      (Lp ~> query <~ Rp) * (Union ~> opt(All) ^^ {
        case all => { (lhs: Query, rhs: Query) =>
          UnionStmt(lhs, rhs, all.isDefined)
        }
      })
    }

  def exceptStmt = positioned {
    (((Lp ~> query <~ Rp) <~ Except) ~ (Lp ~> query <~ Rp)) ^^ {
      case lhs ~ rhs =>
        ExceptStmt(lhs, rhs)
    }
  }

  def intersectStmt = positioned {
    ((Lp ~> query <~ Rp) <~ Intersect) ~ (Lp ~> query <~ Rp) ^^ {
      case lhs ~ rhs =>
        IntersectStmt(lhs, rhs)
    }
  }

  def withElem =
    (identifier ~ opt(
      Lp ~> rep1sep(identifier, Com) <~ Rp
    ) <~ As) ~ (Lp ~> query <~ Rp) ^^ { case name ~ columns ~ query =>
      WithElem(name, columns, query)
    }
  def withStmt: Parser[Query] =
    positioned {
      With ~> opt(
        Recursive ~> opt(Lp ~> intLit <~ Rp ^^ { case x: IntLit => x.d })
      ) ~ rep1sep(withElem, Com) ~ query ^^ {
        case recursive ~ withList ~ query =>
          WithStmt(recursive, withList, query)
      }
    }

  def projections: Parser[Seq[Projection]] = rep1sep(projection, Com)
  def projection =
    positioned {
      star ^^ { case expression => Projection(expression, None) } |
        expression ~ opt(As ~> identifier) ^^ { case expression ~ alias =>
          Projection(expression, alias)
        }
    }

  //----------------------------------------------Main Parsing method----------------------------------------------

  def tokens(sqlText: String) = {
    SQLLexer(sqlText) match {
      case Left(error) =>
        throw new ParseException(
          Some(sqlText),
          error.msg + "\n" + error.p.longString,
          Origin(Some(error.p.line), Some(error.p.column)),
          Origin(None, None)
        )
      case Right(tokens) => tokens
    }
  }

  def apply(d: Seq[Token]) = {
    phrase(query)(TokenReader(d)) match {
      case NoSuccess(msg, next)  => Left(ParserError(next.pos, msg))
      case Success(result, next) => Right(result)
    }
  }

  def apply(sqlText: String) = {
    phrase(query <~ opt(Sim))(TokenReader(tokens(sqlText))) match {
      case NoSuccess(msg, next) =>
        Left(
          new ParseException(
            Some(sqlText),
            msg + "\n" + next.pos.longString,
            Origin(Some(next.pos.line), Some(next.pos.column)),
            Origin(None, None)
          )
        )
      case Success(result, _) => Right(result)
    }
  }

  def parseAST(sqlText: String): AST = {
    apply(sqlText) match {
      case Left(exception) => throw exception
      case Right(q)        => q
    }
  }

  override def parsePlan(sqlText: String): LogicalPlan = {
    phrase(query <~ opt(Sim))(TokenReader(tokens(sqlText))) match {
      case NoSuccess(msg, next) =>
        throw new ParseException(
          Some(sqlText),
          msg + "\n" + next.pos.longString,
          Origin(Some(next.pos.line), Some(next.pos.column)),
          Origin(None, None)
        )
      case Success(result, _) => LogicalPlanBuilder.fromQuery(result)
    }
  }
  override def parseExpression(sqlText: String) = {
    phrase(expression)(TokenReader(tokens(sqlText))) match {
      case NoSuccess(msg, next) =>
        throw new ParseException(
          Some(sqlText),
          msg + "\n" + next.pos.longString,
          Origin(Some(next.pos.line), Some(next.pos.column)),
          Origin(None, None)
        )
      case Success(result, _) => LogicalPlanBuilder.fromExpression(result)
    }
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    phrase(identifier)(TokenReader(tokens(sqlText))) match {
      case NoSuccess(msg, next) =>
        throw new ParseException(
          Some(sqlText),
          msg + "\n" + next.pos.longString,
          Origin(Some(next.pos.line), Some(next.pos.column)),
          Origin(None, None)
        )
      case Success(result, _) =>
        TableIdentifier(LogicalPlanBuilder.fromTableIdentifier(result))
    }
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    ???

  override def parseTableSchema(sqlText: String): StructType = ???

  override def parseDataType(sqlText: String): DataType = ???
}
