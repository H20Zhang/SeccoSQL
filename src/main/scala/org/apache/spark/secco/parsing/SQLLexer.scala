package org.apache.spark.secco.parsing
import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.Position

/* This file defines token used for parsing. */

case class LexerError(p: Position, msg: String)

//RegexParsers is more suitable to implement lexer
object SQLLexer extends RegexParsers {
  def token = positioned {
    operator | numericLit | literal_ | (identifier ||| keyword)
  }

  def literal_ =
    positioned {
      stringLit | booleanLit | nullLit
    }

  def numericLit = positioned {
    floatLit | doubleLit | longLit | intLit
  }

  def stringLit =
    positioned {
      """"([^\"\\]|\\[\\'"bfnrt])*"""".r ^^ { d =>
        StringLit(d)
      }
    }
  def booleanLit =
    positioned {
      "true" ^^^ BooleanLit(true) | "false" ^^^ BooleanLit(false)
    }
  def floatLit =
    positioned {
      """-?(\d+\.\d*|\d*\.\d+|\d+)f""".r ^^ { d =>
        FloatLit(d.toFloat)
      }
    }
  def doubleLit =
    positioned {
      """-?(\d+\.\d*|\d*\.\d+)""".r ^^ { d =>
        DoubleLit(d.toDouble)
      }
    }
  def longLit =
    positioned {
      """-?\d+l""".r ^^ { d =>
        LongLit(d.substring(0, d.length - 1).toLong)
      }
    }
  def intLit =
    positioned {
      """-?\d+""".r ^^ { d =>
        IntLit(d.toInt)
      }
    }
  def nullLit = positioned { "null" ^^^ NullLit }

  def operator =
    positioned {
      rightArrow | leftArrow | add | sub | mul | div | mod | eq_ | notEq | leq | le | geq | ge | lb | rb | lsb | rsb | lp | rp | sim | dot | com | col
    }
  def add = positioned { "+" ^^^ Add }
  def sub = positioned { "-" ^^^ Sub }
  def mul = positioned { "*" ^^^ Mul }
  def div = positioned { "/" ^^^ Div }
  def mod = positioned { "%" ^^^ Mod }
  def eq_ = positioned { "=" ^^^ Eq }
  def notEq = positioned { "!=" ^^^ NotEq }
  def leq = positioned { "<=" ^^^ Leq }
  def le = positioned { "<" ^^^ Le }
  def geq = positioned { ">=" ^^^ Geq }
  def ge = positioned { ">" ^^^ Ge }
  def lp = positioned { "(" ^^^ Lp }
  def rp = positioned { ")" ^^^ Rp }
  def lb = positioned { "{" ^^^ LCb }
  def rb = positioned { "}" ^^^ RCb }
  def lsb = positioned { "[" ^^^ LSb }
  def rsb = positioned { "]" ^^^ RSb }
  def sim = positioned { ";" ^^^ Sim }
  def dot = positioned { "." ^^^ Dot }
  def com = positioned { "," ^^^ Com }
  def col = positioned { ":" ^^^ Col }
  def rightArrow = positioned { "->" ^^^ RightArrow }
  def leftArrow = positioned { "<-" ^^^ LeftArrow }

  def identifier =
    positioned {
      """[a-zA-Z_][a-zA-Z0-9_]*""".r ^^ { d =>
        Identifier(d)
      }
    }

  def keyword =
    positioned {
      with_ ||| recursive ||| as ||| asc ||| select |||
        distinct ||| all ||| from ||| where ||| groupBy |||
        having ||| orderBy ||| and ||| or ||| any ||| exists |||
        inner ||| in ||| not ||| isNull ||| isNotNull ||| case_ |||
        when ||| then ||| else_ ||| join ||| on ||| using ||| leftOuter |||
        rightOuter ||| fullOuter ||| union ||| intersect ||| except |||
        byUpdate ||| limit ||| desc ||| natural ||| match_
    }
  def with_ = positioned { "with" ^^^ With }
  def recursive = positioned { "recursive" ^^^ Recursive }
  def as = positioned { "as" ^^^ As }
  def select = positioned { "select" ^^^ Select }
  def distinct = positioned { "distinct" ^^^ Distinct }
  def all = positioned { "all" ^^^ All }
  def from = positioned { "from" ^^^ From }
  def where = positioned { "where" ^^^ Where }
  def groupBy = positioned { "group" ~ "by" ^^^ GroupBy }
  def having = positioned { "having" ^^^ Having }
  def and = positioned { "and" ^^^ And }
  def or = positioned { "or" ^^^ Or }
  def any = positioned { "any" ^^^ Any }
  def exists = positioned { "exists" ^^^ Exists }
  def inner = positioned { "inner" ^^^ Inner }
  def in = positioned { "in" ^^^ In }
  def not = positioned { "not" ^^^ Not }
  def isNull = positioned { "is" ~ "null" ^^^ IsNull }
  def isNotNull = positioned { "is" ~ "not" ~ "null" ^^^ IsNotNull }
  def case_ = positioned { "case" ^^^ Case }
  def when = positioned { "when" ^^^ When }
  def then = positioned { "then" ^^^ Then }
  def else_ = positioned { "else" ^^^ Else }
  def join = positioned { "join" ^^^ Join }
  def on = positioned { "on" ^^^ On }
  def using = positioned { "using" ^^^ Using }
  def natural = positioned { "natural" ^^^ Natural }
  def leftOuter = positioned { "left" ~ opt("outer") ^^^ LeftOuter }
  def rightOuter = positioned { "right" ~ opt("outer") ^^^ RightOuter }
  def fullOuter = positioned { "full" ~ opt("outer") ^^^ FullOuter }
  def union = positioned { "union" ^^^ Union }
  def intersect = positioned { "intersect" ^^^ Intersect }
  def except = positioned { "except" ^^^ Except }
  def byUpdate = positioned { "by" ~ "update" ^^^ ByUpdate }
  def orderBy = positioned { "order" ~ "by" ^^^ OrderBy }
  def limit = positioned { "limit" ^^^ Limit }
  def asc = positioned { "asc" ^^^ Asc }
  def desc = positioned { "desc" ^^^ Desc }
  def match_ = positioned { "match" ^^^ Match }

  override val whiteSpace = """[ \t\n\r\f]+""".r

  def apply(d: String) = {
    parse(phrase(token *), d) match {
      case NoSuccess(msg, next)  => Left(LexerError(next.pos, msg))
      case Success(result, next) => Right(result)
    }
  }
}
