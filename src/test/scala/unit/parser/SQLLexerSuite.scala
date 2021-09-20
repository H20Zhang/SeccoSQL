package unit.parser

import org.apache.spark.secco.parsing.SQLLexer
import util.{SeccoFunSuite, UnitTestTag}

class SQLLexerSuite extends SeccoFunSuite {

  test("basic", UnitTestTag) {
    val inputString =
      s"""
         |"123"
         |true
         |false
         |1.0f
         |1.0
         |1l
         |-1l
         |1
         |+
         |-
         |*
         |/
         |%
         |=
         |!=
         |<=
         |<
         |>=
         |>
         |(
         |)
         |;
         |.
         |,
         |with
         |recursive
         |as
         |select
         |distinct
         |all
         |from
         |where
         |group by
         |having
         |and
         |or
         |any
         |exists
         |inner
         |in
         |not
         |is null
         |is not null
         |case
         |when
         |then
         |else
         |join
         |on
         |using
         |left outer
         |right outer
         |full outer
         |union
         |by update
         |order by
         |limit
         |asc
         |desc
         |Id1
         |Id2
         |natural
         |""".stripMargin
    val tokens = SQLLexer(inputString)

    assert(
      tokens.right.get
        .toString() == "List(StringLit(\"123\"), BooleanLit(true), BooleanLit(false), " +
        "FloatLit(1.0), DoubleLit(1.0), LongLit(1), LongLit(-1), IntLit(1), Add, Sub, " +
        "Mul, Div, Mod, Eq, NotEq, Leq, Le, Geq, Ge, Lp, Rp, Sim, Dot, Com, With, " +
        "Recursive, As, Select, Distinct, All, From, Where, GroupBy, Having, And, " +
        "Or, Any, Exists, Inner, In, Not, IsNull, IsNotNull, Case, When, Then, " +
        "Else, Join, On, Using, LeftOuter, RightOuter, FullOuter, Union, " +
        "ByUpdate, OrderBy, Limit, Asc, Desc, Identifier(Id1), Identifier(Id2), Natural)"
    )
  }
}
