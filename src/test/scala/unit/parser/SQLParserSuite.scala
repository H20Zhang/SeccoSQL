package unit.parser

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.parsing.{SQLLexer, SQLParser}
import util.{SeccoFunSuite, UnitTestTag}

import scala.util.Try

class SQLParserSuite extends SeccoFunSuite {

  test("parsePatternExpression", UnitTestTag) {
    val seccoSession = SeccoSession.currentSession

    val pattern0 = "()"
    val pattern1 = "(a)"
    val pattern2 = "(a:Person:Student)"
    val pattern3 = "(a:Person:Student {name:123, test:12})"
    val pattern4 = "(a)-[]-()"
    val pattern5 = "()-[b]-()"
    val pattern6 = "(a)-[b]->(c)"
    val pattern7 =
      "(a:Person:Student {name:123, test:12})-[b:Friend {year:10}]->(c:Person:Student)"

    val pattern8 = "(a)-[b]->(c)-[d]->(e)"
    val pattern9 = "(a)-[b]->(c);(b)-[d]-(e)"

    val patterns = Seq(
      pattern0,
      pattern1,
      pattern2,
      pattern3,
      pattern4,
      pattern5,
      pattern6,
      pattern7,
      pattern8,
      pattern9
    )

    patterns.foreach { pattern =>
      seccoSession.sessionState.sqlParser
        .parsePatternExpression(pattern)
    }

  }

  test("parseSQL", UnitTestTag) {

    val seccoSession = SeccoSession.currentSession

    // make sure following query can be parsed

    // select queries
    val selectQuery1 =
      s"""
         |select *
         |from R1
         |where R1.a < b
         |limit 10
         |""".stripMargin

    val selectQuery2 =
      s"""
         |select distinct *
         |from R1
         |where a < b and b < c
         |""".stripMargin

    val selectQuery3 =
      s"""
         |select *
         |from R1
         |where a < b and (b < c or c > d)
         |""".stripMargin

    val selectQuery4 =
      s"""
         |select *
         |from R1
         |where a < (b+1) and (b < c-1 or c > d+1)
         |""".stripMargin

    // project & aggregate queries
    val projectQuery1 =
      s"""
         |select a
         |from R1
         |""".stripMargin

    val projectQuery2 =
      s"""
         |select *
         |from R1
         |""".stripMargin

    val projectQuery3 =
      s"""
         |select a, a, b
         |from R1
         |""".stripMargin

    val projectQuery4 =
      s"""
         |select a, *
         |from R1
         |""".stripMargin

    val projectQuery5 =
      s"""
         |select a, sum(b) as d
         |from R1
         |""".stripMargin

    val projectQuery6 =
      s"""
         |select a, b+c+1
         |from R1
         |""".stripMargin

    // groupby, aggregate, and having queries
    val aggregateQuery1 =
      s"""
         |select a, count(*)
         |from R1
         |group by a
         |""".stripMargin

    val aggregateQuery2 =
      s"""
         |select a, sum(b+1+2+c) as m
         |from R1
         |group by a
         |""".stripMargin

    val aggregateQuery3 =
      s"""
         |select a, sum(b+1+2+c) as m, b+c as w
         |from R1
         |group by a
         |having m > 1
         |""".stripMargin

    // join queries
    val joinQuery1 =
      s"""
         |select *
         |from R1, R2
         |""".stripMargin

    val joinQuery2 =
      s"""
         |select *
         |from R1 natural join R2
         |""".stripMargin

    val joinQuery3 =
      s"""
         |select *
         |from R1 e1 natural join R1 e2
         |""".stripMargin

    val joinQuery4 =
      s"""
         |select *
         |from R1 join R2 on R1.a = R2.b
         |""".stripMargin

    val joinQuery5 =
      s"""
         |select *
         |from R1 join R2 using (a)
         |""".stripMargin

    val joinQuery6 =
      s"""
         |select *
         |from R1 left join R2 using (a)
         |""".stripMargin

    val joinQuery7 =
      s"""
         |select *
         |from R1 full outer join R2 using (a)
         |""".stripMargin

    // set operation queries
    val setQuery1 =
      s"""
         |(select *
         |from R1, R2)
         |union
         |(select *
         |from R1, R3)
         |""".stripMargin

    val setQuery2 =
      s"""
         |(select *
         |from R1, R2)
         |union all
         |(select *
         |from R1, R3)
         |""".stripMargin

    val setQuery3 =
      s"""
         |(select *
         |from R1, R2)
         |except
         |(select *
         |from R1, R3)
         |""".stripMargin

    val setQuery4 =
      s"""
         |(select *
         |from R1, R2)
         |intersect
         |(select *
         |from R1, R3)
         |""".stripMargin

    val patternQuery1 =
      s"""
         |select *
         |from match(G1, (a)-[]->(b)-[]->(c))
         |limit 10
         |""".stripMargin

    val patternQuery2 =
      s"""
         |select a
         |from match(G2,(a:Person:Student {name:123, test:12})-[b:Friend {year:10}]->(c:Person:Student))
         |""".stripMargin

    val queryList = Seq(
      selectQuery1,
      selectQuery2,
      selectQuery3,
      selectQuery4,
      projectQuery1,
      projectQuery2,
      projectQuery3,
      projectQuery4,
      projectQuery5,
      projectQuery6,
      aggregateQuery1,
      aggregateQuery2,
      aggregateQuery3,
      joinQuery1,
      joinQuery2,
      joinQuery3,
      joinQuery4,
      joinQuery5,
      joinQuery6,
      joinQuery7,
      setQuery1,
      setQuery2,
      setQuery3,
      setQuery4,
      patternQuery1,
      patternQuery2
    )

    queryList.foreach { query =>
      val res = Try(seccoSession.sql(query).queryExecution.logical)
      assert(res.isSuccess, res.failed.toString)
    }

//    var plan = seccoSession.sql(patternQuery2).queryExecution.logical
//    println(plan)

  }

}
