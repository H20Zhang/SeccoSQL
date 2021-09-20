package unit.parser

import org.apache.spark.secco.parsing.SQLParser
import util.{SeccoFunSuite, UnitTestTag}

class SQLParserSuite extends SeccoFunSuite {

  val subgraphQueryString =
    """
      |select count(*)
      |from
      |	R1 natural join R2
      """.stripMargin

  val complexSubgraphQueryString =
    """
      |select a, sum(weight)
      |from
      |	W natural join (
      |   select distinct a, c
      |   from R1 natural join R2
      | ) as T
      |group by a
      """.stripMargin

  val iterativeGraphQuery =
    """
      |with recursive(5) PR(ID , vw) as (
      |   (select * from V)
      | union by update ID
      |   (select src as ID, 0.85*sum(PR.vw)+0.15 as vw  
      |       from PR natural join (
      |         select src, dst as ID
      |         from E
      |         ) as T
      |     group by ID 
      |   )
      | ) select * from PR;
      |
      """.stripMargin

  val complexIterativeGraphQuery =
    """
      |with recursive(5) PR(ID , vw) as (
      |   (select * from V)
      | union by update ID
      |   (select src as ID, 0.85*sum(PR.vw)+0.15 as vw  
      |       from PR natural join (
      |         select src, dst as ID
      |         from E1 natural join E2
      |         ) as G
      |     group by ID 
      |   )
      | ) select * from PR;
      |
      """.stripMargin

  test("parser", UnitTestTag) {

    val Ast1 = SQLParser.parseAST(subgraphQueryString)
    pprint.pprintln(Ast1)

    val Ast2 = SQLParser.parseAST(complexSubgraphQueryString)
    pprint.pprintln(Ast2)

    val Ast3 = SQLParser.parseAST(iterativeGraphQuery)
    pprint.pprintln(Ast3)

    val Ast4 = SQLParser.parseAST(complexIterativeGraphQuery)
    pprint.pprintln(Ast4)
  }

  test("builder", UnitTestTag) {

    val plan1 = SQLParser.parsePlan(subgraphQueryString)
    println(plan1.treeString)

    val plan2 = SQLParser.parsePlan(complexSubgraphQueryString)
    println(plan2.treeString)

    val plan3 = SQLParser.parsePlan(iterativeGraphQuery)
    println(plan3.treeString)

    val plan4 = SQLParser.parsePlan(complexIterativeGraphQuery)
    println(plan4.treeString)

  }

}
