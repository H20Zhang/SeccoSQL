package unit.analysis

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import util.{SeccoFunSuite, UnitTestTag}

import scala.util.Try

class AnalyzerSuite extends SeccoFunSuite {

  def checkCorrectness(queryList: Seq[String]) = {
    queryList.foreach { query =>
      val analyzedPlanOpt =
        Try(seccoSession.sql(query).queryExecution.analyzedPlan)

      if (!analyzedPlanOpt.isSuccess) {
        val analyzedPlan = analyzedPlanOpt.get
        if (!analyzedPlan.resolved) {
          assert(false, s"query:${query} cannot be analyzed")
        }
      }
    }
  }

  override def setupDB(): Unit = {
    val catalog = seccoSession.sessionState.catalog
    catalog.createTable(
      CatalogTable("R1", CatalogColumn("a") :: CatalogColumn("b") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R2", CatalogColumn("b") :: CatalogColumn("c") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R3", CatalogColumn("c") :: CatalogColumn("d") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R4", CatalogColumn("d") :: CatalogColumn("e") :: Nil)
    )
  }

  test("select_query", UnitTestTag) {

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

    val queryList = Seq(
      selectQuery1,
      selectQuery2,
      selectQuery3,
      selectQuery4
    )

    checkCorrectness(queryList)
  }

  test("project_query", UnitTestTag) {

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
         |select a, sum(b+1+2+c) as m
         |from R1
         |group by a
         |having m > 1
         |""".stripMargin

    val queryList = Seq(
      projectQuery1,
      projectQuery2,
      projectQuery3,
      projectQuery4,
      projectQuery5,
      projectQuery6,
      aggregateQuery1,
      aggregateQuery2,
      aggregateQuery3
    )

    checkCorrectness(queryList)
  }

  test("join_query", UnitTestTag) {

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
         |from R1 join R2 using (b)
         |""".stripMargin

    val joinQuery6 =
      s"""
         |select *
         |from R1 left join R2 using (b)
         |""".stripMargin

    val joinQuery7 =
      s"""
         |select *
         |from R1 full outer join R2 using (b)
         |""".stripMargin

    val queryList = Seq(
      joinQuery1,
      joinQuery2,
      joinQuery3,
      joinQuery4,
      joinQuery5,
      joinQuery6,
      joinQuery7
    )

    checkCorrectness(queryList)
  }

  test("set_query", UnitTestTag) {

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

    val queryList = Seq(
      setQuery1,
      setQuery2,
      setQuery3,
      setQuery4
    )

    checkCorrectness(queryList)
  }

//  test("complex_query", UnitTestTag) {
//
//    val catalog = seccoSession.sessionState.catalog
//    catalog.dropTable("R1")
//    catalog.dropTable("R2")
//    catalog.dropTable("W")
//    catalog.dropTable("PR")
//    catalog.dropTable("E")
//    catalog.dropTable("V")
//
//    catalog.createTable(
//      CatalogTable("R1", CatalogColumn("a") :: CatalogColumn("b") :: Nil)
//    )
//    catalog.createTable(
//      CatalogTable("R2", CatalogColumn("b") :: CatalogColumn("c") :: Nil)
//    )
//    catalog.createTable(
//      CatalogTable("W", CatalogColumn("c") :: CatalogColumn("weight") :: Nil)
//    )
//    catalog.createTable(
//      CatalogTable("PR", CatalogColumn("ID") :: CatalogColumn("vw") :: Nil)
//    )
//    catalog.createTable(
//      CatalogTable("E", CatalogColumn("src") :: CatalogColumn("dst") :: Nil)
//    )
//    catalog.createTable(
//      CatalogTable("V", CatalogColumn("ID") :: CatalogColumn("vw") :: Nil)
//    )
//
//    val subgraphQueryString =
//      """
//        |select *
//        |from R1 natural join R2
//      """.stripMargin
//
//    val complexSubgraphQueryString =
//      """
//        |select a, sum(weight)
//        |from
//        |	W natural join (
//        |   select distinct a, c
//        |   from R1 natural join R2
//        | ) as T
//        |group by a
//      """.stripMargin
//
//    val iterativeGraphQuery =
//      """
//        |with recursive(5) PR(ID , vw) as (
//        |   (select * from V)
//        | union by update ID
//        |   (select src as ID, 0.85*sum(PR.vw)+0.15 as vw
//        |       from PR natural join (
//        |         select src, dst as ID
//        |         from E
//        |         ) as T
//        |     group by PR.ID
//        |   )
//        | ) select * from PR;
//        |
//      """.stripMargin
//
//    val complexIterativeGraphQuery =
//      """
//        |with recursive(5) PR(ID , vw) as (
//        |   (select * from V)
//        | union by update ID
//        |   (select src as ID, 0.85*sum(PR.vw)+0.15 as vw
//        |       from PR natural join (
//        |         select src, dst as ID
//        |         from E1 natural join E2
//        |         ) as G
//        |     group by ID
//        |   )
//        | ) select * from PR;
//        |
//      """.stripMargin
//
//    val ds1 = seccoSession.sql(subgraphQueryString)
//    val logicalPlan1 = ds1.queryExecution.logical
//    val analyzedPlan1 = ds1.queryExecution.analyzedPlan
//    println(logicalPlan1)
//    println(analyzedPlan1)
//
//    val ds2 = seccoSession.sql(complexSubgraphQueryString)
//    val logicalPlan2 = ds2.queryExecution.logical
//    val analyzedPlan2 = ds2.queryExecution.analyzedPlan
//    println(logicalPlan2)
//    println(analyzedPlan2)
//
////    val ds3 = dl.sql(iterativeGraphQuery)
////    val logicalPlan3 = ds3.queryExecution.logical
////    val analyzedPlan3 = ds3.queryExecution.analyzedPlan
////    println(logicalPlan3)
////    println(analyzedPlan3)
//  }

}
