package unit.analysis

import org.apache.spark.secco.analysis.{
  SimpleAnalyzer,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedRelation,
  UnresolvedStar
}
import org.apache.spark.secco.catalog.{Catalog, CatalogColumn, CatalogTable}
import org.apache.spark.secco.expression.{Alias, Expression}
import org.apache.spark.secco.expression.aggregate._
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.plan.{Project, Relation}
import spire.ClassTag
import util.{SeccoFunSuite, UnitTestTag}

import scala.util.Try

class AnalyzerSuite extends SeccoFunSuite {

  test("individual rule", UnitTestTag) {

    val catalog = dlSession.sessionState.catalog
    catalog.dropTable("R1")
    catalog.createTable(
      CatalogTable("R1", CatalogColumn("a") :: CatalogColumn("b") :: Nil)
    )

    val analyzer = dlSession.sessionState.analyzer

    val plan1 = Project(
      UnresolvedRelation("R1"),
      Nil,
      ExecMode.Coupled,
      Seq(
        Alias(
          UnresolvedFunction(
            "sum",
            UnresolvedAttribute(Seq("R1", "a")) :: Nil,
            true
          ),
          "sum(a)"
        )(),
        UnresolvedAlias(UnresolvedAttribute("a" :: Nil)),
        UnresolvedAlias(UnresolvedStar(Some("R1" :: Nil))),
        UnresolvedAlias(
          UnresolvedFunction(
            "sum",
            UnresolvedAttribute(Seq("R1", "a")) :: Nil,
            true
          )
        )
      )
    )

    val plan2 = Project(
      Relation("R1"),
      Nil,
      ExecMode.Coupled,
      Alias(
        UnresolvedFunction(
          "sum1",
          UnresolvedAttribute(Seq("R1", "a")) :: Nil,
          true
        ),
        "sum(a)"
      )() :: Nil
    )

    /** test individual rules */
    //test LookupFunctions
    assert(Try(analyzer.LookupFunctions(plan1)).isSuccess)
    assert(Try(analyzer.LookupFunctions(plan2)).isFailure)

    //test ResolveRelations
    val relationResolvedPlan1 = analyzer.ResolveRelations(plan1)
    assert(relationResolvedPlan1.collect {
      case u: UnresolvedRelation => u
    }.isEmpty)

    def existExpression[T: ClassTag](p: LogicalPlan) =
      p.expressions.exists(e => e.collect { case u: T => u }.nonEmpty)

    //test ResolveReference
    val attributeResolvedPlan1 =
      analyzer.ResolveReferences(
        analyzer.ResolveReferences(relationResolvedPlan1)
      )
    assert(attributeResolvedPlan1.collect {
      case p: LogicalPlan if existExpression[UnresolvedAttribute](p) =>
        p
    }.isEmpty)

    //test ResolveFunction
    val functionResolvedPlan1 =
      analyzer.ResolveFunctions(attributeResolvedPlan1)
    assert(functionResolvedPlan1.collect {
      case p: LogicalPlan if existExpression[UnresolvedFunction](p) =>
        p
    }.isEmpty)

    //test ResolveAlias
    val aliasResolvedPlan1 = analyzer.ResolveAliases(functionResolvedPlan1)
    assert(aliasResolvedPlan1.collect {
      case p: LogicalPlan if existExpression[UnresolvedAlias](p) => p
    }.isEmpty)

    //test ResolveGlobalAggregateInSelect
    val globalAggResolvedPlan1 =
      analyzer.ResolveGlobalAggregatesInSelect(aliasResolvedPlan1)
    assert(globalAggResolvedPlan1.nodeName == "Aggregate")

    //test CleanupAlias
    val cleanUpAliasedPlan1 = analyzer.CleanupAliases(aliasResolvedPlan1)
//    assert(
//      cleanUpAliasedPlan1.toString ==
//        "Project[Coupled, [Sum(a#4) AS sum(a)#0, a#6, a#2, b#3, Count(a#8) AS Count(R1.`a`)#10]]-> ()" +
//          "\n+- Relation[R1, Atomic]-> (a,b)\n"
//    )
  }

  test("analyzer", UnitTestTag) {

    val catalog = dlSession.sessionState.catalog
    catalog.dropTable("R1")
    catalog.createTable(
      CatalogTable("R1", CatalogColumn("a") :: CatalogColumn("b") :: Nil)
    )

    val sqlText =
      s"""
         |select count(a), *, sum(a)
         |from R1
         |""".stripMargin

    val ds = dlSession.sql(sqlText)
    val analyzedPlan = ds.queryExecution.analyzedPlan

//    assert(
//      analyzedPlan.toString == s"""|Aggregate[(List(),error), Coupled, [Count(a#13) AS Count(R1.`a`)#17, a#11, b#12, Sum(a#15) AS Sum(R1.`a`)#18]]-> (L1)
//                                   |+- Relation[R1, Atomic]-> (a,b)
//                                   |""".stripMargin
//    )

  }

  test("simple_analyzer", UnitTestTag) {

    val catalog = dlSession.sessionState.catalog
    catalog.dropTable("R1")
    catalog.dropTable("R2")
    catalog.dropTable("W")
    catalog.dropTable("PR")
    catalog.dropTable("E")
    catalog.dropTable("V")

    catalog.createTable(
      CatalogTable("R1", CatalogColumn("a") :: CatalogColumn("b") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R2", CatalogColumn("b") :: CatalogColumn("c") :: Nil)
    )
    catalog.createTable(
      CatalogTable("W", CatalogColumn("c") :: CatalogColumn("weight") :: Nil)
    )
    catalog.createTable(
      CatalogTable("PR", CatalogColumn("ID") :: CatalogColumn("vw") :: Nil)
    )
    catalog.createTable(
      CatalogTable("E", CatalogColumn("src") :: CatalogColumn("dst") :: Nil)
    )
    catalog.createTable(
      CatalogTable("V", CatalogColumn("ID") :: CatalogColumn("vw") :: Nil)
    )

    val subgraphQueryString =
      """
        |select *
        |from R1 natural join R2
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
        |     group by PR.ID 
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

    val ds1 = dlSession.sql(subgraphQueryString)
    val logicalPlan1 = ds1.queryExecution.logical
    val analyzedPlan1 = ds1.queryExecution.analyzedPlan
    println(logicalPlan1)
    println(analyzedPlan1)

    val ds2 = dlSession.sql(complexSubgraphQueryString)
    val logicalPlan2 = ds2.queryExecution.logical
    val analyzedPlan2 = ds2.queryExecution.analyzedPlan
    println(logicalPlan2)
    println(analyzedPlan2)

//    val ds3 = dl.sql(iterativeGraphQuery)
//    val logicalPlan3 = ds3.queryExecution.logical
//    val analyzedPlan3 = ds3.queryExecution.analyzedPlan
//    println(logicalPlan3)
//    println(analyzedPlan3)
  }

}
