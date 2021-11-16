package unit.analysis

import org.apache.spark.secco.analysis.{
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedRelation,
  UnresolvedStar
}
import org.apache.spark.secco.analysis.rules.{
  CleanupAliases,
  LookupFunctions,
  ResolveAliases,
  ResolveFunctions,
  ResolveGlobalAggregatesInSelect,
  ResolveNaturalAndUsingJoin,
  ResolveReferences,
  ResolveRelations
}
import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.expression.Alias
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.plan.{Project, Relation}
import spire.ClassTag
import util.{SeccoFunSuite, UnitTestTag}

import scala.util.Try

class AnalysisRuleSuite extends SeccoFunSuite {
  test("resolution", UnitTestTag) {

    val catalog = seccoSession.sessionState.catalog
    catalog.dropTable("R1")
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

    var plan = seccoSession
      .sql("""
        |select a, sum(c) as res
        |from R1, R2 natural join R3 left join R4 using (d)
        |where R1.b = R2.b
        |group by a
        |having res > 10
        |""".stripMargin)
      .logical

    println(s"original plan:${plan}")

    //test LookupFunctions
    plan = LookupFunctions(plan)

    //test ResolveRelations
    plan = ResolveRelations(plan)

    //test ResolveNaturalAndUsingJoin
    plan = ResolveNaturalAndUsingJoin(plan)

    //test ResolveReference
    plan = ResolveReferences(ResolveReferences(plan))

    //test ResolveFunction
    plan = ResolveFunctions(plan)

    //test ResolveAlias
    plan = ResolveReferences(ResolveAliases(plan))

    assert(plan.resolved)
  }
}
