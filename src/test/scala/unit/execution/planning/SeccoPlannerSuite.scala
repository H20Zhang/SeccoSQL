package unit.execution.planning

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.execution.planning.SeccoPlanner
import org.apache.spark.secco.optimization.SeccoOptimizer
import util.{SeccoFunSuite, UnitTestTag}

class SeccoPlannerSuite extends SeccoFunSuite {

  import org.apache.spark.secco.analysis.RelationAlgebraWithAnalysis._

  test("basic", UnitTestTag) {

    val optimizer = dlSession.sessionState.optimizer
    val planner = dlSession.sessionState.planner
    val catalog = dlSession.sessionState.catalog

    val colA = CatalogColumn("A")
    val colB = CatalogColumn("B")

    catalog.createTable(
      CatalogTable("R1", Seq(colA, colB)).attachData("twitter")
    )

    val scanExpr = dlSession.table("R1").logical
    val optimizedExpr = optimizer.execute(scanExpr)
    val plan = planner.plan(optimizedExpr)

    println(optimizedExpr)
    println(plan.next())
  }

}
