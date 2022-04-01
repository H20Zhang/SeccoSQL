package unit.execution.planning

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.execution.planning.SeccoPlanner
import org.apache.spark.secco.optimization.SeccoOptimizer
import util.{SeccoFunSuite, UnitTestTag}

class SeccoPlannerSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    createTestRelation(Seq(Seq(1, 2)), "R1", "a", "b")()
    createTestRelation(Seq(Seq(1, 2)), "R2", "b", "c")()
    createTestRelation(Seq(Seq(1, 2)), "R3", "c", "d")()
    createTestRelation(Seq(Seq(1, 2)), "R4", "d", "e")()
  }

  test("planning scan") {

    val planner = seccoSession.sessionState.planner

    val R1 = seccoSession.table("R1")

    println(R1.queryExecution.executionPlan)

  }

}
