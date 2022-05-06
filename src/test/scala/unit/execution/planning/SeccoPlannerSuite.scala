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
    createTestRelation(Seq(Seq(1, 2)), "R4", "d", "e")("d")

  }

  test("planning") {

    val planner = seccoSession.sessionState.planner

    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")
    val R4 = seccoSession.table("R4")

    // Test planning local multiway join.
    val R123 =
      R1.join(R2, "R1.b = R2.b").join(R3, "R2.c = R3.c").filter("R3.d < 10")
    println(R123.queryExecution.executionPlan)

    // Test planning local binary join.
    val R1234 = R123.subqueryAlias("R123").join(R4, "R123.d = R4.d")
    println(R1234.queryExecution.executionPlan)

    // Test planning selection.
    val ds1 = R1234.select("R123.a > 10")
    println(ds1.queryExecution.executionPlan)

    // Test planning project.
    val ds2 = ds1.project("R123.a, R4.e")
    println(ds2.queryExecution.executionPlan)

    // Test planning distinct.
    val ds3 = ds2.distinct()
    println(ds3.queryExecution.executionPlan)

    // Test planning aggregation
    val ds4 = ds2.aggregate(Seq("sum(R4.e)"), Seq("R123.a"))
    println(ds4.queryExecution.executionPlan)

  }

}
