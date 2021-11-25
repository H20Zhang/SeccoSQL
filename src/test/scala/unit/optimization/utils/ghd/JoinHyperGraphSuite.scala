package unit.optimization.utils.ghd

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.plan.BinaryJoin
import org.apache.spark.secco.optimization.support.ExtractRequiredProjectJoins
import org.apache.spark.secco.optimization.util.ghd.JoinHyperGraph
import util.SeccoFunSuite

//TODO: test the JoinHyperGraph
class JoinHyperGraphSuite extends SeccoFunSuite {

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
      CatalogTable("R4", CatalogColumn("d") :: CatalogColumn("a") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R5", CatalogColumn("a") :: CatalogColumn("c") :: Nil)
    )
  }

  test("joinHyperGraph_operation") {
    val sqlText =
      """
        |select *
        |from R1 natural join R2 natural join R3 natural join R4 natural join R5
        |""".stripMargin

    val plan = seccoSession.sql(sqlText).queryExecution.analyzedPlan
    val binaryJoinPlan = plan.collectFirst { case s: BinaryJoin => s }.get

    val (inputs, conditions, projectionList, mode) =
      ExtractRequiredProjectJoins.unapply(binaryJoinPlan).get

    println(inputs)

    val joinHyperGraph = JoinHyperGraph(inputs, conditions)

    println(joinHyperGraph)
    println(joinHyperGraph.isCyclic())
    println(joinHyperGraph.toPlan())
  }

  test("joinHyperGraph_wrt_complex_query") {
    // join with relations some of whose relation does not participate in join

    // join with relations with same output attribute

    // acyclic join

    // cyclic join

    // join with multiple condition between two relation

    // join with theta-join condition (should fail during testing)

    // join with self-join condition (should fail during testing)

  }

}
