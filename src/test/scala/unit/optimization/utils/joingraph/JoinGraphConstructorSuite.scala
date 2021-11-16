package unit.optimization.utils.joingraph

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.support.ExtractRequiredProjectJoins
import org.apache.spark.secco.optimization.util.joingraph.JoinGraphConstructor
import util.SeccoFunSuite

class JoinGraphConstructorSuite extends SeccoFunSuite {

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
      CatalogTable("R4", CatalogColumn("c") :: CatalogColumn("d") :: Nil)
    )
  }

  test("construct_join_graph") {

    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")
    val R4 = seccoSession.table("R4")

    val plan1 = R1
      .join(R2, "R1.a = R2.b")
      .join(R3, "R2.c = R3.c")
      .join(R4, "R3.c = R4.c")
      .queryExecution
      .analyzedPlan

    val (inputs, conditions, projectionList, mode) =
      ExtractRequiredProjectJoins.unapply(plan1).get

    val (nodes, edges) =
      JoinGraphConstructor.constructJoinGraph(inputs, conditions)

    println(nodes.map(_.simpleString))
    println(edges.map(f => (f._1.simpleString, f._2.simpleString)))

  }
}
