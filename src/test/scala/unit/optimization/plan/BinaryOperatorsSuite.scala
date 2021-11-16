package unit.optimization.plan

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import util.SeccoFunSuite

class BinaryOperatorsSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    val catalog = seccoSession.sessionState.catalog
    catalog.createTable(
      CatalogTable(
        "R1",
        CatalogColumn("a") :: CatalogColumn("b") :: Nil,
        CatalogColumn("a") :: Nil
      )
    )
    catalog.createTable(
      CatalogTable(
        "R2",
        CatalogColumn("b") :: CatalogColumn("c") :: Nil,
        CatalogColumn("b") :: Nil
      )
    )
    catalog.createTable(
      CatalogTable(
        "R3",
        CatalogColumn("c") :: CatalogColumn("d") :: Nil,
        CatalogColumn("c") :: Nil
      )
    )
  }

  test("binary_join") {
    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")

    assert(
      R1.queryExecution.analyzedPlan.primaryKey.map(_.name).toSet == Set("a")
    )

    assert(
      R1.join(R2, "R1.a = R2.b")
        .queryExecution
        .analyzedPlan
        .primaryKey
        .map(_.name)
        .toSet == Set("a", "b")
    )

    assert(
      R1.join(R2, "R1.b = R2.b").queryExecution.analyzedPlan.primaryKey.isEmpty
    )
  }
}
