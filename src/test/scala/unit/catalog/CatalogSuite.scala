package unit.catalog

import org.apache.spark.secco.catalog._
import util.{SeccoFunSuite, UnitTestTag}

//TODO: finish the testing for CatalogGraph, CatalogStatistic, and CatalogStat
class CatalogSuite extends SeccoFunSuite {

  val catalog = seccoSession.sessionState.catalog

  test("test_basic", UnitTestTag) {
    //default catalog creation
    val catalog = seccoSession.sessionState.catalog
    assert(catalog.listDatabase().size == 1)
  }

  test("test_CatalogColumn", UnitTestTag) {
    //create column
    val colA = CatalogColumn("A")
    val colB = CatalogColumn("B")
    val colA_ = CatalogColumn("A")

    assert(colA != colB)
    assert(colA == colA_)
  }

  test("test_CatalogDatabase") {
    //create database
    val db = CatalogDatabase("test")
    catalog.createDatabase(db)
    assert(catalog.listDatabase().size == 2)
    assert(catalog.getDatabase("test").nonEmpty)

    //alter database
    catalog.alterDatabase(CatalogDatabase("test", "this is a test database"))
    assert(
      catalog.getDatabase("test").get.description == Some(
        "this is a test database"
      )
    )

    //drop database
    catalog.dropDatabase("test")
    assert(catalog.listDatabase().size == 1)
    assert(catalog.getDatabase("test").isEmpty)
  }

  test("test_CatalogTable", UnitTestTag) {

    //create table
    val table = CatalogTable("R1", Seq(CatalogColumn("A"), CatalogColumn("B")))
    val table_ = CatalogTable("R1", Seq(CatalogColumn("A"), CatalogColumn("B")))
    assert(table == table_)
    assert(catalog.getDatabase("R1").isEmpty)

    catalog.createTable(table)
    assert(catalog.getTable("R1").get == table)

    //alter table
    catalog.alterTable(CatalogTable("R1", Seq(CatalogColumn("B"))))
    assert(
      catalog.getTable("R1").get == CatalogTable("R1", Seq(CatalogColumn("B")))
    )

    //drop table
    catalog.dropTable("R1")
    assert(catalog.getTable("R1").isEmpty)
    assert(catalog.listTable(Catalog.defaultDBName).isEmpty)
  }

  //TODO: To be done.
  test("test_CatalogGraph", UnitTestTag) {

    //create graph
    val graph1 = CatalogGraphTable("G1")
    assert(catalog.getGraph("G1").isEmpty)

    val graph2 = CatalogGraphTable(
      "G2",
      Some(CatalogColumn("vLabel")),
      Some(CatalogColumn("eLabel"))
    )

    val graph3 = CatalogGraphTable(
      "G3",
      Some(CatalogColumn("vLabel")),
      Some(CatalogColumn("eLabel")),
      Seq(CatalogColumn("nodeProperty1")),
      Seq(CatalogColumn("edgeProperty1"))
    )

    catalog.createGraph(graph1)
    catalog.createGraph(graph2)
    catalog.createGraph(graph3)

    //get graph
    assert(catalog.getGraph("G1").nonEmpty)
    assert(catalog.getGraph("G2").nonEmpty)
    assert(catalog.getGraph("G3").nonEmpty)

    //alter graph
    val _graph1 = graph2.copy(identifier = TableIdentifier("G1"))
    catalog.alterGraph(_graph1)
    catalog.getGraph("G1").foreach(graph1 => assert(graph1 == _graph1))

    //drop graph
    catalog.dropGraph("G1")
    assert(catalog.getGraph("G1").isEmpty)
  }

  test("test_CatalogFunction", UnitTestTag) {
    //create function
    val func = CatalogFunction("sum", "org.apache.spark.function.sum")
    catalog.createFunction(func)
    assert(catalog.getFunction("sum").get == func)

    //alter function
    catalog.renameFunction("sum", "sum1")
    val func1 = CatalogFunction("sum1", "org.apache.spark.function.sum")
    assert(catalog.getFunction("sum1").get == func1)

    //drop function
    catalog.dropFunction("sum1")
    assert(catalog.listFunctions().isEmpty)
  }
}
