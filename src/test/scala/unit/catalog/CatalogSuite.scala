package unit.catalog

import org.apache.spark.secco.catalog._
import util.{SeccoFunSuite, UnitTestTag}

class CatalogSuite extends SeccoFunSuite {

  test("basic", UnitTestTag) {

    //default catalog creation
    val catalog = dlSession.sessionState.catalog
    assert(catalog.listDatabase().size == 1)

    //----------------------------
    //      CatalogColumn
    //----------------------------

    //create column
    val colA = CatalogColumn("A")
    val colB = CatalogColumn("B")
    val colA_ = CatalogColumn("A")

    assert(colA != colB)
    assert(colA == colA_)

    //----------------------------
    //      CatalogDatabase
    //----------------------------

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

    //----------------------------
    //          CatalogTable
    //----------------------------

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

    //----------------------------
    //     CatalogFunction
    //----------------------------

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

    //TODO: finish below
    //----------------------------
    //     CatalogStatistic
    //----------------------------

    //----------------------------
    //     ColumnStat
    //----------------------------

  }

}
