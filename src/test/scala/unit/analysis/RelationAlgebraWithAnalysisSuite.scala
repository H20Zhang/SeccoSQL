package unit.analysis

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.plan.{Relation, Transform}
import util.{SeccoFunSuite, UnitTestTag}

//TODO: the test should replace printf with assert
class RelationAlgebraWithAnalysisSuite extends SeccoFunSuite {

  import org.apache.spark.secco.analysis.RelationAlgebraWithAnalysis._

  //register columns and schemas
  override def setupDB(): Unit = {
    super.setupDB()

    val catalog = dlSession.sessionState.catalog

    val colA = CatalogColumn("A")
    val colB = CatalogColumn("B")
    val colC = CatalogColumn("C")
    catalog.createTable(CatalogTable("R1", Seq(colA, colB, colC)))
    catalog.createTable(CatalogTable("R2", Seq(colA, colB)))
    catalog.createTable(CatalogTable("R3", Seq(colB, colC)))
    catalog.createTable(CatalogTable("R4", Seq(colA, colC)))

    catalog.createTable(CatalogTable("E1", Seq(colA, colB)))
    catalog.createTable(CatalogTable("E2", Seq(colB, colC)))
    catalog.createTable(CatalogTable("PR", "V", "W"))
    catalog.createTable(CatalogTable("DeltaPR", "V", "W"))
  }

  def R1 = dlSession.table("R1").logical
  def R2 = dlSession.table("R2").logical
  def R3 = dlSession.table("R3").logical
  def R4 = dlSession.table("R4").logical
  def E1 = dlSession.table("E1").logical
  def E2 = dlSession.table("E2").logical
  def PR = dlSession.table("PR").logical
  def DeltaPR = dlSession.table("DeltaPR").logical

  test("unary", UnitTestTag) {

    val expr =
      aggregate(
        "sum(C) by A,B",
        project("A,B", project("A,B", select("A<B", R1)))
      )
    println(expr)

    //aggregate with grouping
    val expr1 =
      aggregate(
        "sum(C) by A,B",
        project("A,B", project("A,B", select("A<B", R1)))
      )
    println(expr1)

    //aggregate without grouping
    val expr2 =
      aggregate(
        "count(C)",
        project("A,B", project("A,B", select("A<B", R1)))
      )
    println(expr2)
  }

  test("binary", UnitTestTag) {
    val expr =
      project(
        "A,B",
        join(join(select("A<B", R1), R2), R3, R4)
      )

    print(expr)
  }

  test("atomic", UnitTestTag) {
    val expr = transform("A, A+B", R1)
    val groundTruth =
      Transform(
        Relation("R1"),
        Seq("A", "A+B"),
        Seq("A", "A+B")
      )
    assert(expr == groundTruth)
  }

  test("iterative", UnitTestTag) {

    val agg =
      aggregate("sum(W) by A", join(join(E1, E2), rename("C/V", DeltaPR)))

    val expr1 =
      rename(
        s"V/A,W1/${agg.producedOutputOld.head}",
        agg
      )

    val expr2 =
      iterative(
        update("PR", "DeltaPR", Seq("V"), project("V,W", join(expr1, PR))),
        "PR"
      )

    println(expr2)
  }
}
