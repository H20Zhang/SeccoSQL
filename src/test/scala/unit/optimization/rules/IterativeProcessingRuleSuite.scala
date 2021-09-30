package unit.optimization.rules

import org.apache.spark.secco.Dataset
import org.apache.spark.secco.catalog.{Catalog, CatalogTable}
import org.apache.spark.secco.optimization.SeccoOptimizer
import util.{SeccoFunSuite, UnitTestTag}

class IterativeProcessingRuleSuite extends SeccoFunSuite {

  override def setupDB() = {

    super.setupDB()

    Dataset.empty("E1", Seq("A", "B"))
    Dataset.empty("E2", Seq("B", "C"))
    Dataset.empty("PR", Seq("V", "W"))
    Dataset.empty("DeltaPR", Seq("V", "W"))
  }

  def E1 = dlSession.table("E1")
  def E2 = dlSession.table("E2")
  def PR = dlSession.table("PR")
  def DeltaPR = dlSession.table("DeltaPR")

  test("basic", UnitTestTag) {
    import org.apache.spark.secco.analysis.RelationAlgebraWithAnalysis._

    val agg =
      aggregate(
        "sum(W) by A",
        join(join(E1.logical, E2.logical), rename("C/V", DeltaPR.logical))
      )

    val expr1 =
      rename(
        s"V/A,W/${agg.producedOutputOld.head}",
        agg
      )

    val expr2 =
      iterative(update("PR", "DeltaPR", Seq("V"), expr1), "PR")

    val optimizer = new SeccoOptimizer
    val optimizedExpr = optimizer.execute(expr2)

    println(optimizedExpr)
  }

}
