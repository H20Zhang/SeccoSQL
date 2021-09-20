package unit.optimization.rules

import org.apache.spark.secco.Dataset
import org.apache.spark.secco.analysis.RelationAlgebraWithAnalysis._
import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.ExecMode
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.optimization.rules._
import util.{SeccoFunSuite, UnitTestTag}

class BasicRuleSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {

    //create empty datasets to register relations
    Dataset.empty("R1", Seq("A", "B"))
    Dataset.empty("R2", Seq("B", "C"))
    Dataset.empty("R3", Seq("C", "D"))
    Dataset.empty("R4", Seq("D", "E"))
  }

  def R1 = dlSession.table("R1")
  def R2 = dlSession.table("R2")
  def R3 = dlSession.table("R3")
  def R4 = dlSession.table("R4")

  test("MergeJoin", UnitTestTag) {
    val expr = R1.naturalJoin(R2).naturalJoin(R3).naturalJoin(R4).logical
    val optimizedExpr =
      MergeAllJoin(MergeAllJoin(MergeAllJoin(expr)))

    val groundTruth =
      Join(
        Seq(Relation("R1"), Relation("R2"), Relation("R3"), Relation("R4")),
        mode = ExecMode.Coupled
      )
    assert(optimizedExpr == groundTruth)
  }

  test("MergeUnion", UnitTestTag) {
    val expr = R1.unionAll(R2).unionAll(R3).unionAll(R4).logical
    val optimizedExpr = MergeUnion(MergeUnion(MergeUnion(expr)))

    val groundTruth =
      Union(
        Seq(Relation("R1"), Relation("R2"), Relation("R3"), Relation("R4")),
        mode = ExecMode.Coupled
      )
    assert(optimizedExpr == groundTruth)
  }

  test("MergeSelection", UnitTestTag) {
    //MergeSelection Test
    val expr = R1.select("B < C").select("A < B").select("A < B").logical
    val optimizedExpr = MergeSelection(MergeSelection(expr))

    val groundTruth =
      Filter(
        Relation("R1"),
        Seq(("B", "<", "C"), ("A", "<", "B")),
        ExecMode.Coupled
      )

    assert(optimizedExpr == groundTruth)
  }

  test("MergeProjection", UnitTestTag) {
    //MergeProjection Test
    val expr = R1.project("A, B").project("A, B").project("A").logical
    val optimizedExpr = MergeProjection(MergeProjection(expr))

    val groundTruth =
      Project(Relation("R1"), Seq("A"), ExecMode.Coupled)

    assert(optimizedExpr == groundTruth)
  }

  test("RemoveRedundantSelection", UnitTestTag) {
    //MergeSelection Test
    val expr = R1.select("B < C").select("A < B").select("A < B").logical

    val optimizedExpr =
      RemoveRedundantSelection(MergeSelection(MergeSelection(expr)))

    val groundTruth =
      Filter(Relation("R1"), Seq(("A", "<", "B")), ExecMode.Coupled)

    assert(optimizedExpr == groundTruth)
  }

  test("RemoveRedundantProjection", UnitTestTag) {
    //MergeProjection Test
    val expr = R1.project("B, C, D").project("B, C").project("B, C").logical
    val optimizedExpr =
      RemoveRedundantProjection(MergeProjection(MergeProjection(expr)))

    val groundTruth =
      Project(Relation("R1"), Seq("B"), ExecMode.Coupled)

    assert(optimizedExpr == groundTruth)
  }

  test("PushSelectionThroughJoin", UnitTestTag) {
    val expr =
      R1.naturalJoin(R2, R3, R4).select("A < B").select("B < C").logical

    val optimizedExpr =
      PushSelectionThroughJoin(MergeSelection(MergeDelayedJoin(expr)))

    val groundTruth =
      Join(
        Seq(
          Filter(Relation("R1"), Seq(("A", "<", "B")), ExecMode.Coupled),
          Filter(Relation("R2"), Seq(("B", "<", "C")), ExecMode.Coupled),
          Relation("R3"),
          Relation("R4")
        ),
        mode = ExecMode.Coupled
      )
    assert(optimizedExpr == groundTruth)
  }

  test("PushProjectionThroughJoin", UnitTestTag) {
    val expr = R1.naturalJoin(R2, R3, R4).project("A, B, C").logical

    val optimizedExpr =
      RemoveRedundantProjection(
        PushProjectionThroughJoin(MergeDelayedJoin(expr))
      )

    val groundTruth =
      Project(
        Join(
          Seq(
            Relation("R1"),
            Relation("R2"),
            Relation("R3"),
            Project(Relation("R4"), Seq("D"), mode = ExecMode.Coupled)
          ),
          mode = ExecMode.Coupled
        ),
        Seq("A", "B", "C"),
        mode = ExecMode.Coupled
      )
    assert(optimizedExpr == groundTruth)
  }

  test("PushRenameDown", UnitTestTag) {
    val expr =
      rename(
        "B/A",
        join(R1.logical, join(R2.logical), join(R3.logical), join(R4.logical))
      )
    val optimizedExpr = PushRenameToLeaf(MergeAllJoin(expr))

    val groundTruth =
      Join(
        Seq(
          Rename(Relation("R1"), Map("A" -> "B")),
          Relation("R2"),
          Relation("R3"),
          Relation("R4")
        ),
        mode = ExecMode.Coupled
      )
    assert(optimizedExpr == groundTruth)
  }
}
