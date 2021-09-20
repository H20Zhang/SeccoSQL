package unit.optimization.statsEstimation.exact.deprecated

import org.apache.spark.secco.Dataset
import org.apache.spark.secco.optimization.statsEstimation.exact.ExactStatsPlanVisitor
import org.apache.spark.secco.optimization.statsEstimation.exact.deprecated._
import util.{SeccoFunSuite, UnitTestTag}

import scala.util.Try

class ExactStatsPlanVisitorSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    val seq1 = Seq(
      Array(1.0, 2.0),
      Array(2.0, 3.0),
      Array(3.0, 4.0),
      Array(4.0, 5.0),
      Array(5.0, 6.0)
    )

    Dataset.fromSeq(seq1, Some("R1"), Some(Seq("A", "B")))
    Dataset.fromSeq(seq1, Some("R2"), Some(Seq("B", "C")))
    Dataset.fromSeq(seq1, Some("R3"), Some(Seq("C", "D")))

    ExactRelationEstimation.setDefaultCardinality(5)
  }

  def R1 = dlSession.table("R1")
  def R2 = dlSession.table("R2")
  def R3 = dlSession.table("R3")

  // test if the ExactStatsPlanVisitor can initialize
  test("initialization", UnitTestTag) {
    val stats1 = ExactStatsPlanVisitor.visit(R1.logical)
    assert(stats1 != null)
  }

  //test if the set and get cardinality and selectivity of ExactEstimation classes work correctly.
  test("check_set_get_cardinality_selectivity_for_single_op", UnitTestTag) {

    val R12 = R1.naturalJoin(R2)
    val P1 = R1.project("A, B")
    val A1 = R1.aggregate("count(*) by A, B")
    val F1 = R1.select("A < B")

    // check ExactJoinEstimation
    ExactJoinEstimation.setDefaultCardinality(10)

    assert(
      ExactStatsPlanVisitor.visit(R12.logical).rowCount == 10
    )

    ExactJoinEstimation.setCardinalityMap(
      Map(
        Set("R1", "R2") -> 20
      )
    )

    assert(ExactStatsPlanVisitor.visit(R12.logical).rowCount == 20)

    //check filter
    ExactFilterEstimation.setDefaultSelectivity(1.0)

    assert(ExactStatsPlanVisitor.visit(F1.logical).rowCount == 5)

    ExactFilterEstimation.setSelectivityMap(
      Map(
        (Set(("A", "<", "B")), Set("R1")) -> 0.2
      )
    )

    assert(ExactStatsPlanVisitor.visit(F1.logical).rowCount == 1)

    //check project, aggregate
    ExactProjectEstimation.setDefaultSelectivity(1.0)
    ExactAggregateEstimation.setDefaultSelectivity(1.0)

    assert(ExactStatsPlanVisitor.visit(P1.logical).rowCount == 5)
    assert(ExactStatsPlanVisitor.visit(A1.logical).rowCount == 5)

    ExactProjectEstimation.setSelectivityMap(
      Map(
        (Set("A", "B"), Set("R1")) -> 0.2
      )
    )

    ExactAggregateEstimation.setSelectivityMap(
      Map(
        (Set("A", "B"), Set("R1")) -> 0.2
      )
    )

    assert(ExactStatsPlanVisitor.visit(P1.logical).rowCount == 1)
    assert(ExactStatsPlanVisitor.visit(A1.logical).rowCount == 1)

  }

  test("check_set_get_cardinality_selectivity_for_join_tree", UnitTestTag) {
    val R12 = R1.naturalJoin(R2)
    val R123 = R3.naturalJoin(R12)
    val P123 = R123.project("A, B")
    val A123 = R123.aggregate("count(*) by A, B")
    val F123 = R123.select("A < B")

    // check ExactJoinEstimation

    ExactJoinEstimation.setCardinalityMap(
      Map(
        Set("R1", "R2", "R3") -> 200
      )
    )

    assert(ExactStatsPlanVisitor.visit(R123.logical).rowCount == 200)

    //check filter

    ExactFilterEstimation.setSelectivityMap(
      Map(
        (Set(("A", "<", "B")), Set("R1", "R2", "R3")) -> 0.2
      )
    )

    assert(ExactStatsPlanVisitor.visit(F123.logical).rowCount == 40)

    //check project, aggregate

    ExactProjectEstimation.setSelectivityMap(
      Map(
        (Set("A", "B"), Set("R1", "R2", "R3")) -> 0.2
      )
    )

    ExactAggregateEstimation.setSelectivityMap(
      Map(
        (Set("A", "B"), Set("R1", "R2", "R3")) -> 0.2
      )
    )

    assert(ExactStatsPlanVisitor.visit(P123.logical).rowCount == 40)
    assert(ExactStatsPlanVisitor.visit(A123.logical).rowCount == 40)

  }

  test("check_throw_exception", UnitTestTag) {

    val R12 = R1.naturalJoin(R2)
    val P1 = R1.project("A, B")
    val A1 = R1.aggregate("count(*) by A, B")
    val F1 = R1.select("A < B")

    // check ExactJoinEstimation

    ExactJoinEstimation.setCardinalityMap(
      Map(
        Set("R1", "R2", "R3") -> 20
      )
    )

    assert(
      Try(ExactStatsPlanVisitor.visit(R12.logical).rowCount == 20).isFailure
    )

    //check filter

    ExactFilterEstimation.setSelectivityMap(
      Map(
        (Set(("A", ">", "B")), Set("R1")) -> 0.2
      )
    )

    assert(Try(ExactStatsPlanVisitor.visit(F1.logical).rowCount == 1).isFailure)

    //check project, aggregate
    ExactProjectEstimation.setSelectivityMap(
      Map(
        (Set("A", "B"), Set("R2")) -> 0.2
      )
    )

    ExactAggregateEstimation.setSelectivityMap(
      Map(
        (Set("A"), Set("R1")) -> 0.2
      )
    )

    assert(Try(ExactStatsPlanVisitor.visit(P1.logical).rowCount == 1).isFailure)
    assert(Try(ExactStatsPlanVisitor.visit(A1.logical).rowCount == 1).isFailure)

  }

}
