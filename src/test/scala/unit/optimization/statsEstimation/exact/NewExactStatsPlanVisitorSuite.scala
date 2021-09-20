package unit.optimization.statsEstimation.exact

import org.apache.spark.secco.{Dataset, SeccoSession}
import org.apache.spark.secco.SeccoSession.setCurrentSession
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.optimization.SeccoOptimizer
import org.apache.spark.secco.optimization.plan.LocalStage
import org.apache.spark.secco.optimization.statsEstimation.exact.{
  ExactLogicalPlanEstimation,
  ExactStatsPlanVisitor
}
import util.{SeccoFunSuite, UnitTestTag}

import scala.util.Try

class NewExactStatsPlanVisitorSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {

    val conf = SeccoConfiguration.newDefaultConf()
    conf.setEnableOnlyDecoupleOptimization(true)
    conf.setEnableEarlyAggregationOptimization(false)
    SeccoSession.setCurrentSession(SeccoSession.newSessionWithConf(conf))

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

  }

  def R1 = dlSession.table("R1")

  def R2 = dlSession.table("R2")

  def R3 = dlSession.table("R3")

  // test if the ExactStatsPlanVisitor can initialize
  test("initialization", UnitTestTag) {
    ExactLogicalPlanEstimation.setCardinality("R1", 5)

    val stats1 = ExactStatsPlanVisitor.visit(R1.logical)
    assert(stats1 != null)
  }

  test("check_set_get_cardinality_without_decouple") {
    val R12 = R1.naturalJoin(R2)
    val P1 = R1.project("A, B")
    val A1 = R1.aggregate("count(*) by A, B")
    val F1 = R1.select("A < B")

    //check cardinality before decouple
    ExactLogicalPlanEstimation.setCardinality(R12.logical.relationalString, 5)
    ExactLogicalPlanEstimation.setCardinality(P1.logical.relationalString, 10)
    ExactLogicalPlanEstimation.setCardinality(A1.logical.relationalString, 20)
    ExactLogicalPlanEstimation.setCardinality(F1.logical.relationalString, 40)

    assert(ExactStatsPlanVisitor.visit(R12.logical).rowCount == 5)
    assert(ExactStatsPlanVisitor.visit(P1.logical).rowCount == 10)
    assert(ExactStatsPlanVisitor.visit(A1.logical).rowCount == 20)
    assert(ExactStatsPlanVisitor.visit(F1.logical).rowCount == 40)
  }

  test("check_set_get_cardinality_with_decouple") {
    val R12 = R1.naturalJoin(R2)
    val P1 = R1.project("A, B")
    val A1 = R1.aggregate("count(*) by A, B")
    val F1 = R1.select("A < B")

    val optimizer = SeccoSession.currentSession.sessionState.optimizer

    println(optimizer.execute(P1.logical))

    ExactLogicalPlanEstimation.setCardinality(R12.logical.relationalString, 5)
    ExactLogicalPlanEstimation.setCardinality(P1.logical.relationalString, 10)
    ExactLogicalPlanEstimation.setCardinality(A1.logical.relationalString, 20)
    ExactLogicalPlanEstimation.setCardinality(F1.logical.relationalString, 40)

    //check cardinality before decouple
//    ExactLogicalPlanEstimation.setCardinality("⋈(R1,R2)", 5)
//    ExactLogicalPlanEstimation.setCardinality("∏[A,B](∏[A,B](R1))", 10)
//    ExactLogicalPlanEstimation.setCardinality(
//      "[A,B]\uD835\uDF6A[count(c1)]([A,B]\uD835\uDF6A[count(*)](R1))",
//      20
//    )
//    ExactLogicalPlanEstimation.setCardinality("\uD835\uDF0E[A<B](R1)", 40)

//    check cardinality after decouple
    assert(
      ExactStatsPlanVisitor.visit(optimizer.execute(R12.logical)).rowCount == 5
    )

    assert(
      ExactStatsPlanVisitor.visit(optimizer.execute(P1.logical)).rowCount == 10
    )
    assert(
      ExactStatsPlanVisitor.visit(optimizer.execute(A1.logical)).rowCount == 20
    )
    assert(
      ExactStatsPlanVisitor.visit(optimizer.execute(F1.logical)).rowCount == 40
    )
  }

  test("check_throw_exception", UnitTestTag) {

    val R12 = R1.naturalJoin(R2)
    val P1 = R1.project("A, B")
    val A1 = R1.aggregate("count(*) by A, B")
    val F1 = R1.select("A < B")

    ExactLogicalPlanEstimation.clear()

    assert(
      Try(ExactStatsPlanVisitor.visit(R12.logical).rowCount == 5).isFailure
    )
    assert(
      Try(ExactStatsPlanVisitor.visit(F1.logical).rowCount == 10).isFailure
    )
    assert(
      Try(ExactStatsPlanVisitor.visit(P1.logical).rowCount == 20).isFailure
    )
    assert(
      Try(ExactStatsPlanVisitor.visit(A1.logical).rowCount == 40).isFailure
    )

  }
}
