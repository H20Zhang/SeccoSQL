package unit.optimization.plan

import org.apache.spark.secco.SeccoDataFrame
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.plan.{
  Filter,
  PairThenCompute,
  Project
}
import org.apache.spark.secco.types.{DataTypes, StructField, StructType}
import util.SeccoFunSuite

//TODO: implement the test.
class PairThenComputeSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    createTestEmptyRelation("R1", "a", "b")()
    createTestEmptyRelation("R2", "b", "c")()
    createTestEmptyRelation("R3", "c", "d")()
  }

  test("box and unbox") {

    // Check whether local plan can be boxed into PairThenCompute.
    val R1 = seccoSession.table("R1")
    val ds1 = R1.select("a < b")
    val localPlan = ExecMode.newPlanWithMode(ds1.logical, ExecMode.Computation)
    val ptc = PairThenCompute.box(
      ds1.logical.children,
      localPlan
    )

    assert(ptc.isInstanceOf[PairThenCompute])

    // Check whether localPlan in PairThenCompute be unboxed.
    val unboxedLocalPlan = ptc.unboxedPlan()

    assert(unboxedLocalPlan == localPlan)
  }

  test("merge") {

    val R1 = seccoSession.table("R1")
    val ds1 = R1.select("a < b")
    val ds2 = ds1.project("a")
    val plan = ds2.queryExecution.analyzedPlan
      .transformUp {
        case s: Filter =>
          PairThenCompute.box(
            s.children,
            ExecMode.newPlanWithMode(s, ExecMode.Computation)
          )
        case p: Project =>
          PairThenCompute.box(
            p.children,
            ExecMode.newPlanWithMode(p, ExecMode.Computation)
          )
      }
      .asInstanceOf[PairThenCompute]

    val mergedPlan = plan
      .mergeConsecutiveLocalStage()

    assert(
      mergedPlan.localPlan.relationalString == "∏[a#6](\uD835\uDF0E[(a#2 < b#5)]([0]))"
    )

//    val recoupledPlan = mergedPlan.recoupledPlan()

//    println(ds2.queryExecution.analyzedPlan)

//    println(recoupledPlan)

  }

  test("check_recoupledPlan") {}

}
