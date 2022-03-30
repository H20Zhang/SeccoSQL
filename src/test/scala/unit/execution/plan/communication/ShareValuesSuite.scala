package unit.execution.plan.communication

import org.apache.spark.secco.execution.plan.communication.ShareConstraint
import org.apache.spark.secco.expression.utils.{AttributeMap, AttributeSet}
import org.apache.spark.secco.optimization.plan.BinaryJoin
import util.SeccoFunSuite

class ShareValuesSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    createDummyRelation("R1", "a", "b")()
    createDummyRelation("R2", "b", "c")()
    createDummyRelation("R3", "b", "c", "d")()
  }

  test("ShareConstaint") {

    // Check initialization of ShareConstraint.
    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")
    val R12 = R1.join(R2, "R1.b = R2.b")
    val R123 = R12.join(R3, "R3.b = R2.b")

    val R12Plan = R12.queryExecution.analyzedPlan.asInstanceOf[BinaryJoin]
    val R123Plan = R123.queryExecution.analyzedPlan.asInstanceOf[BinaryJoin]

    val attrB = R12Plan.output(2)
    val shareConstraint1 =
      ShareConstraint(AttributeMap(Seq(attrB -> 1)), R12Plan.condition.get)
    val shareConstraint2 =
      ShareConstraint(AttributeMap(Seq()), R123Plan.condition.get)

    println(shareConstraint1.rawConstraint)
    println(shareConstraint1.equivalenceAttrs.toSet)

    // Check the constraint after merging with new constraint.
    shareConstraint1.addNewConstraints(shareConstraint2)
    println(shareConstraint1.rawConstraint)
    println(shareConstraint1.equivalenceAttrs.toSet)

  }

  test("ShareConstraint") {}

}
