package unit.execution.plan.communication

import org.apache.spark.secco.execution.plan.communication.{
  Coordinate,
  PartitionExchangeExec,
  ShareConstraint,
  ShareValues,
  ShareValuesContext
}
import org.apache.spark.secco.expression.utils.{AttributeMap, AttributeSet}
import org.apache.spark.secco.optimization.plan.BinaryJoin
import org.apache.spark.secco.optimization.util.EquiAttributes
import util.SeccoFunSuite

import scala.util.Try

class ShareValuesSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    createTestEmptyRelation("R1", "a", "b")()
    createTestEmptyRelation("R2", "b", "c")()
    createTestEmptyRelation("R3", "b", "c", "d")()

    createTestRelation(
      Seq(Seq(0, 1, 1), Seq(2, 3, 3), Seq(3, 4, 4)),
      "R4",
      "a",
      "b",
      "c"
    )()
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
      ShareConstraint.fromRawConstraintAndCond(
        AttributeMap(Seq(attrB -> 1)),
        R12Plan.condition.get
      )
    val shareConstraint2 =
      ShareConstraint.fromRawConstraintAndCond(
        AttributeMap(Seq()),
        R123Plan.condition.get
      )

    println(shareConstraint1.rawConstraint)
    println(shareConstraint1.equivalenceAttrs)

    // Check the constraint after merging with new constraint.
    shareConstraint1.addNewConstraints(shareConstraint2)
    println(shareConstraint1.rawConstraint)
    println(shareConstraint1.equivalenceAttrs)

  }

  test("ShareValues") {
    val R4 = seccoSession.table("R4")
    val inputExec = R4.queryExecution.executionPlan

    val attrA = inputExec.output(0)
    val attrB = inputExec.output(1)
    val attrC = inputExec.output(2)

    val equiAttrs =
      EquiAttributes.fromEquiAttributes(Seq(Seq(attrA), Seq(attrB, attrC)))
    val rawShares =
      AttributeMap(
        Seq((attrA, 2), (attrB, 3), (attrC, 3))
      )
    val shareValues = ShareValues(rawShares, equiAttrs)

    val sentry = shareValues.genSentryRows(Seq(attrA, attrB, attrC).toArray)

    println(sentry.toSeq)
  }

  test("coordinate") {
    val attrA = createTestAttribute("A")
    val attrB = createTestAttribute("B")
    val attrC = createTestAttribute("C")
    val attrD = createTestAttribute("D")

    val equiAttrs =
      EquiAttributes.fromEquiAttributes(
        Seq(Seq(attrA), Seq(attrB, attrC), Seq(attrD))
      )

    val coordinate1 =
      Coordinate(Array(attrA, attrC, attrD), Array(1, 2, 3), equiAttrs)

    assert(
      coordinate1.repAttrs.toSeq == Seq(attrA, attrB, attrD),
      "Every attribute's representative attribute must appear"
    )

    assert(
      Try(coordinate1.subCoordinate(Array(attrB, attrD))).isFailure,
      "subCooridnate should fail if there exists subAttribtues that is not included in the original attributes."
    )

    assert(
      coordinate1.subCoordinate(Array(attrC, attrD)).index.toSeq == Seq(2, 3),
      "subCoordinate does not correctly compute sub-index."
    )

  }

}
