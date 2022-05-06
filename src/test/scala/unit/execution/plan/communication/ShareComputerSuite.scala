package unit.execution.plan.communication

import org.apache.spark.secco.debug
import org.apache.spark.secco.execution.plan.communication.{
  EnumShareComputer,
  ShareConstraint
}
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.optimization.util.EquiAttributes
import util.SeccoFunSuite

class ShareComputerSuite extends SeccoFunSuite {

  test("basic") {

    val attrA = createTestAttribute("a")
    val attrB = createTestAttribute("b")
    val attrC = createTestAttribute("c")
    val attrD = createTestAttribute("d")
    val attrE = createTestAttribute("e")

    val schemas = Seq(
      Seq(attrA, attrB),
      Seq(attrC, attrD)
    )

    val rawConstraint = AttributeMap(Seq(attrA -> 1))
    val equiAttributes = EquiAttributes.fromEquiAttributes(
      Seq(Seq(attrA), Seq(attrB, attrC), Seq(attrD))
    )
    val shareConstraint = ShareConstraint(rawConstraint, equiAttributes)
    val tasks = 8
    val statisticMap = schemas
      .map(f => (f, 10L))
      .toMap

    val shareComputer =
      new EnumShareComputer(schemas, shareConstraint, tasks, statisticMap)

    val shareResults =
      shareComputer.optimalShareWithBudget()

    debug(shareResults)
  }
}
