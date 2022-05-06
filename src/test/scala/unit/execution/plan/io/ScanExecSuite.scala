package unit.execution.plan.io

import util.SeccoFunSuite

class ScanExecSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    createTestRelation(
      Seq(Seq(0, 1, 1), Seq(2, 3, 3), Seq(3, 4, 4)),
      "R1",
      "a",
      "b",
      "c"
    )()
  }

  test("simple") {
    val R1 = seccoSession.table("R1")
    val rows = R1.collect()
    println(rows(0).numFields)
  }

}
