package unit.execution.plan.support

import org.apache.spark.secco.execution.plan.support.FuncGenSupport
import org.scalatest.FunSuite
import util.UnitTestTag

class SupportSuite extends FunSuite {

  test("FuncGenSupport", UnitTestTag) {
    val funcGen = new FuncGenSupport {}

    val transformFunc1 =
      funcGen.genTransformFunc("0.85*weight3+0.001", Seq("src", "weight3"))
    val y1 = transformFunc1(Array(1.0, 1.5))
    assert(y1 == 0.85 * 1.5 + 0.001)

    val transformFunc2 =
      funcGen.genTransformFunc("src", Seq("src", "weight3"))
    val y2 = transformFunc2(Array(1.0, 1.5))
    assert(y2 == 1.0)

  }
}
