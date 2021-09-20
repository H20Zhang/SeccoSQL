package unit.execution.plan.computation.utils

import org.apache.spark.secco.execution.plan.computation.utils.InternalRowHashMap
import util.{SeccoFunSuite, UnitTestTag}

class InternalRowHashMapSuite extends SeccoFunSuite {

  test("basic", UnitTestTag) {
    val keyAttr = Array("B")
    val localAttributeOrder = Array("B", "C")
    val content = Array(
      Array(1.0, 2.0),
      Array(2.0, 3.0),
      Array(2.0, 4.0)
    )

    val hashMap = InternalRowHashMap(keyAttr, localAttributeOrder, content)
    val key1 = Array(1.0)
    val value1 = hashMap.get(key1).map(_.toSeq).toSeq
    pprint.pprintln(s"value1:${value1}")

    val s = Array(0, 1, 2)

  }
}
