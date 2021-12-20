package unit.execution.plan.computation.newIter

import org.apache.spark.secco.execution.storage.row.UnsafeInternalRow
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable.ArrayBuffer

class TableIteratorSuite extends FunSuite with BeforeAndAfter{
  var testRows: ArrayBuffer[UnsafeInternalRow] = ArrayBuffer[UnsafeInternalRow]()
  var testRow: UnsafeInternalRow = _
  val length = 7
  before {
    //    val theArray = Array("abc", 1, 3, 4.0, true, null)
    testRow = new UnsafeInternalRow(7, true)
    //    testRow.initWithByteArray(new Array[Byte](80), 80)
    //    for (i <- 0 to 6) testRow.setNullAt(i)
    testRow.setString(0, "abc")
    testRow.setInt(1, 1)
    //    testRow.setInt(2,3)
    testRow.setShort(2, 3)
    testRow.setDouble(3, 4.0)
    testRow.setBoolean(4, true)
    testRow.setNullAt(5)
    testRow.setString(6, "variableLength")
  }
}
