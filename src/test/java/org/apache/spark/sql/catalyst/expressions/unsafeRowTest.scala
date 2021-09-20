package org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.types.{DataType, IntegerType, LongType}
import org.scalatest._
class unsafeRowTest extends FunSuite with BeforeAndAfter {
  var testRow = new UnsafeRow(6)
  before{

    testRow = new UnsafeRow(6)
    testRow.pointTo(new Array[Byte](64), 64)
  }

  test("test setter and getter method") {
    testRow = new UnsafeRow(6)
    // Boolean
//    testRow.setBoolean(0, true)
//    assert(testRow.getBoolean(0))
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(LongType, LongType, IntegerType)
    val converter = factory.create(fieldTypes)
    val row = new SpecificInternalRow(fieldTypes)
    row.setLong(0, 0)
    row.setLong(1, 1)
    row.setInt(2, 2)

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.getSizeInBytes === 8 + (3 * 8))
    assert(unsafeRow.getLong(0) === 0)
    assert(unsafeRow.getLong(1) === 1)
    assert(unsafeRow.getInt(2) === 2)
  }
}
