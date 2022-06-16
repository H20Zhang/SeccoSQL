/*
 * Write test cases for UnsafeInternalRow
 * Written by trf and lgh
 */
package unit.execution.storage.row

import org.apache.spark.secco.execution.storage.row.UnsafeInternalRow
import org.apache.spark.secco.types._
import org.scalatest._

import java.util.Date

class UnsafeInternalRowTest extends FunSuite with BeforeAndAfter {
//  var testRow = new UnsafeInternalRow(7, true)
  var testRow: UnsafeInternalRow = _
  val length = 7
  before {
    //    val theArray = Array("abc", 1, 3, 4.0, true, null)
    testRow = new UnsafeInternalRow(numFields = 7, autoInit = true)
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

  test("test setter and getter methods") {
    // Boolean
    testRow.setBoolean(0, true)
    assert(testRow.getBoolean(0))

    // Int
    testRow.setInt(1, 1)
    assert(testRow.getInt(1) == 1)

    // Long
    val longValue = (2).toLong
    testRow.setLong(1, longValue)
    assert(testRow.getLong(1) == longValue)

    // Float
    val floatValue = (2.0).toFloat
    testRow.setFloat(1, floatValue)
    assert(testRow.getFloat(1) == floatValue)

    // Double
    testRow.setDouble(1, 2.0)
    assert(testRow.getDouble(1) == 2.0)

    // String
    testRow.setString(0, "test")
    assert(testRow.getString(0) == "test")
    assert(testRow.get(0, StringType).asInstanceOf[String] == "test")
    assert(testRow.getString(6) == "variableLength")
    assert(testRow.getString(5) == null)

//    // Byte
//    val byteValue = (1).toByte
//    testRow.setByte(1, byteValue)
//    //    assert(testRow.get(1, AnyDataType).asInstanceOf[Byte] == byteValue)
//    assert(testRow.get(1, AnyDataType).asInstanceOf[Long].toByte == byteValue)
//
//    // Short
//    val shortValue = (2).toShort
//    testRow.setShort(2, shortValue)
//    //    assert(testRow.get(2, AnyDataType).asInstanceOf[Short] == shortValue)
//    assert(testRow.get(2, AnyDataType).asInstanceOf[Long].toShort == shortValue)
  }

  test("test get") {
    // BooleanType
    assert(testRow.get(4, BooleanType).asInstanceOf[Boolean])

    // IntegerType
    assert(testRow.get(2, IntegerType).asInstanceOf[Int] == 3)

//    // Short
//    assert(testRow.get(2, AnyDataType).asInstanceOf[Long].toShort == 3)

    // LongType
    val longValue = (1).toLong
    testRow.setLong(1, longValue)
    assert(testRow.get(1, LongType).asInstanceOf[Long] == longValue)

    // FloatType
    val floatValue = (1.0).toFloat
    testRow.setFloat(1, floatValue)
    assert(testRow.get(1, FloatType).asInstanceOf[Float] == floatValue)

    // DoubleType
    assert(testRow.get(3, DoubleType).asInstanceOf[Double] == 4.0)

    // StringType
    assert(testRow.get(0, StringType).asInstanceOf[String] == "abc")

//    // others
//    val arrayForTest = Array(1, 2)
//    testRow.update(2, arrayForTest) // lgh: UnsafeInternalRow doesn't support updating value as Array, nothing will be done
//    val arrayGot = testRow.get(2, AnyDataType).asInstanceOf[Long]  //lgh: the value gotten is unknown, uncertain
//    assert(arrayGot == 3)
  }

  test("test setNullAt") {
    testRow.setNullAt(0)
    assert(testRow.isNullAt(0))
  }

  test("test using [this] method to construct a UnsafeInternalRow") {
    val row0 = new UnsafeInternalRow(3, true)
    assert(row0.numFields == 3)
  }

  test("test numFields") {
    assert(testRow.numFields == 7)
  }

  test("test grow") {
    val numFields = 64
    val testRow2 = new UnsafeInternalRow(numFields, true)
//    testRow2.initWithByteArray(new Array[Byte](64 * 9), 64 * 9)
//    for (i <- 0 to 63) testRow2.setNullAt(i)
//    testRow2.setLong(0, (1).toLong)
    testRow2.setString(0, "asf")
    testRow2.setString(1, "abcdefgh") // occupy the allocated space tightly
    assert(testRow2.getString(1) == "abcdefgh")
    testRow2.setString(1, "abcdefghi") // grow a little
    assert(testRow2.getString(1) == "abcdefghi")

//    // exception test
//    assertThrows[IllegalArgumentException]({
//      for (i <- 0 to 63) {
//        testRow2.setString(i, new String(new Array[Char](Integer.MAX_VALUE / 32)))
//        // lgh: In fact, OutOfMemoryError would be thrown before the neededSize exceeds the
//        // threshold that would trigger IllegalArgumentException that we ourselves defined.
//        // lgh: It's only after I have changed the threshold smaller such as [Integer.MAX_VALUE /  2 - 63]
//        // that the IllegalArgumentException was thrown before OutOfMemoryError triggered.
//        // lgh: I think that creating an object with "new" may allocate the needed memory inside the heap,
//        // I am attempting to use _UNSAFE.allocateMemory() to do this job and this way, the allocated memory would
//        // be off heap.
//      }
//    }) // grow again
  }

  test("test update") {
    val testRow2 = new UnsafeInternalRow(2, true)
//    testRow2.initWithByteArray(new Array[Byte](24), 32)
//    for (i <- 0 to 1) testRow2.setNullAt(i)
    testRow2.setLong(0, (1).toLong)
    testRow2.setString(1, "CUHKWHU666")
    assert(testRow2.getString(1) == "CUHKWHU666")
    testRow2.update(1, "CU")
    assert(testRow2.getString(1) == "CU")

    val testRow3 = new UnsafeInternalRow(3, true)
//    testRow3.initWithByteArray(new Array[Byte](32), 32)
//    for (i <- 0 to 2) testRow3.setNullAt(i)
    testRow3.update(0, "test")
    testRow3.update(0, "spearsArmy")
    testRow3.setString(1, "abc")
    assert(testRow3.getString(1) == "abc")
  }

  test("test copy") {
    val copiedRow = testRow.copy()
    val seqOfDataType = Seq(
      StringType,
      IntegerType,
      IntegerType,
      DoubleType,
      BooleanType,
      BooleanType,
      StringType
    )
    val scalaSeq = testRow.toSeq(seqOfDataType)
    val copiedScalaSeq = copiedRow.toSeq(seqOfDataType)
    assert(scalaSeq.equals(copiedScalaSeq))
  }

  test("test if there is any null") {
    assert(testRow.anyNull)

    val copiedRow = testRow.copy()
    copiedRow.setInt(5, 2)
    assert(!copiedRow.anyNull)
  }

  test("test toSeq") {
    val seqOfDataType = Seq(
      StringType,
      IntegerType,
      IntegerType,
      DoubleType,
      BooleanType,
      BooleanType,
      StringType
    )
    val scalaSeq = testRow.toSeq(seqOfDataType)
    var i = 0
    for (value <- scalaSeq) {
      assert(value == testRow.get(i, seqOfDataType(i)))
      i += 1
    }
    val structFields = new Array[StructField](7)
    structFields(0) = StructField("test0", StringType, nullable = false)
    structFields(1) = StructField("test1", IntegerType, nullable = false)
    structFields(2) = StructField("test2", IntegerType, nullable = false)
    structFields(3) = StructField("test3", DoubleType, nullable = false)
    structFields(4) = StructField("test4", BooleanType, nullable = false)
    structFields(5) = StructField("test5", BooleanType, nullable = true)
    structFields(6) = StructField("test6", StringType, nullable = false)
    val schema = StructType(structFields)
    val schemaSeq = testRow.toSeq(schema)
    i = 0
    for (value <- schemaSeq) {
      assert(value == testRow.get(i, structFields(i).dataType))
      i += 1
    }

  }

  test("test performance for write, read, update") {
    var i = 0
    val threshold = 1e7
    val originalArray = new Array[Any](length)

    // UnsafeInternalRow
//    val backingArray = new Array[Byte]((length + 2) * 8)
//    val testRow2 = new UnsafeInternalRow(length, true)
//    testRow2.initWithByteArray(backingArray, (length + 2) * 8)
    val testRow2 = testRow.copy()

    // write
    // original Array
    i = 0
    var start_time = new Date().getTime
    while (i < threshold) {
      originalArray(i % length) = i
      i += 1
    }
    var end_time = new Date().getTime
    var sencondsCosted = end_time - start_time
    info(s" the orginalArray write $threshold times cost $sencondsCosted ms")

    // UnsafeInternalRow
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
      testRow2.setInt(i % length, i)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(
      s" the UnsafeInternalRow write $threshold times cost $sencondsCosted ms"
    )

    // read
    // original Array
    start_time = new Date().getTime
    while (i < threshold) {
      var _ = originalArray(i % length)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(s" the original Array read $threshold times cost $sencondsCosted ms")

    // UnsafeInternalRow
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
//      var _ = testRow2.get(i % length, LongType)
      var _ = testRow2.getLong(i % length)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(
      s" the UnsafeInternalRow read $threshold times cost $sencondsCosted ms"
    )

    // update
    // originalArray
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
      originalArray(i % length) = i
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(s" the originalArray update $threshold times cost $sencondsCosted ms")

    // UnsafeInternalRow
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
      testRow2.update(i % length, i)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(
      s" the UnsafeInternalRow update $threshold times cost $sencondsCosted ms"
    )
  }

  test("Run a comprehensive test for read, write, update") {
    var i = 0
    val threshold = 1e7
    //    testRow2.initWithByteArray(backingArray, threshold.toInt * 8 + (100000000 + 63) / 64 * 8)
    val originalArray = new Array[Any](length)
    // original Array
    var start_time = new Date().getTime
    while (i < threshold) {
      originalArray(i % length) = i.toLong
      var tmp = originalArray(i % length).asInstanceOf[Long]
      originalArray(i % length) = tmp + 1
      i += 1
    }
    var end_time = new Date().getTime
    var sencondsCosted = end_time - start_time
    info(
      s"Operations on the original Array for $threshold times cost $sencondsCosted ms"
    )

    // UnsafeInternalRow
    val testRow2 = new UnsafeInternalRow(length, true)
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
      testRow2.setLong(i % length, i)
      var tmp = testRow2.getLong(i % length)
      testRow2.setLong(i % length, tmp + 1)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(
      s"Operations on the UnsafeInternalRow for $threshold times cost $sencondsCosted ms"
    )
  }

  test("test_copy"){
    val rowCopyArray = new Array[UnsafeInternalRow](5)
    for(idx <- rowCopyArray.indices){
      rowCopyArray(idx) = testRow.copy().asInstanceOf[UnsafeInternalRow]
      println(s"rowwCopy $idx:" + rowCopyArray(idx))
      println(s"testRow: " + testRow )
    }
  }
}
