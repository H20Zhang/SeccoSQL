/*
 *test cases written by Ruifeng Tan
 *Requirement: test cases satisfy 100% statement coverage and branch coverage
 *Time: 16/7/2021
 */
package org.apache.spark.dolphin.execution.storage.row

import org.apache.spark.dolphin.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.util.Date

class GenericInternalRowTest extends FunSuite with BeforeAndAfter {
  var testRow = new GenericInternalRow(3)
  val length = 7
  before {
    val theArray = Array("abc", 1, 3, 4.0, true, null)
    testRow = new GenericInternalRow(theArray)
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

//    // Short
//    val shortValue = (2).toShort
//    testRow.setShort(0, shortValue)
//    assert(testRow.get(0, ShortDataType).asInstanceOf[Short] == shortValue)
  }

  test("test get") {
    // BooleanType
    assert(testRow.get(4, BooleanType).asInstanceOf[Boolean])

    // IntegerType
    assert(testRow.get(2, IntegerType).asInstanceOf[Int] == 3)

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
//    testRow.update(2, arrayForTest)
//    val arrayGot = testRow.get(2, AnyDataType)
//    assert(arrayForTest == arrayGot && arrayForTest.sameElements(Array(1, 2)))
  }

  test("test setNullAt") {
    testRow.setNullAt(0)
    assert(testRow.isNullAt(0))
  }

  test("test using [this] method to construct a GenericInternalRow") {
    val row0 = new GenericInternalRow(3)
    assert(row0.numFields == 3)
  }

  test("test numFields") {
    assert(testRow.numFields == 6)
  }

  test("test copy and equal") {
    // row is not a instance of BaseGenericInternalRow
    val tmpRow0 = 1
    assert(!testRow.equals(tmpRow0))

    // row is a instance of BaseGenericInternalRow
    // testRow equals row
    val copiedRow = testRow.copy()
    val copiedRow2 = testRow.copy()
    assert(testRow.equals(copiedRow))
    assert(copiedRow.equals(copiedRow2))

    // To satisfy branch coverage
    // NaN test
    val copiedRow3 = testRow.copy()
    val copiedRow4 = testRow.copy()

    // Float NaN
    val nanFloatValue = Float.NaN
    copiedRow3.setFloat(3, nanFloatValue)
    assert(!copiedRow3.equals(copiedRow4))
    copiedRow4.setInt(3, 4)
    assert(!copiedRow3.equals(copiedRow4))
    copiedRow4.setFloat(3, (4.0).toFloat)
    assert(!copiedRow3.equals(copiedRow4))

    copiedRow4.setFloat(3, nanFloatValue)
    assert(copiedRow3.equals(copiedRow4))

    // Double NaN
    val nanDoubleValue = Double.NaN
    copiedRow3.setDouble(4, nanDoubleValue)
    assert(!copiedRow3.equals(copiedRow4))
    copiedRow4.setInt(4, 4)
    assert(!copiedRow3.equals(copiedRow4))
    copiedRow4.setDouble(4, 4.0)
    assert(!copiedRow3.equals(copiedRow4))

    copiedRow4.setDouble(4, nanDoubleValue)
    assert(copiedRow3.equals(copiedRow4))

    // Array[Byte] test
    val arrayByte = new Array[Byte](2)
    arrayByte(0) = (1).toByte
    arrayByte(1) = (2).toByte
    val testRow2 = new GenericInternalRow(Array(2, "abc", arrayByte))
    val testRow3 = new GenericInternalRow(Array(2, "abc", arrayByte))
    val testRow4 = new GenericInternalRow(Array(2, "abc", Array[Byte](3, 4)))
    assert(testRow2.equals(testRow3))
    assert(!testRow2.equals(testRow4))
    assert(!testRow2.equals(testRow))

    // null
    val copiedRow5 = testRow.copy()
    copiedRow5.setInt(5, 10)
    assert(!testRow.equals(copiedRow5))

    // others
    val copiedRow6 = testRow.copy()
    copiedRow6.setInt(1, 2)
    assert(!testRow.equals(copiedRow6))
  }

  test("test toString") {
    // test when row is empty
    val emptyRow = new GenericInternalRow(0)
    assert(emptyRow.toString == "[empty row]")

    // test when row is not empty
    assert(testRow.toString == "[abc,1,3,4.0,true,null]")
  }

  test("test if there is any null") {
    assert(testRow.anyNull)

    val copiedRow = testRow.copy()
    copiedRow.setInt(5, 2)
    assert(!copiedRow.anyNull)
  }

  test("test toSeq") {
    val seqOfDataType =
      Seq(
        StringType,
        IntegerType,
        IntegerType,
        DoubleType,
        BooleanType,
        BooleanType
      )
    val scalaSeq = testRow.toSeq(seqOfDataType)
    var i = 0
    for (value <- scalaSeq) {
      assert(value == testRow.get(i, seqOfDataType(i)))
      i += 1
    }
    val structFields = new Array[StructField](6)
    structFields(0) = StructField("test0", StringType, nullable = false)
    structFields(1) = StructField("test1", IntegerType, nullable = false)
    structFields(2) = StructField("test2", IntegerType, nullable = false)
    structFields(3) = StructField("test3", DoubleType, nullable = false)
    structFields(4) = StructField("test4", BooleanType, nullable = false)
    structFields(5) = StructField("test5", BooleanType, nullable = true)
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
    // test using extremely long tuple
    val originalArray = new Array[Any](threshold.toInt)
    val testRow2 = new GenericInternalRow(threshold.toInt)
    // test using short tuple
    //    val originalArray = new Array[Any](length)
    //    val testRow2 = new GenericInternalRow(length)

    // write
    // original Array
    i = 0
    var start_time = new Date().getTime
    while (i < threshold) {
      originalArray(i) = i
      i += 1
    }
    var end_time = new Date().getTime
    var sencondsCosted = end_time - start_time
    info(s" the orginalArray write $threshold times cost $sencondsCosted ms")

    // GenericInternalRow
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
      testRow2.setLong(i, i)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(
      s" the GenericInternalRow write $threshold times cost $sencondsCosted ms"
    )

    // read
    // original Array
    start_time = new Date().getTime
    while (i < threshold) {
      var _ = originalArray(i)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(s" the original Array read $threshold times cost $sencondsCosted ms")

    // GenericInternalRow
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
      var _ = testRow2.get(i, LongType)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(
      s" the GenericInternalRow read $threshold times cost $sencondsCosted ms"
    )

    // update
    // originalArray
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
      originalArray(i) = i
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(s" the originalArray update $threshold times cost $sencondsCosted ms")

    // GenericInternalRow
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
      testRow2.update(i, i)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(
      s" the GenericInternalRow update $threshold times cost $sencondsCosted ms"
    )
  }

  test("Run a comprehensive test for read, write, update") {
    var i = 0
    val threshold = 1e7
    // test using extremely long tuple
    val originalArray = new Array[Any](length)
    val testRow2 = new GenericInternalRow(length)
    // test using short tuple
    // read
    // original Array
    var start_time = new Date().getTime
    while (i < threshold) {
      originalArray(i % length) = (i).toLong
      var tmp = originalArray(i % length).asInstanceOf[Long]
      originalArray(i % length) = tmp + 1
      i += 1
    }
    var end_time = new Date().getTime
    var sencondsCosted = end_time - start_time
    info(
      s"Operations on the original Array for $threshold times cost $sencondsCosted ms"
    )

    // GenericInternalRow
    i = 0
    start_time = new Date().getTime
    while (i < threshold) {
      testRow2.setLong(i % length, (i).toLong)
      var tmp = testRow2.getLong(i % length)
      testRow2.setLong(i % length, tmp + 1)
      i += 1
    }
    end_time = new Date().getTime
    sencondsCosted = end_time - start_time
    info(
      s"Operations on GenericInternalRow for  $threshold times cost $sencondsCosted ms"
    )
  }
}
