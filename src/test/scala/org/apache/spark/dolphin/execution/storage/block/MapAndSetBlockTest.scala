package org.apache.spark.dolphin.execution.storage.block

import org.apache.spark.dolphin.execution.storage.row._
import org.apache.spark.dolphin.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable.ArrayBuffer

class MapAndSetBlockTest extends FunSuite with BeforeAndAfter {
  var testMapBlock: HashMapInternalBlock = _
  var testSetBlock: HashSetInternalBlock = _
  val dictionaryOrder: Seq[String] = Seq("string0", "int1")
  val structField0: StructField = StructField("string0", StringType)
  val structField1: StructField = StructField("int1", IntegerType)
  val structField2: StructField = StructField("int2", IntegerType)
  val structField3: StructField = StructField("double3", DoubleType)
  val structField4: StructField = StructField("bool4", BooleanType)
  val structField5: StructField = StructField("double5", DoubleType)
  val structField6: StructField = StructField("string6", StringType)
  val fields: Array[StructField] = Array(structField0, structField1, structField2, structField3, structField4,
    structField5, structField6)
  val schema: StructType = StructType(fields)
  val theArray: Array[Any] = Array("abc", 1, 3, 4.0, true, 3.3, "variableLength")
  val theArray2: Array[Any] = Array("bcd", 2, 3, 4.0, true, 5.2, "variableLength")
  val theArray3: Array[Any] = Array("ecd", 3, 4, 4.0, true, 5.2, "variableLength")
  val theArray4: Array[Any] = Array("bcd", 3, 3, 4.0, true, 5.2, "variableLength")
  val genericInternalRow: GenericInternalRow = new GenericInternalRow(theArray)
  val genericInternalRow2: GenericInternalRow = new GenericInternalRow(theArray2)
  val genericInternalRow3: GenericInternalRow = new GenericInternalRow(theArray3)
  val genericInternalRow4: GenericInternalRow = new GenericInternalRow(theArray4)
  val unsafeInternalRow = new UnsafeInternalRow(7)
  val addedRows: Array[InternalRow] = Array[InternalRow](genericInternalRow, genericInternalRow2, genericInternalRow3, genericInternalRow4, unsafeInternalRow)

  before {
    // create two InternalRows with different type
    unsafeInternalRow.setString(0, "abc")
    unsafeInternalRow.setInt(1, 1)
    unsafeInternalRow.setInt(2, 3)
    unsafeInternalRow.setDouble(3, 4.0)
    unsafeInternalRow.setBoolean(4, true)
    unsafeInternalRow.setDouble(5, 3.3)
    unsafeInternalRow.setString(6, "variableLength")

    // build UnsafeInternalBlock and GenericInternalBlock based on the two
    val mapBlockBuilder = new HashMapInternalBlockBuilder(schema)
    mapBlockBuilder.add(genericInternalRow)
    mapBlockBuilder.add(genericInternalRow2)
    mapBlockBuilder.add(genericInternalRow3)
    mapBlockBuilder.add(genericInternalRow4)
    mapBlockBuilder.add(unsafeInternalRow)
    testMapBlock = mapBlockBuilder.build()

    val setBlockBuilder = new HashSetInternalBlockBuilder(schema)
    setBlockBuilder.add(genericInternalRow)
    setBlockBuilder.add(genericInternalRow2)
    setBlockBuilder.add(genericInternalRow3)
    setBlockBuilder.add(genericInternalRow4)
    setBlockBuilder.add(unsafeInternalRow)
    testSetBlock = setBlockBuilder.build()
  }

  test("test some basic functions") {
    // test size
    assert(testMapBlock.size() == 5)
    assert(testSetBlock.size() == 5)

    // test isEmpty
    assert(!testMapBlock.isEmpty() && !testSetBlock.isEmpty())

    // test nonEmpty
    assert(testMapBlock.nonEmpty() && testSetBlock.nonEmpty())

    // test schema
    assert(testMapBlock.schema() == schema && testSetBlock.schema() == schema)

    // test contains
    for (row <- addedRows) {
      assert(testMapBlock.contains(row))
      assert(testSetBlock.contains(row))
    }

    // test getDictionaryOrder
    assertThrows[Exception] {
      testMapBlock.getDictionaryOrder == Option(dictionaryOrder)
    }
    assertThrows[Exception] {
      testSetBlock.getDictionaryOrder == Option(dictionaryOrder)
    }
  }

  test("test get") {

  }

  test("test toArray") {
    val mapRowArray = testMapBlock.toArray()
    val setRowArray = testSetBlock.toArray()
    for (row <- addedRows) {
      assert(mapRowArray.contains(row))
      assert(setRowArray.contains(row))
    }
  }

  test("test iterator") {
    val mapIterator = testMapBlock.iterator
    val setIterator = testSetBlock.iterator
    val mapRows = new Array[InternalRow](testMapBlock.size().toInt)
    val setRows = new Array[InternalRow](testSetBlock.size().toInt)
    for (index <- 0 until testMapBlock.size().toInt) {
      mapRows(index) = mapIterator.next()
      setRows(index) = setIterator.next()
    }
    // all addedRows should be in the gotten rows
    assert(addedRows.forall(mapRows.contains(_)))
    assert(addedRows.forall(setRows.contains(_)))

    // all the gotten rows should be in the addedRows
    assert(mapRows.forall(addedRows.contains(_)))
    assert(setRows.forall(addedRows.contains(_)))
  }

  test("test merge") {
    // if sort, throw exception
    assertThrows[Exception]{
      val mapSetMergedBlock = testMapBlock.merge(testSetBlock)
    }
    assertThrows[Exception]{
      val mapSetMergedBlock = testSetBlock.merge(testSetBlock)
    }

    // if not sort, merge normally
    val mapSetMergedBlock = testMapBlock.merge(testSetBlock,maintainSortOrder = false)
    val setMapMergedBlock = testSetBlock.merge(testMapBlock,maintainSortOrder = false)
    assert(mapSetMergedBlock.size()==5)
    assert(setMapMergedBlock.size()==5)

  }
}
