package unit.execution.storage.block

import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.util.Date

/** These test cases are used to test both UnsafeInternalBlock and GenericInternalBlock
  * tested by trf
  */
class UnsafeAndGenericBlockTest extends FunSuite with BeforeAndAfter {
  var testUnsafeBlock: UnsafeInternalBlock = _
  var testGenericBlock: GenericInternalBlock = _
  val dictionaryOrder: Seq[String] = Seq("string0", "int1")
  val structField0: StructField = StructField("string0", StringType)
  val structField1: StructField = StructField("int1", IntegerType)
  val structField2: StructField = StructField("int2", IntegerType)
  val structField3: StructField = StructField("double3", DoubleType)
  val structField4: StructField = StructField("bool4", BooleanType)
  val structField5: StructField = StructField("double5", DoubleType)
  val structField6: StructField = StructField("string6", StringType)
  val fields: Array[StructField] = Array(
    structField0,
    structField1,
    structField2,
    structField3,
    structField4,
    structField5,
    structField6
  )
  val schema: StructType = StructType(fields)
  val theArray: Array[Any] =
    Array("abc", 1, 3, 4.0, true, null, "variableLength")
  val theArray2: Array[Any] =
    Array("bcd", 2, 3, 4.0, true, 5.2, "variableLength")
  val theArray3: Array[Any] =
    Array("ecd", 3, 4, 4.0, true, 5.2, "variableLength")
  val theArray4: Array[Any] =
    Array("bcd", 3, 3, 4.0, true, 5.2, "variableLength")

  before {
    // create two InternalRows with different type
    val unsafeInternalRow = new UnsafeInternalRow(7)

    val genericInternalRow: GenericInternalRow =
      new GenericInternalRow(theArray)
    val genericInternalRow2: GenericInternalRow =
      new GenericInternalRow(theArray2)
    val genericInternalRow3: GenericInternalRow =
      new GenericInternalRow(theArray3)
    val genericInternalRow4: GenericInternalRow =
      new GenericInternalRow(theArray4)
    unsafeInternalRow.setString(0, "abc")
    unsafeInternalRow.setInt(1, 1)
    unsafeInternalRow.setInt(2, 3)
    unsafeInternalRow.setDouble(3, 4.0)
    unsafeInternalRow.setBoolean(4, true)
    unsafeInternalRow.setNullAt(5)
    unsafeInternalRow.setString(6, "variableLength")

    // build UnsafeInternalBlock and GenericInternalBlock based on the two
    val unsafeBuilder = UnsafeInternalBlock.builder(schema)
    unsafeBuilder.add(genericInternalRow)
    unsafeBuilder.add(genericInternalRow2)
    unsafeBuilder.add(genericInternalRow3)
    unsafeBuilder.add(genericInternalRow4)
    unsafeBuilder.add(unsafeInternalRow)
    testUnsafeBlock = unsafeBuilder.build()
    //    testUnsafeBlock.show(testUnsafeBlock.size().toInt)

    val genericBuilder = GenericInternalBlock.builder(schema)
    genericBuilder.add(genericInternalRow)
    genericBuilder.add(genericInternalRow2)
    genericBuilder.add(genericInternalRow3)
    genericBuilder.add(genericInternalRow4)
    genericBuilder.add(unsafeInternalRow)
    testGenericBlock = genericBuilder.build()
    //    testGenericBlock.show(testGenericBlock.size().toInt)
  }

  test("test some basic functions") {
    // getRow
    val unsafeRow0 = testUnsafeBlock.getRow(0)
    val genericRow0 = testGenericBlock.getRow(0)
    assert(unsafeRow0.getString(0) == genericRow0.getString(0))
    assert(unsafeRow0.getInt(1) == genericRow0.getInt(1))
    assert(unsafeRow0.getInt(2) == genericRow0.getInt(2))
    assert(unsafeRow0.getDouble(3) == genericRow0.getDouble(3))
    assert(unsafeRow0.getBoolean(4) && genericRow0.getBoolean(4))
    assert(unsafeRow0.getDouble(5) == genericRow0.getDouble(5))
    assert(unsafeRow0.getString(6) == genericRow0.getString(6))

    // schema()
    assert(testUnsafeBlock.schema() == testGenericBlock.schema())

    // size
    assert(
      testUnsafeBlock.size() == 5 && testUnsafeBlock.size() == testGenericBlock
        .size()
    )

    // isEmpty
    assert(!testUnsafeBlock.isEmpty() && !testGenericBlock.isEmpty())

    // nonEmpty
    assert(testUnsafeBlock.nonEmpty() && testGenericBlock.nonEmpty())
  }

  test("test iterator") {
    val unsafeIterator = testUnsafeBlock.iterator
    val genericIterator = testGenericBlock.iterator
    while (unsafeIterator.hasNext) {
      assert(genericIterator.hasNext)
      val unsafeRow = unsafeIterator.next()
      val genericRow = genericIterator.next()
      assert(unsafeRow.getString(0) == genericRow.getString(0))
      assert(unsafeRow.getInt(1) == genericRow.getInt(1))
      assert(unsafeRow.getInt(2) == genericRow.getInt(2))
      assert(unsafeRow.getDouble(3) == genericRow.getDouble(3))
      assert(unsafeRow.getBoolean(4) == genericRow.getBoolean(4))
      assert(unsafeRow.getDouble(5) == genericRow.getDouble(5))
      assert(unsafeRow.getString(6) == genericRow.getString(6))
    }
  }

  test("test toArray") {
    val unsafeRowArray = testUnsafeBlock.toArray()
    val genericRowArray = testGenericBlock.toArray()
    for (index <- unsafeRowArray.indices) {
      val unsafeRow = unsafeRowArray(index)
      val genericRow = genericRowArray(index)
      assert(unsafeRow.getString(0) == genericRow.getString(0))
      assert(unsafeRow.getInt(1) == genericRow.getInt(1))
      assert(unsafeRow.getInt(2) == genericRow.getInt(2))
      assert(unsafeRow.getDouble(3) == genericRow.getDouble(3))
      assert(unsafeRow.getBoolean(4) == genericRow.getBoolean(4))
      assert(unsafeRow.getDouble(5) == genericRow.getDouble(5))
      assert(unsafeRow.getString(6) == genericRow.getString(6))
    }
    genericRowArray(0).setString(0, "aaa")
    val row0 = testGenericBlock.getRow(0)
    assert(row0.getString(0) == "abc")
  }

  test("test sortBy") {
    val sortedUnsafeBlock = testUnsafeBlock.sortBy(dictionaryOrder)
    val sortedGenericBlock = testGenericBlock.sortBy(dictionaryOrder)
    // first, there should be same results, check it
    val unsafeRowArray = sortedUnsafeBlock.toArray()
    val genericRowArray = sortedGenericBlock.toArray()
    for (index <- unsafeRowArray.indices) {
      val unsafeRow = unsafeRowArray(index)
      val genericRow = genericRowArray(index)
      assert(unsafeRow.getString(0) == genericRow.getString(0))
      assert(unsafeRow.getInt(1) == genericRow.getInt(1))
      assert(unsafeRow.getInt(2) == genericRow.getInt(2))
      assert(unsafeRow.getDouble(3) == genericRow.getDouble(3))
      assert(unsafeRow.getBoolean(4) == genericRow.getBoolean(4))
      assert(unsafeRow.getDouble(5) == genericRow.getDouble(5))
      assert(unsafeRow.getString(6) == genericRow.getString(6))
    }

    // Then, check if the sorted order is right
    val sortedRow0 =
      sortedUnsafeBlock.asInstanceOf[UnsafeInternalBlock].getRow(0)
    val sortedRow1 =
      sortedUnsafeBlock.asInstanceOf[UnsafeInternalBlock].getRow(1)
    val sortedRow2 =
      sortedUnsafeBlock.asInstanceOf[UnsafeInternalBlock].getRow(2)
    val sortedRow3 =
      sortedUnsafeBlock.asInstanceOf[UnsafeInternalBlock].getRow(3)
    val sortedRow4 =
      sortedUnsafeBlock.asInstanceOf[UnsafeInternalBlock].getRow(4)
    assert(sortedRow0.getString(0) == "abc" && sortedRow0.getInt(1) == 1)
    assert(sortedRow1.getString(0) == "abc" && sortedRow1.getInt(1) == 1)
    assert(sortedRow2.getString(0) == "bcd" && sortedRow2.getInt(1) == 2)
    assert(sortedRow3.getString(0) == "bcd" && sortedRow3.getInt(1) == 3)
    assert(sortedRow4.getString(0) == "ecd" && sortedRow4.getInt(1) == 3)
  }

  test("test the assertion in merge") {
    val anotherDictionaryOrder: Seq[String] = Seq("int1", "int2")
    assertThrows[AssertionError] {
      testUnsafeBlock.merge(testGenericBlock)
    }
    assertThrows[AssertionError] {
      testGenericBlock.merge(testUnsafeBlock)
    }
    assertThrows[AssertionError] {
      val sortedUnsafeBlock = testUnsafeBlock.sortBy(dictionaryOrder)
      sortedUnsafeBlock.merge(testGenericBlock)
    }
    assertThrows[AssertionError] {
      val sortedGenericBlock = testGenericBlock.sortBy(dictionaryOrder)
      val sortedUnsafeBlock = testUnsafeBlock.sortBy(anotherDictionaryOrder)
      sortedGenericBlock.merge(sortedUnsafeBlock)
    }
    assertThrows[AssertionError] {
      val sortedGenericBlock = testGenericBlock.sortBy(dictionaryOrder)
      val sortedUnsafeBlock = testUnsafeBlock.sortBy(anotherDictionaryOrder)
      sortedUnsafeBlock.merge(sortedGenericBlock)
    }
  }

  test("test the assertion in merge (supply to above)") {
    assertThrows[AssertionError] {
      val sortedGenericBlock = testGenericBlock.sortBy(dictionaryOrder)
      sortedGenericBlock.merge(testUnsafeBlock)
    }
  }

  test("test merge") {
    // merge and sort
    val sortedGenericBlock = testGenericBlock.sortBy(dictionaryOrder)
    val sortedUnsafeBlock = testUnsafeBlock.sortBy(dictionaryOrder)
    val mergedSortedBlock0 = sortedUnsafeBlock.merge(sortedGenericBlock)
    val mergedSortedBlock1 = sortedGenericBlock.merge(sortedUnsafeBlock)
    assert(mergedSortedBlock0.size() == mergedSortedBlock1.size())
    assert(mergedSortedBlock0.size() == 10)
    assert(mergedSortedBlock1.size() == 10)
    val mergedSortedBlockArray0 = mergedSortedBlock0.toArray()
    val mergedSortedBlockArray1 = mergedSortedBlock1.toArray()
    val sotedExpectedString = Array[String](
      "abc",
      "abc",
      "abc",
      "abc",
      "bcd",
      "bcd",
      "bcd",
      "bcd",
      "ecd",
      "ecd"
    )
    val sotedExpectedInt = Array[Int](1, 1, 1, 1, 2, 2, 3, 3, 3, 3)
    for (index <- mergedSortedBlockArray1.indices) {
      val row0 = mergedSortedBlockArray0(index)
      val row1 = mergedSortedBlockArray1(index)
      assert(
        row0.getString(0) == sotedExpectedString(index) && row0
          .getInt(1) == sotedExpectedInt(index)
      )
      assert(
        row1.getString(0) == sotedExpectedString(index) && row1
          .getInt(1) == sotedExpectedInt(index)
      )
    }

    // merge without sorting
    val mergedUnSotedBlock0 = sortedUnsafeBlock.merge(sortedGenericBlock, false)
    val mergedUnSotedBlock1 = sortedUnsafeBlock.merge(sortedUnsafeBlock, false)
    assert(mergedUnSotedBlock0.size() == mergedUnSotedBlock1.size())
    val mergedUnSortedBlockArray0 = mergedUnSotedBlock0.toArray()
    val mergedUnSortedBlockArray1 = mergedUnSotedBlock1.toArray()
    val unsotedExpetedString = Array[String](
      "abc",
      "abc",
      "bcd",
      "bcd",
      "ecd",
      "abc",
      "abc",
      "bcd",
      "bcd",
      "ecd"
    )
    val unsotedExpetedInt = Array[Int](1, 1, 2, 3, 3, 1, 1, 2, 3, 3)
    for (index <- mergedUnSortedBlockArray0.indices) {
      val row0 = mergedUnSortedBlockArray0(index)
      val row1 = mergedUnSortedBlockArray1(index)
      assert(
        row0.getString(0) == unsotedExpetedString(index) && row0
          .getInt(1) == unsotedExpetedInt(index)
      )
      assert(
        row1.getString(0) == unsotedExpetedString(index) && row1
          .getInt(1) == unsotedExpetedInt(index)
      )
    }

  }

  test("test performance of sortBy") {
    val genericInternalRow: GenericInternalRow =
      new GenericInternalRow(theArray)
    val genericInternalRow2: GenericInternalRow =
      new GenericInternalRow(theArray2)
    val genericInternalRow3: GenericInternalRow =
      new GenericInternalRow(theArray3)
    val genericInternalRow4: GenericInternalRow =
      new GenericInternalRow(theArray4)
    val unsafeInternalRow = new UnsafeInternalRow(7)
    unsafeInternalRow.setString(0, "abc")
    unsafeInternalRow.setInt(1, 1)
    unsafeInternalRow.setInt(2, 3)
    unsafeInternalRow.setDouble(3, 4.0)
    unsafeInternalRow.setBoolean(4, true)
    unsafeInternalRow.setNullAt(5)
    unsafeInternalRow.setString(6, "variableLength")
    val rows = Array[InternalRow](
      genericInternalRow,
      genericInternalRow2,
      genericInternalRow3,
      genericInternalRow4,
      unsafeInternalRow
    )
    val unsafeBuilder = new UnsafeInternalRowBlockBuilder(schema)
    val genericBuilder = new GenericInternalRowBlockBuilder(schema)

    // add rows to builder
    val addTimes = 5000
    for (i <- 0 until addTimes) {
      val index = i % 5
      unsafeBuilder.add(rows(index))
      genericBuilder.add(rows(index))
    }

    // build the block
    val unsafeBlock = unsafeBuilder.build()
    val genericBlock = genericBuilder.build()

    // sort Unsafe
    var start_time = new Date().getTime
    unsafeBlock.sortBy(dictionaryOrder)
    var end_time = new Date().getTime
    var costTime = end_time - start_time
    val unsafeRowsNum = unsafeBlock.size()
    info(
      s"The sort of $unsafeRowsNum rows UnsafeInternalBlock cost $costTime ms"
    )

    // sort Generic
    start_time = new Date().getTime
    genericBlock.sortBy(dictionaryOrder)
    end_time = new Date().getTime
    costTime = end_time - start_time
    val genericRowsNum = genericBlock.size()
    info(
      s"The sort of $genericRowsNum rows GenericInternalBlock cost $costTime ms"
    )
  }

}
