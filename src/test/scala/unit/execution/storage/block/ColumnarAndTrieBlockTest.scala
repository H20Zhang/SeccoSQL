package unit.execution.storage.block

import org.apache.spark.secco.execution.storage.Utils
import org.apache.spark.secco.execution.storage.block.{ColumnarInternalBlock, ColumnarInternalBlockBuilder, TrieInternalBlock, TrieInternalBlockBuilder}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.spark.secco.execution.storage.row.{GenericInternalRow, InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.types._

class ColumnarAndTrieBlockTest extends FunSuite with BeforeAndAfter {

  print("This is beginning aaa")

  var testColumnarBlock: ColumnarInternalBlock = _
  var testTrieBlock: TrieInternalBlock = _
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
  val unsafeInternalRow = new UnsafeInternalRow(numFields = 7, autoInit = true)
  val addedRows: Array[InternalRow] = Array[InternalRow](genericInternalRow, genericInternalRow2, genericInternalRow3, genericInternalRow4, unsafeInternalRow)

  before {
    // create two InternalRows with different type
    unsafeInternalRow.setString(0, vStr = "abc")
    unsafeInternalRow.setInt(1, value = 1)
    unsafeInternalRow.setInt(2, 2)
    unsafeInternalRow.setDouble(3, 4.0)
    unsafeInternalRow.setBoolean(4, true)
    unsafeInternalRow.setDouble(5, 3.3)
    unsafeInternalRow.setString(6, "variableLength")

    // build ColumnarInternalBlockBuilder and TrieInternalBlockBuilder based on the two
    val columnarBlockBuilder = new ColumnarInternalBlockBuilder(schema)
    columnarBlockBuilder.add(genericInternalRow)
    columnarBlockBuilder.add(genericInternalRow2)
    columnarBlockBuilder.add(genericInternalRow3)
    columnarBlockBuilder.add(genericInternalRow4)
    columnarBlockBuilder.add(unsafeInternalRow)
    testColumnarBlock = columnarBlockBuilder.build()

    val trieBlockBuilder = new TrieInternalBlockBuilder(schema)
    trieBlockBuilder.add(genericInternalRow)
    trieBlockBuilder.add(genericInternalRow2)
    trieBlockBuilder.add(genericInternalRow3)
    trieBlockBuilder.add(genericInternalRow4)
    trieBlockBuilder.add(unsafeInternalRow)
    testTrieBlock = trieBlockBuilder.build()

//    print("abc")
//    print("efg")
  }

  test("test some basic functions") {

    print("This is a test aaa")

    // test size
    assert(testColumnarBlock.size() == 5)
    assert(testTrieBlock.size() == 5)

    // test isEmpty
    assert(!testColumnarBlock.isEmpty() && !testTrieBlock.isEmpty())

    // test nonEmpty
    assert(testColumnarBlock.nonEmpty() && testTrieBlock.nonEmpty())

    // test schema
    assert(testColumnarBlock.schema() == schema && testTrieBlock.schema() == schema)

//    // test contains
//    for (row <- addedRows) {
//      assert(testColumnarBlock.contains(row))
//      assert(testTrieBlock.contains(row))
//    }

    // test getDictionaryOrder
//    assertThrows[Exception] {
      testColumnarBlock.getDictionaryOrder == Option(dictionaryOrder)
//    }
//    assertThrows[Exception] {
//      testTrieBlock.getDictionaryOrder == Option(dictionaryOrder)
//    }
  }

  test("test get") {
    val addr = Utils.allocateMemory(16)
    for (i <- 0 until 100){
      Utils._UNSAFE.putInt(addr + 4, i)
      Utils._UNSAFE.putInt(addr + 12, ( i - 1 ) * 2)
      val iLong = Utils._UNSAFE.getLong(addr)
      println(s"i: ${i}, iLong: ${iLong}, iLong / 2^32 : ${iLong / math.pow(2, 32)} iLong >> 32 : ${iLong >> 32}")
    }
  }

  test("test toArray") {
    val columnarRowArray = testColumnarBlock.toArray()
    println(columnarRowArray.mkString("\n"))
    val trieRowArray = testTrieBlock.toArray()
    println(trieRowArray.mkString("\n"))
    for (row <- addedRows) {
      assert(columnarRowArray.contains(row))
      assert(trieRowArray.contains(row))
    }
  }

  test("test iterator") {
    val mapIterator = testColumnarBlock.iterator
    val trieIterator = testTrieBlock.iterator
    val mapRows = new Array[InternalRow](testColumnarBlock.size().toInt)
    val setRows = new Array[InternalRow](testTrieBlock.size().toInt)
    for (index <- 0 until testColumnarBlock.size().toInt) {
      mapRows(index) = mapIterator.next()
      setRows(index) = trieIterator.next()
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
      val mapSetMergedBlock = testColumnarBlock.merge(testTrieBlock)
    }
    assertThrows[Exception]{
      val mapSetMergedBlock = testTrieBlock.merge(testTrieBlock)
    }

    // if not sort, merge normally
    val mapSetMergedBlock = testColumnarBlock.merge(testTrieBlock,maintainSortOrder = false)
    val setMapMergedBlock = testTrieBlock.merge(testColumnarBlock,maintainSortOrder = false)
    assert(mapSetMergedBlock.size()==5)
    assert(setMapMergedBlock.size()==5)

  }
}