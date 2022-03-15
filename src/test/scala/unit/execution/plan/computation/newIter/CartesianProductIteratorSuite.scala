package unit.execution.plan.computation.newIter

import org.apache.spark.secco.execution.plan.computation.newIter._
import org.apache.spark.secco.execution.storage.block._
import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.codegen._
import org.apache.spark.secco.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.util.Comparator

class CartesianProductIteratorSuite  extends FunSuite with BeforeAndAfter{

  var schema: Seq[Attribute] = _
  var childrenSchemas: Seq[Seq[Attribute]] = _
  var blocks_left: Array[InternalBlock] = _
  var blocks_right: Array[InternalBlock] = _
  //  var tableIter: TableIterator = _

  before {
    val names = Seq("name", "id", "price", "gender", "weight")
    val types = Seq(StringType, IntegerType, DoubleType, BooleanType, FloatType)

    schema = names.zip(types).map {
      case (name, dataType) => AttributeReference(name, dataType)().asInstanceOf[Attribute]
    }

    val childrenSchemaIndices: Seq[Seq[Int]] = Seq(
      names.indices,
      Seq(0, 2, 3),
      Seq(2, 3)
    )

    childrenSchemas = childrenSchemaIndices.map(_.map {
      idx => schema(idx)
    })

    val childrenStructTypes: Seq[StructType] = childrenSchemas.map {
      attrSeq =>
        StructType(attrSeq.map(attr => StructField(attr.name, attr.dataType)))
    }

    val leftChild0 = InternalBlock(Array(
      InternalRow(Array[Any]("jacket", 0, 2.5, false, 14.3f)),
      InternalRow(Array[Any]("socks", 0, 5.5, false, 30.6f)),
      InternalRow(Array[Any]("sportswear", 0, 5.5, false, 32.3f)),
      InternalRow(Array[Any]("shoes", 2, 8.9, true, 6.5f))
    ), childrenStructTypes.head)

    val rightChild0 = InternalBlock(Array(
      InternalRow(Array[Any]("jacket", 0, 2.5, false, 14.3f)),
      InternalRow(Array[Any]("socks", 0, 5.9, false, 30.6f)),
      InternalRow(Array[Any]("sportswear", 0, 5.5, true, 32.3f))
    ), childrenStructTypes.head)

    val leftChild1 = InternalBlock(Array(
      InternalRow(Array[Any]("trousers", 8.9, false)),
      InternalRow(Array[Any]("trousers", 8.9, true)),
      InternalRow(Array[Any]("shorts", 7.2, true)),
      InternalRow(Array[Any]("shorts", 8.8, true)),
      InternalRow(Array[Any]("sportswear", 6.5, false))
    ), childrenStructTypes(1))

    val rightChild1 = InternalBlock(Array(
      InternalRow(Array[Any]("trousers", 8.9, false)),
      InternalRow(Array[Any]("trousers", 0.9, false)),
      InternalRow(Array[Any]("apple", 7.2, true)),
      InternalRow(Array[Any]("shorts", 8.1, true)),
      InternalRow(Array[Any]("dance_dress", 16.5, false)),
      InternalRow(Array[Any]("dance_dress", 13.5, false))
    ), childrenStructTypes(1))

    val leftChild2 = InternalBlock(Array(
      InternalRow(Array[Any](8.9, false)),
      InternalRow(Array[Any](8.9, true)),
      InternalRow(Array[Any](7.2, true)),
      InternalRow(Array[Any](8.8, true)),
      InternalRow(Array[Any](6.5, false))
    ), childrenStructTypes(2))

    val rightChild2 = InternalBlock(Array(
      InternalRow(Array[Any](-8.9, false)),
      InternalRow(Array[Any](3.2, true)),
      InternalRow(Array[Any](8.8, false)),
      InternalRow(Array[Any](6.5, false))
    ), childrenStructTypes(2))

    blocks_left = Array(leftChild0, leftChild1, leftChild2)
    blocks_right = Array(rightChild0, rightChild1, rightChild2)

    //    val prefixLength = 4 // prefixLength [0, 5)
    //    prefixAndCurAttributes = schema.slice(0, prefixLength + 1)
    //    prefixRow = InternalRow(Array[Any]("trousers", 2, 8.9, true, 6.5f).slice(0, prefixLength + 1))
  }

  var condition: Expression = _
  var cartesianIter: CartesianProductIterator = _
  def showNextAndResults(iter: CartesianProductIterator): Unit = {
    var count = 0
    while(iter.hasNext){
      println(s"$count - cartesianIter.next(): " + iter.next().copy())
      count += 1
    }
    println("cartesianIter.results(): " + iter.results())
    println()
  }

  test("basic_functions_not_sorted"){
    var childIdx = 0
    var leftChildBlock = blocks_left(childIdx)
    var rightChildBlock = blocks_right(childIdx)
    var childSchema = childrenSchemas(childIdx).toArray
    var leftTableIter = TableIterator(leftChildBlock, childSchema, isSorted = false)
    var rightTableIter = TableIterator(rightChildBlock, childSchema, isSorted = false)
    cartesianIter = CartesianProductIterator(leftTableIter, rightTableIter)
    showNextAndResults(cartesianIter)
    assert(!cartesianIter.hasNext)

    childIdx = 1
    leftChildBlock = blocks_left(childIdx)
    rightChildBlock = blocks_right(childIdx)
    childSchema = childrenSchemas(childIdx).toArray
    leftTableIter = TableIterator(leftChildBlock, childSchema, isSorted = false)
    rightTableIter = TableIterator(rightChildBlock, childSchema, isSorted = false)
    cartesianIter = CartesianProductIterator(leftTableIter, rightTableIter)
    showNextAndResults(cartesianIter)
    assert(!cartesianIter.hasNext)

    childIdx = 2
    leftChildBlock = blocks_left(childIdx)
    rightChildBlock = blocks_right(childIdx)
    childSchema = childrenSchemas(childIdx).toArray
    leftTableIter = TableIterator(leftChildBlock, childSchema, isSorted = false)
    rightTableIter = TableIterator(rightChildBlock, childSchema, isSorted = false)
    cartesianIter = CartesianProductIterator(leftTableIter, rightTableIter)
    showNextAndResults(cartesianIter)
    assert(!cartesianIter.hasNext)

  }

  test("basic_functions_sorted"){

    var childIdx = 0
    var leftChildBlock = blocks_left(childIdx)
    var rightChildBlock = blocks_right(childIdx)
    var childSchema = childrenSchemas(childIdx).toArray
    var rowArray = leftChildBlock.toArray() ++ rightChildBlock.toArray()
    sortRowArray(childSchema, rowArray)
    println(rowArray.mkString("Array[ ", ",\n", "]"))
    var leftTableIter = TableIterator(leftChildBlock, childSchema, isSorted = true)
    var rightTableIter = TableIterator(rightChildBlock, childSchema, isSorted = true)
    cartesianIter = CartesianProductIterator(leftTableIter, rightTableIter)
    assert(cartesianIter.isSorted())
    showNextAndResults(cartesianIter)
    assert(!cartesianIter.hasNext)

    childIdx = 1
    leftChildBlock = blocks_left(childIdx)
    rightChildBlock = blocks_right(childIdx)
    childSchema = childrenSchemas(childIdx).toArray
    rowArray = leftChildBlock.toArray() ++ rightChildBlock.toArray()
    sortRowArray(childSchema, rowArray)
    println(rowArray.mkString("Array[ ", ",\n", "]"))
    leftTableIter = TableIterator(leftChildBlock, childSchema, isSorted = true)
    rightTableIter = TableIterator(rightChildBlock, childSchema, isSorted = true)
    cartesianIter = CartesianProductIterator(leftTableIter, rightTableIter)
    assert(cartesianIter.isSorted())
    showNextAndResults(cartesianIter)
    assert(!cartesianIter.hasNext)

    childIdx = 2
    leftChildBlock = blocks_left(childIdx)
    rightChildBlock = blocks_right(childIdx)
    childSchema = childrenSchemas(childIdx).toArray
    rowArray = leftChildBlock.toArray() ++ rightChildBlock.toArray()
    sortRowArray(childSchema, rowArray)
    println(rowArray.mkString("Array[ ", ",\n", "]"))
    leftTableIter = TableIterator(leftChildBlock, childSchema, isSorted = true)
    rightTableIter = TableIterator(rightChildBlock, childSchema, isSorted = true)
    cartesianIter = CartesianProductIterator(leftTableIter, rightTableIter)
    assert(cartesianIter.isSorted())
    showNextAndResults(cartesianIter)
    assert(!cartesianIter.hasNext)

  }

  test("test_basic_functions_sorted_emptySide(s)"){
    var childIdx = 2
    var leftChildBlock = blocks_left(childIdx)
    var childSchema = childrenSchemas(childIdx).toArray
    var rightChildBlock = InternalBlock(Array[InternalRow](), StructType.fromAttributes(childSchema))
    var rowArray = leftChildBlock.toArray() ++ rightChildBlock.toArray()
    sortRowArray(childSchema, rowArray)
    println(rowArray.mkString("Array[ ", ",\n", "]"))
    var leftTableIter = TableIterator(leftChildBlock, childSchema, isSorted = true)
    var rightTableIter = TableIterator(rightChildBlock, childSchema, isSorted = true)
    cartesianIter = CartesianProductIterator(leftTableIter, rightTableIter)
    assert(cartesianIter.isSorted())
    showNextAndResults(cartesianIter)
    assert(!cartesianIter.hasNext)

    childIdx = 1
    rightChildBlock = blocks_right(childIdx)
    childSchema = childrenSchemas(childIdx).toArray
    leftChildBlock = InternalBlock(Array[InternalRow](), StructType.fromAttributes(childSchema))
    rowArray = leftChildBlock.toArray() ++ rightChildBlock.toArray()
    sortRowArray(childSchema, rowArray)
    println(rowArray.mkString("Array[ ", ",\n", "]"))
    leftTableIter = TableIterator(leftChildBlock, childSchema, isSorted = true)
    rightTableIter = TableIterator(rightChildBlock, childSchema, isSorted = true)
    cartesianIter = CartesianProductIterator(leftTableIter, rightTableIter)
    assert(cartesianIter.isSorted())
    showNextAndResults(cartesianIter)
    assert(!cartesianIter.hasNext)

    childIdx = 0
    childSchema = childrenSchemas(childIdx).toArray
    leftChildBlock = InternalBlock(Array[InternalRow](), StructType.fromAttributes(childSchema))
    rightChildBlock = InternalBlock(Array[InternalRow](), StructType.fromAttributes(childSchema))
    rowArray = leftChildBlock.toArray() ++ rightChildBlock.toArray()
    sortRowArray(childSchema, rowArray)
    println(rowArray.mkString("Array[ ", ",\n", "]"))
    leftTableIter = TableIterator(leftChildBlock, childSchema, isSorted = true)
    rightTableIter = TableIterator(rightChildBlock, childSchema, isSorted = true)
    cartesianIter = CartesianProductIterator(leftTableIter, rightTableIter)
    assert(cartesianIter.isSorted())
    showNextAndResults(cartesianIter)
    assert(!cartesianIter.hasNext)
  }

  private def sortRowArray(childSchema: Array[Attribute], rowArray: Array[InternalRow]):Unit = {
    val ordering: Array[SortOrder] = childSchema.zipWithIndex.map {
      case (attr, index) =>
        SortOrder(BoundReference(index, attr.asInstanceOf[AttributeReference].dataType, nullable = true), Ascending)
    }
    val comparator = GenerateOrdering.generate(ordering)
    java.util.Arrays.sort(rowArray,
      new Comparator[InternalRow] {
        override def compare(o1: InternalRow, o2: InternalRow): Int = comparator.compare(o1, o2)
      })
  }
}