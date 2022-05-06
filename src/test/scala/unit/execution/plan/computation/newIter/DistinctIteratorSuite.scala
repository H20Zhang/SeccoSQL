package unit.execution.plan.computation.newIter

import org.apache.spark.secco.execution.plan.computation.newIter._
import org.apache.spark.secco.execution.storage.block._
import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.codegen.GenerateOrdering
import org.apache.spark.secco.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.util.Comparator

class DistinctIteratorSuite extends FunSuite with BeforeAndAfter{

  var schema: Seq[Attribute] = _
  var childrenSchemas: Seq[Seq[Attribute]] = _
  var blocks: Array[InternalBlock] = _
  //  var tableIter: TableIterator = _

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

  before {
    val names = Seq("name", "id", "price", "gender", "weight")
    val types = Seq(StringType, IntegerType, DoubleType, BooleanType, FloatType)

    schema = names.zip(types).map {
      case (name, dataType) => AttributeReference(name, dataType)().asInstanceOf[Attribute]
    }

    val childrenSchemaIndices: Seq[Seq[Int]] = Seq(
      names.indices,
      Seq(1, 2, 4),
      Seq(0, 2),
      Seq(1, 3),
      Seq(0, 1, 2, 4),
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

    val child0 = InternalBlock(Array(
      InternalRow(Array[Any]("jacket", 0, 2.5, false, 14.3f)),
      InternalRow(Array[Any]("socks", 0, 5.5, false, 30.6f)),
      InternalRow(Array[Any]("socks", 0, 5.5, false, 30.6f)),
      InternalRow(Array[Any]("sportswear", 0, 5.5, false, 32.3f)),
      InternalRow(Array[Any]("sportswear", 0, 5.5, false, 32.3f)),
      InternalRow(Array[Any]("shoes", 2, 8.9, true, 6.5f))
    ), childrenStructTypes.head)

    val child1 = InternalBlock(Array(
      InternalRow(Array[Any](0, 2.5, 14.3f)),
      InternalRow(Array[Any](0, 5.5, 30.6f)),
      InternalRow(Array[Any](0, 5.5, 32.3f)),
      InternalRow(Array[Any](0, 5.5, 32.3f)),
      InternalRow(Array[Any](0, 5.5, 32.3f)),
      InternalRow(Array[Any](0, 5.5, 32.3f)),
      InternalRow(Array[Any](2, 8.9, 6.5f)),
      InternalRow(Array[Any](2, 8.9, 6.5f))
    ), childrenStructTypes(1))

    val child2 = InternalBlock(Array(
      InternalRow(Array[Any]("shoes", 2.5)),
      InternalRow(Array[Any]("shoes", 2.5)),
      InternalRow(Array[Any]("shirts", 5.5)),
      InternalRow(Array[Any]("shirts", 5.5)),
      InternalRow(Array[Any]("shirts", 5.5)),
      InternalRow(Array[Any]("shirts", 5.5)),
      InternalRow(Array[Any]("shirts", 6.5)),
      InternalRow(Array[Any]("trousers", 8.9)),
      InternalRow(Array[Any]("trousers", 8.9))
    ), childrenStructTypes(2))

    val child3 = InternalBlock(Array(
      InternalRow(Array[Any](1, true)),
      InternalRow(Array[Any](1, true)),
      InternalRow(Array[Any](1, false)),
      InternalRow(Array[Any](1, false)),
      InternalRow(Array[Any](2, true)),
      InternalRow(Array[Any](2, false)),
      InternalRow(Array[Any](2, false)),
      InternalRow(Array[Any](3, true))
    ), childrenStructTypes(3))

    val child4 = InternalBlock(Array(
      InternalRow(Array[Any]("T-shirts", 4, 4.5, 7.3f)),
      InternalRow(Array[Any]("T-shirts", 4, 4.5, 7.3f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.3f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.3f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.6f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.6f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.5f)),
      InternalRow(Array[Any]("socks", 6, 8.5, 4.3f)),
      InternalRow(Array[Any]("trousers", 2, 8.9, 6.5f)),
      InternalRow(Array[Any]("trousers", 2, 8.9, 6.5f))
    ), childrenStructTypes(4))

    val child5 = InternalBlock(Array(
      InternalRow(Array[Any]("trousers", 8.9, false)),
      InternalRow(Array[Any]("shorts", 7.2, true)),
      InternalRow(Array[Any]("shorts", 8.8, true)),
      InternalRow(Array[Any]("shorts", 8.8, true)),
      InternalRow(Array[Any]("sportswear", 6.5, false)),
      InternalRow(Array[Any]("sportswear", 6.5, false))
    ), childrenStructTypes(5))

    val child6 = InternalBlock(Array(
      InternalRow(Array[Any](8.9, false)),
      InternalRow(Array[Any](8.9, false)),
      InternalRow(Array[Any](8.9, true)),
      InternalRow(Array[Any](8.9, true)),
      InternalRow(Array[Any](7.2, true)),
      InternalRow(Array[Any](7.2, true)),
      InternalRow(Array[Any](8.8, true)),
      InternalRow(Array[Any](6.5, false))
    ), childrenStructTypes(6))

    blocks = Array(child0, child1, child2, child3, child4, child5, child6)

    //    val prefixLength = 4 // prefixLength [0, 5)
    //    prefixAndCurAttributes = schema.slice(0, prefixLength + 1)
    //    prefixRow = InternalRow(Array[Any]("trousers", 2, 8.9, true, 6.5f).slice(0, prefixLength + 1))
  }

  var condition: Expression = _
  var distinctIter: DistinctIterator = _
  def showNextAndResults(iter: DistinctIterator): Unit = {
    while(iter.hasNext){
      println("distinctIter.next(): " + iter.next())
    }
    println("distinctIter.results(): " + iter.results())
    println()
  }

  def getChildBlock(childIdx: Int): InternalBlock = {
    val childSchema = childrenSchemas(childIdx).toArray
    val rowArray = blocks(childIdx) toArray()
    sortRowArray(childSchema, rowArray)
    println(rowArray.mkString("Array[ ", ",\n", "]"))
    InternalBlock(rowArray, StructType.fromAttributes(childSchema))
  }

  test("basic_functions_1"){

    for (childIdx <- 0 until 7){
      val childBlock = getChildBlock(childIdx)
      val tableIter = TableIterator(childBlock, childrenSchemas(childIdx).toArray, isSorted = true)
      distinctIter = DistinctIterator(tableIter)
      assert(distinctIter.isSorted())
      showNextAndResults(distinctIter)
      assert(!distinctIter.hasNext)
    }

  }

  test("basic_functions_2"){

    for (childIdx <- 0 until 6){
      val childSchema = childrenSchemas(childIdx).toArray
      val rowArray = blocks(childIdx).toArray()
      println(rowArray.mkString("Array[ ", ",\n", "]"))
      val childBlock = InternalBlock(rowArray, StructType.fromAttributes(childSchema))
      val tableIter = TableIterator(childBlock, childSchema, isSorted = false)
      distinctIter = DistinctIterator(tableIter)
      assert(distinctIter.isBreakPoint())
      assertThrows[NoSuchMethodException](distinctIter.hasNext)
      println(s"$childIdx results:(): " + distinctIter.results())
    }

  }

}
