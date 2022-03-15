package unit.execution.plan.computation.newIter

import org.apache.spark.secco.execution.plan.computation.newIter.TableIterator
import org.apache.spark.secco.execution.storage.block._
import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.expression._
import org.apache.spark.secco.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable.ArrayBuffer

class TableIteratorSuite extends FunSuite with BeforeAndAfter{

  var schema: Seq[Attribute] = _
  var childrenSchemas: Seq[Seq[Attribute]] = _
  var blocks: Array[InternalBlock] = _
//  var iter: TableIterator = _
  
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
      InternalRow(Array[Any]("sportswear", 0, 5.5, false, 32.3f)),
      InternalRow(Array[Any]("shoes", 2, 8.9, true, 6.5f))
    ), childrenStructTypes.head)
    
    val child1 = InternalBlock(Array(
    InternalRow(Array[Any](0, 2.5, 14.3f)),
    InternalRow(Array[Any](0, 5.5, 30.6f)),
    InternalRow(Array[Any](0, 5.5, 32.3f)),
    InternalRow(Array[Any](2, 8.9, 6.5f))
    ), childrenStructTypes(1))
    
    val child2 = InternalBlock(Array(
      InternalRow(Array[Any]("shoes", 2.5)),
      InternalRow(Array[Any]("shirts", 5.5)),
      InternalRow(Array[Any]("shirts", 6.5)),
      InternalRow(Array[Any]("trousers", 8.9))
    ), childrenStructTypes(2))

    val child3 = InternalBlock(Array(
      InternalRow(Array[Any](1, true)),
      InternalRow(Array[Any](1, false)),
      InternalRow(Array[Any](2, true)),
      InternalRow(Array[Any](2, false)),
      InternalRow(Array[Any](3, true))
    ), childrenStructTypes(3))

    val child4 = InternalBlock(Array(
      InternalRow(Array[Any]("T-shirts", 4, 4.5, 7.3f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.3f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.6f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.5f)),
      InternalRow(Array[Any]("socks", 6, 8.5, 4.3f)),
      InternalRow(Array[Any]("trousers", 2, 8.9, 6.5f))
    ), childrenStructTypes(4))

    val child5 = InternalBlock(Array(
      InternalRow(Array[Any]("trousers", 8.9, false)),
      InternalRow(Array[Any]("trousers", 8.9, true)),
      InternalRow(Array[Any]("shorts", 7.2, true)),
      InternalRow(Array[Any]("shorts", 8.8, true)),
      InternalRow(Array[Any]("sportswear", 6.5, false))
    ), childrenStructTypes(5))

    val child6 = InternalBlock(Array(
      InternalRow(Array[Any](8.9, false)),
      InternalRow(Array[Any](8.9, true)),
      InternalRow(Array[Any](7.2, true)),
      InternalRow(Array[Any](8.8, true)),
      InternalRow(Array[Any](6.5, false))
    ), childrenStructTypes(6))

    blocks = Array(child0, child1, child2, child3, child4, child5, child6)

    //    val prefixLength = 4 // prefixLength [0, 5)
    //    prefixAndCurAttributes = schema.slice(0, prefixLength + 1)
    //    prefixRow = InternalRow(Array[Any]("trousers", 2, 8.9, true, 6.5f).slice(0, prefixLength + 1))
  }
  
  test("basic_functions"){
    val childIdx = 0
    val childBlock = blocks(childIdx)
    val iter = TableIterator(childBlock, childrenSchemas(childIdx).toArray, isSorted = true)
    while (iter.hasNext){
      println("iter.next(): " + iter.next())
    }
    println(iter.results())
//    for (rowIdx <- 0 until  childBlock.size()){
//      assert(iter.next().equals(childBlock(rowIdx)))
//    }
  }

}


// bak code fragments
// var testRows: ArrayBuffer[UnsafeInternalRow] = ArrayBuffer[UnsafeInternalRow]()
//  var testRow: UnsafeInternalRow = _
//  val length = 7
// before {
// //    val theArray = Array("abc", 1, 3, 4.0, true, null)
// testRow = new UnsafeInternalRow(7, true)
// //    testRow.initWithByteArray(new Array[Byte](80), 80)
// //    for (i <- 0 to 6) testRow.setNullAt(i)
// testRow.setString(0, "abc")
// testRow.setInt(1, 1)
// //    testRow.setInt(2,3)
// testRow.setShort(2, 3)
// testRow.setDouble(3, 4.0)
// testRow.setBoolean(4, true)
// testRow.setNullAt(5)
// testRow.setString(6, "variableLength") }
