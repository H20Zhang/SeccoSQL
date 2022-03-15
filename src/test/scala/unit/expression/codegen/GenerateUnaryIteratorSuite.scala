package unit.expression.codegen

import org.apache.spark.secco.execution.storage.block.{TrieInternalBlock, TrieInternalBlockBuilder}
import org.apache.spark.secco.execution.storage.row.{GenericInternalRow, InternalRow}
import org.apache.spark.secco.expression.{Ascending, Attribute, AttributeReference, BoundReference, SortOrder}
import org.apache.spark.secco.expression.codegen.{GenerateUnaryIterator, GenerateOrdering}
import org.apache.spark.secco.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

class GenerateUnaryIteratorSuite extends FunSuite with BeforeAndAfter {

  var schema: Seq[Attribute] = _
  var prefixAndCurAttributes: Seq[Attribute] = _
  var childrenSchemas: Seq[Seq[Attribute]] = _
  var tries: Array[TrieInternalBlock] = _
  var prefixRow: InternalRow = _

  before {
    val names = Seq("name", "id", "price", "gender", "weight")
    val types = Seq(StringType, IntegerType, DoubleType, BooleanType, FloatType)

    schema = names.zip(types).map {
      case (name, dataType) => AttributeReference(name, dataType)().asInstanceOf[Attribute]
    }

    val childrenSchemaIndices: Seq[Seq[Int]] = Seq(
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


    val builder0 = new TrieInternalBlockBuilder(childrenStructTypes.head)
    builder0.add(InternalRow(Array[Any](0, 2.5, 14.3f)))
    builder0.add(InternalRow(Array[Any](0, 5.5, 30.6f)))
    builder0.add(InternalRow(Array[Any](0, 5.5, 32.3f)))
    builder0.add(InternalRow(Array[Any](2, 8.9, 6.5f)))
    val child0 = builder0.build()

    val child1 = TrieInternalBlock(Array(
      InternalRow(Array[Any]("shoes", 2.5)),
      InternalRow(Array[Any]("shirts", 5.5)),
      InternalRow(Array[Any]("shirts", 6.5)),
      InternalRow(Array[Any]("trousers", 8.9))
    ), childrenStructTypes(1))

    val child2 = TrieInternalBlock(Array(
      InternalRow(Array[Any](1, true)),
      InternalRow(Array[Any](1, false)),
      InternalRow(Array[Any](2, true)),
      InternalRow(Array[Any](2, false)),
      InternalRow(Array[Any](3, true))
    ), childrenStructTypes(2))

    val child3 = TrieInternalBlock(Array(
      InternalRow(Array[Any]("T-shirts", 4, 4.5, 7.3f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.3f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.6f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.5f)),
      InternalRow(Array[Any]("socks", 6, 8.5, 4.3f)),
      InternalRow(Array[Any]("trousers", 2, 8.9, 6.5f))
    ), childrenStructTypes(3))

    val child4 = TrieInternalBlock(Array(
      InternalRow(Array[Any]("trousers", 8.9, false)),
      InternalRow(Array[Any]("trousers", 8.9, true)),
      InternalRow(Array[Any]("shorts", 7.2, true)),
      InternalRow(Array[Any]("shorts", 8.8, true)),
      InternalRow(Array[Any]("sportswear", 6.5, false))
    ), childrenStructTypes(4))

    val child5 = TrieInternalBlock(Array(
      InternalRow(Array[Any](8.9, false)),
      InternalRow(Array[Any](8.9, true)),
      InternalRow(Array[Any](7.2, true)),
      InternalRow(Array[Any](8.8, true)),
      InternalRow(Array[Any](6.5, false))
    ), childrenStructTypes(5))

    tries = Array(child0, child1, child2, child3, child4, child5)

    val prefixLength = 4 // prefixLength [0, 5)
    prefixAndCurAttributes = schema.slice(0, prefixLength + 1)
    prefixRow = InternalRow(Array[Any]("trousers", 2, 8.9, true, 6.5f).slice(0, prefixLength + 1))
  }

  test("generate_UnaryIterator"){

    val producer = GenerateUnaryIterator.generate((prefixAndCurAttributes, childrenSchemas))

    val iter = producer.getIterator(prefixRow, tries)

    val hasNext1 = iter.hasNext
    println(s"hasNext1: $hasNext1")
    if (iter.hasNext){
      val first = iter.next()
      println(s"first: $first")
      //    assert(first.equals(false))
    }
    val hasNext2 = iter.hasNext
    println(s"hasNext2: $hasNext2")
    if (iter.hasNext){
      val second = iter.next()
      println(s"second: $second")
      //    assert(second.equals(true))
    }
  }

}
