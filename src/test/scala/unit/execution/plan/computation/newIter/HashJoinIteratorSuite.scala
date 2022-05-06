package unit.execution.plan.computation.newIter

import org.apache.spark.secco.execution.plan.computation.newIter._
import org.apache.spark.secco.execution.storage.block._
import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.codegen._
import org.apache.spark.secco.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.util.Comparator
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HashJoinIteratorSuite extends FunSuite with BeforeAndAfter{

  var schema_left: Seq[Attribute] = _
  var schema_right: Seq[Attribute] = _
  var childrenSchemas_left: Seq[Seq[Attribute]] = _
  var childrenSchemas_right: Seq[Seq[Attribute]] = _
  var blocks_left: Array[InternalBlock] = Array[InternalBlock]()
  var blocks_right: Array[InternalBlock] = Array[InternalBlock]()
//  var rightIndexableIters: Array[IndexableHashMapTableIterator] = Array[IndexableHashMapTableIterator]()
  var rightBuildHashMapIters: Array[BuildHashMap] = Array[BuildHashMap]()
  var conditions: Array[Expression] = Array[Expression]()
  var keysArray: Array[Array[Attribute]] = Array[Array[Attribute]]()

  before {

    val names_left = Seq("name", "id", "price", "gender", "weight")
    val types_left = Seq(StringType, IntegerType, DoubleType, BooleanType, FloatType)
    val childrenSchemaIndices_left: Seq[Seq[Int]] = Seq(
      names_left.indices,
      Seq(0, 2, 3),
      Seq(2, 4)
    )
    val (schema_temp1, childrenSchemas_temp1, childrenStructTypes_left) =
      prepareSchemas(names_left, types_left, childrenSchemaIndices_left)
    schema_left = schema_temp1
    childrenSchemas_left = childrenSchemas_temp1

    val names_right = Seq("tag", "number", "size", "onSale", "price")
    val types_right = Seq(StringType, IntegerType, DoubleType, BooleanType, FloatType)
    val childrenSchemaIndices_right: Seq[Seq[Int]] = Seq(
      names_right.indices,
      Seq(2, 3),
      Seq(2, 3, 4)
    )
    val (schema_temp2, childrenSchemas_temp2, childrenStructTypes_right) =
      prepareSchemas(names_right, types_right, childrenSchemaIndices_right)
    schema_right = schema_temp2
    childrenSchemas_right = childrenSchemas_temp2




    val leftChild0 = InternalBlock(Array(
      InternalRow(Array[Any]("jacket", 0, 2.5, false, 14.3f)),
      InternalRow(Array[Any]("socks", 0, 5.5, false, 30.6f)),
      InternalRow(Array[Any]("sportswear", 0, 5.5, false, 32.3f)),
      InternalRow(Array[Any]("shoes", 2, 8.9, true, 6.5f))
    ), childrenStructTypes_left.head)
    blocks_left = blocks_left :+ leftChild0

    val rightChild0 = InternalBlock(Array(
      InternalRow(Array[Any]("jacket", 0, 2.5, false, 14.3f)),
      InternalRow(Array[Any]("socks", 1, 5.9, false, 30.6f)),
      InternalRow(Array[Any]("sportswear", 0, 5.5, true, 32.3f))
    ), childrenStructTypes_right.head)
    blocks_right = blocks_right :+ rightChild0

    val condition0_0 = EqualTo(childrenSchemas_left.head.head,childrenSchemas_right.head.head)
    val condition0_1 = EqualTo(childrenSchemas_left.head(1),childrenSchemas_right.head(1))
    val condition0 = And(condition0_0, condition0_1)
    conditions = conditions :+ condition0

//    val indexableIter0: IndexableHashMapTableIterator =
//      getIndexableSeccoIterator(rightChild0, condition0, childrenSchemas_right.head)
//    rightBuildHashMapIters = rightBuildHashMapIters :+ indexableIter0
    keysArray = keysArray :+ condition0.children.map(_.children(0).asInstanceOf[Attribute]).toArray
    val rightTableIter0 = TableIterator(rightChild0, childrenSchemas_right.head.toArray, isSorted = false)
    val rightBuildHashMapIter0: BuildHashMap =
      BuildHashMap(rightTableIter0, condition0.children.map(exprs => exprs.children(1).asInstanceOf[Attribute]).toArray)
    rightBuildHashMapIters = rightBuildHashMapIters :+ rightBuildHashMapIter0



    val leftChild1 = InternalBlock(Array(
      InternalRow(Array[Any]("trousers", 8.9, false)),
      InternalRow(Array[Any]("trousers", 8.9, true)),
      InternalRow(Array[Any]("shorts", 7.2, true)),
      InternalRow(Array[Any]("shorts", 8.8, true)),
      InternalRow(Array[Any]("sportswear", 6.5, false))
    ), childrenStructTypes_left(1))
    blocks_left = blocks_left :+ leftChild1

    val rightChild1 = InternalBlock(Array(
      InternalRow(Array[Any](8.9, false)),
      InternalRow(Array[Any](0.9, false)),
      InternalRow(Array[Any](7.2, true)),
      InternalRow(Array[Any](8.1, true)),
      InternalRow(Array[Any](16.5, false)),
      InternalRow(Array[Any](13.5, false))
    ), childrenStructTypes_right(1))
    blocks_right = blocks_right :+ rightChild1

    val condition1_0 = EqualTo(childrenSchemas_left(1)(1),childrenSchemas_right(1).head)
    val condition1_1 = EqualTo(childrenSchemas_left(1)(2),childrenSchemas_right(1)(1))
    val condition1 = And(condition1_0, condition1_1)
    conditions = conditions :+ condition1

//    val indexableIter1: IndexableHashMapTableIterator =
//      getIndexableSeccoIterator(rightChild1, condition1, childrenSchemas_right(1))
//    rightIndexableIters = rightIndexableIters :+ indexableIter1
    keysArray = keysArray :+ condition1.children.map(_.children(0).asInstanceOf[Attribute]).toArray
    val rightTableIter1 = TableIterator(rightChild1, childrenSchemas_right(1).toArray, isSorted = false)
    val rightBuildHashMapIter1: BuildHashMap =
      BuildHashMap(rightTableIter1, condition1.children.map(exprs => exprs.children(1).asInstanceOf[Attribute]).toArray)
    rightBuildHashMapIters = rightBuildHashMapIters :+ rightBuildHashMapIter1



    val leftChild2 = InternalBlock(Array(
      InternalRow(Array[Any](8.9, 6.5f)),
      InternalRow(Array[Any](8.9, 32.3f)),
      InternalRow(Array[Any](7.2, 14.3f)),
      InternalRow(Array[Any](8.8, 30.6f)),
      InternalRow(Array[Any](6.6, 30.6f)),
      InternalRow(Array[Any](6.5, 26.6f))
    ), childrenStructTypes_left(2))
    blocks_left = blocks_left :+ leftChild2

    val rightChild2 = InternalBlock(Array(
      InternalRow(Array[Any](-8.9, false, 6.5f)),
      InternalRow(Array[Any](8.9, true, 32.3f)),
      InternalRow(Array[Any](3.2, true, 14.3f)),
      InternalRow(Array[Any](8.8, false, 30.6f)),
      InternalRow(Array[Any](8.8, false, 30.6f)),
      InternalRow(Array[Any](6.6, false, 24.6f)),
      InternalRow(Array[Any](6.5, false, 26.6f))
    ), childrenStructTypes_right(2))
    blocks_right = blocks_right :+ rightChild2

    val condition2_0 = EqualTo(childrenSchemas_left(2)(1),childrenSchemas_right(2)(2))
    val condition2 = Alias(condition2_0, "wrapper")()
    conditions = conditions :+ condition2

//    val indexableIter2: IndexableHashMapTableIterator =
//      getIndexableSeccoIterator(rightChild2, condition2, childrenSchemas_right(2))
//    rightIndexableIters = rightIndexableIters :+ indexableIter2
    keysArray = keysArray :+ condition2.children.map(_.children(0).asInstanceOf[Attribute]).toArray
    val rightTableIter2 = TableIterator(rightChild2, childrenSchemas_right(2).toArray, isSorted = false)
    val rightBuildHashMapIter2: BuildHashMap =
      BuildHashMap(rightTableIter2, condition2.children.map(exprs => exprs.children(1).asInstanceOf[Attribute]).toArray)
    rightBuildHashMapIters = rightBuildHashMapIters :+ rightBuildHashMapIter2

  }

//  private def getIndexableSeccoIterator(block: InternalBlock, condition: Expression, attributes_right: Seq[Attribute]) = {
//    val leftKeys = condition.children.map(expr => expr.children.head)
//    val rightKeys = condition.children.map(exprs => exprs.children(1))
//    val hashMap: mutable.HashMap[InternalRow, ArrayBuffer[InternalRow]] = getRowHashMap(block, rightKeys, attributes_right)
//    val indexableIter = IndexableHashMapTableIterator(hashMap, attributes_right.toArray)
//    keysArray = keysArray :+ leftKeys.map(_.asInstanceOf[Attribute]).toArray
//    indexableIter
//  }
//
//  private def getRowHashMap(block: InternalBlock,
//                            keys: Seq[Expression],
//                            attributes: Seq[Attribute]): mutable.HashMap[InternalRow, ArrayBuffer[InternalRow]] = {
//    val hashMap = new mutable.HashMap[InternalRow, ArrayBuffer[InternalRow]]
//    val projectFunc0 = GenerateSafeProjection.generate(keys,
//      attributes)
//    for (row <- block.toArray()) {
//      val key = projectFunc0(row)
//      if (hashMap.contains(key))
//        hashMap(key).append(row.copy())
//      else
//        hashMap.put(key.copy(), ArrayBuffer[InternalRow](row.copy()))
//    }
//    println(s"hashMap: $hashMap")
//    hashMap
//  }

  private def prepareSchemas(names: Seq[String],
                             types: Seq[DataType],
                             childrenSchemaIndices: Seq[Seq[Int]]): 
                             (Seq[Attribute], Seq[Seq[Attribute]], Seq[StructType]) = {
    assert(names.length == types.length)
    assert(childrenSchemaIndices.flatten.max < names.length)
    val schema = names.zip(types).map {
      case (name, dataType) => AttributeReference(name, dataType)().asInstanceOf[Attribute]
    }
    val childrenSchemas = childrenSchemaIndices.map(_.map {
      idx => schema(idx)
    })
    val childrenStructTypes: Seq[StructType] = childrenSchemas.map {
      attrSeq =>
        StructType(attrSeq.map(attr => StructField(attr.name, attr.dataType)))
    }
    (schema, childrenSchemas, childrenStructTypes)
  }

  var condition: Expression = _
  var hashJoinIterator: HashJoinIterator = _
  def showNextAndResults(iter: HashJoinIterator): Unit = {
    while(iter.hasNext){
      println("hashJoinIter.next(): " + iter.next().copy())
    }
    println("hashJoinIter.results(): " + iter.results())
    println()
  }

  test("basic_functions_not_sorted"){
    val childIndices = Seq(0, 1, 2)
    for(childIdx <- childIndices) {
      val leftChildBlock = blocks_left(childIdx)
      val leftChildSchema = childrenSchemas_left(childIdx).toArray
      val leftTableIter = TableIterator(leftChildBlock, leftChildSchema, isSorted = false)
//      hashJoinIterator = HashJoinIterator(leftTableIter, rightIndexableIters(childIdx),
      hashJoinIterator = HashJoinIterator(leftTableIter, rightBuildHashMapIters(childIdx),
        keysArray(childIdx), conditions(childIdx))
      showNextAndResults(hashJoinIterator)
      assert(!hashJoinIterator.hasNext)
    }

  }

}