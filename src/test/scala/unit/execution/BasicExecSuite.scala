package unit.execution

import org.apache.spark.secco.execution._
import org.apache.spark.secco.execution.plan.computation.newIter.TableIterator
import org.apache.spark.secco.execution.storage.block._
import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.expression._
import org.apache.spark.secco.types._
import org.scalatest._


class BasicExecSuite extends FunSuite with BeforeAndAfter {

  var schema: Seq[Attribute] = _
  var childrenSchemas: Seq[Seq[Attribute]] = _
  var blocks: Array[InternalBlock] = _
  var tableIters: Seq[TableIterator] = _
  var inputExecs: Seq[BlockInputExec] = _
  var resultIterList: List[Iterator[InternalRow]] = List.empty

  before {
    val names = Seq("name", "id", "price", "gender", "weight")
    val types = Seq(StringType, IntegerType, DoubleType, BooleanType, FloatType)

    schema = names.zip(types).map {
      case (name, dataType) => AttributeReference(name, dataType)().asInstanceOf[Attribute]
    }

    val childrenSchemaIndices: Seq[Seq[Int]] = Seq(
      Seq(0, 1, 2, 3, 4),
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
      InternalRow(Array[Any]("socks", 0, 5.3, false, 30.6f)),
      InternalRow(Array[Any]("socks", 0, 5.6, false, 30.6f)),
      InternalRow(Array[Any]("book", 1, 8.9, true, 6.5f)),
      InternalRow(Array[Any]("book", 2, 8.9 ,true, 6.5f)),
      InternalRow(Array[Any]("sportswear", 0, 5.5, false, 32.3f)),
      InternalRow(Array[Any]("shoes", 2, 8.9, true, 6.5f))
    ), childrenStructTypes.head)

    val child1 = InternalBlock(Array(
      InternalRow(Array[Any](0, 2.5, 14.3f)),
      InternalRow(Array[Any](0, 5.5, 30.6f)),
      InternalRow(Array[Any](0, 5.6, 30.6f)),
      InternalRow(Array[Any](0, 5.5, 32.3f)),
      InternalRow(Array[Any](2, 8.9, 6.5f))
    ), childrenStructTypes(1))

    val child2 = InternalBlock(Array(
      InternalRow(Array[Any]("shoes", 2.5)),
      InternalRow(Array[Any]("shirts", 5.5)),
      InternalRow(Array[Any]("socks",5.6)),
      InternalRow(Array[Any]("shirts", 6.5)),
      InternalRow(Array[Any]("book", 8.9))
    ), childrenStructTypes(2))

    val child3 = InternalBlock(Array(
      InternalRow(Array[Any](1, true)),
      InternalRow(Array[Any](1, false)),
      InternalRow(Array[Any](0, false)),
      InternalRow(Array[Any](2, true)),
      InternalRow(Array[Any](2, false)),
      InternalRow(Array[Any](3, true))
    ), childrenStructTypes(3))

    val child4 = InternalBlock(Array(
      InternalRow(Array[Any]("T-shirts", 4, 4.5, 7.3f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.3f)),
      InternalRow(Array[Any]("socks", 0, 5.6, 30.6f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.6f)),
      InternalRow(Array[Any]("blouse", 5, 4.5, 7.5f)),
      InternalRow(Array[Any]("socks", 6, 8.5, 4.3f)),
      InternalRow(Array[Any]("book", 2, 8.9, 6.5f))
    ), childrenStructTypes(4))

    val child5 = InternalBlock(Array(
      InternalRow(Array[Any]("book", 8.9, false)),
      InternalRow(Array[Any]("socks", 5.6, false)),
      InternalRow(Array[Any]("book", 8.9, true)),
      InternalRow(Array[Any]("shorts", 7.2, true)),
      InternalRow(Array[Any]("shorts", 8.8, true)),
      InternalRow(Array[Any]("sportswear", 6.5, false))
    ), childrenStructTypes(5))

    val child6 = InternalBlock(Array(
      InternalRow(Array[Any](8.9, false)),
      InternalRow(Array[Any](8.9, true)),
      InternalRow(Array[Any](7.2, true)),
      InternalRow(Array[Any](5.6, false)),
      InternalRow(Array[Any](8.8, true)),
      InternalRow(Array[Any](6.5, false))
    ), childrenStructTypes(6))

    blocks = Array(child0, child1, child2, child3, child4, child5, child6)
    tableIters = blocks.zipWithIndex.flatMap {
//            case (block, 0) => None
      //      case (block, 3) => None
      case (block, idx) => Seq(TableIterator(block, childrenSchemas(idx).toArray, isSorted = false))
//      case (block, idx) if(idx < 4) => Seq(TableIterator(block, childrenSchemas(idx).toArray, isSorted = false))
      case _ => None
    }
    inputExecs = blocks.zipWithIndex.flatMap {
//            case (block, 0) => None
      //      case (block, 3) => None
      case (block, idx) => Seq(BlockInputExec(childrenSchemas(idx), block))
//      case (block, idx) if(idx < 4) => Seq(BlockInputExec(childrenSchemas(idx), block))
      case _ => None
    }

    println("the begin part finished! ")
  }

  test("BlockInputExec"){
    val inputExec = BlockInputExec(childrenSchemas.head, blocks.head)
    val pushBasedCodegenExec = PushBasedCodegenExec(inputExec)(0)
    resultIterList = resultIterList :+ pushBasedCodegenExec.executeWithCodeGen()
  }

  test("LeapFrogJoinExec"){
    val leapFrogJoinExec = LeapFrogJoinExec(inputExecs, schema.toArray)
    val pushBasedCodegenExec = PushBasedCodegenExec(leapFrogJoinExec)(0)
    println("in test, before executeWithCodeGen()")
    resultIterList = resultIterList :+ pushBasedCodegenExec.executeWithCodeGen()
  }

  test("FilterExec"){
    for(intVal <- Seq(1,2)) {
      val condition = EqualTo(childrenSchemas.head(1), Literal(intVal))
      val inputExec = BlockInputExec(childrenSchemas.head, blocks.head)
      val filterExec = FilterExec(condition: Expression, inputExec)
      val pushBasedCodegenExec = PushBasedCodegenExec(filterExec)(0)
      resultIterList = resultIterList :+ pushBasedCodegenExec.executeWithCodeGen()
    }
  }

  test("ProjectExec"){
    val projectSeq = (childrenSchemas.head(0) :: Nil) :+ childrenSchemas.head(2)
    val inputExec = BlockInputExec(childrenSchemas.head, blocks.head)
    val projectExec = ProjectExec(projectSeq, inputExec)
    val pushBasedCodegenExec = PushBasedCodegenExec(projectExec)(0)
    resultIterList = resultIterList :+ pushBasedCodegenExec.executeWithCodeGen()
  }

  test("HashJoinExec"){
    val leftInput = BlockInputExec(childrenSchemas(2), blocks(2))
    val rightInput = BlockInputExec(childrenSchemas(1), blocks(1))
    val leftKeys = Seq(childrenSchemas(2)(1))
    val rightKeys = Seq(childrenSchemas(1)(1))
    println("in test, before HashJoinExecExec")
    val hashJoinExec = HashJoinExec(leftInput, rightInput, leftKeys, rightKeys, None)
    println("in test, before PushBasedCodegenExec")
    val pushBasedCodegenExec = PushBasedCodegenExec(hashJoinExec)(0)
    println("in test, before executeWithCodeGen()")
    resultIterList = resultIterList :+ pushBasedCodegenExec.executeWithCodeGen()
  }

  after{
    var count = 0
    for(resultIter <- resultIterList) {
      println(s"$count:")
      if (!resultIter.hasNext) {
        println("resultIter is empty!!!")
      }
      while (resultIter.hasNext) {
        println("iter.next(): " + resultIter.next())
      }
      count += 1
    }
  }
}
