package unit.execution

import org.apache.spark.secco.execution._
import org.apache.spark.secco.execution.plan.computation.deprecated.{
  AggregateExec,
  BlockInputExec
}
import org.apache.spark.secco.execution.plan.computation.{
  PushBasedCodegenExec,
  deprecated
}
import org.apache.spark.secco.execution.plan.computation.newIter._
import org.apache.spark.secco.execution.storage.block._
import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.expression.aggregate._
import org.apache.spark.secco.expression.{
  Attribute,
  AttributeReference,
  Expression,
  NamedExpression
}
import org.apache.spark.secco.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable.ArrayBuffer

class AggregateExecSuite extends FunSuite with BeforeAndAfter {

  var schema: Seq[Attribute] = _
  var childrenSchemas: Seq[Seq[Attribute]] = Seq[Seq[Attribute]]()
  var blocks: Array[InternalBlock] = Array[InternalBlock]()
  var resultIterList: List[Iterator[InternalRow]] = List.empty

  before {
    val names = Seq("name", "id", "price", "gender", "weight")
    val types = Seq(StringType, IntegerType, DoubleType, BooleanType, FloatType)

    schema = names.zip(types).map { case (name, dataType) =>
      AttributeReference(name, dataType)().asInstanceOf[Attribute]
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

    childrenSchemas = childrenSchemaIndices.map(_.map { idx =>
      schema(idx)
    })

    val childrenStructTypes: Seq[StructType] = childrenSchemas.map { attrSeq =>
      StructType(attrSeq.map(attr => StructField(attr.name, attr.dataType)))
    }

    val child0 = InternalBlock(
      Array(
        InternalRow(Array[Any]("jacket", 0, 2.5, false, 14.3f)),
        InternalRow(Array[Any]("socks", 0, 5.5, false, 30.6f)),
        InternalRow(Array[Any]("socks", 0, 5.5, false, 30.6f)),
        InternalRow(Array[Any]("sportswear", 0, 5.5, false, 32.3f)),
        InternalRow(Array[Any]("sportswear", 0, 5.5, false, 32.3f)),
        InternalRow(Array[Any]("shoes", 2, 8.9, true, 6.5f))
      ),
      childrenStructTypes.head
    )

    val child1 = InternalBlock(
      Array(
        InternalRow(Array[Any](0, 2.5, 14.3f)),
        InternalRow(Array[Any](0, 5.5, 30.6f)),
        InternalRow(Array[Any](0, 5.5, 32.3f)),
        InternalRow(Array[Any](0, 5.5, 32.3f)),
        InternalRow(Array[Any](0, 5.5, 32.3f)),
        InternalRow(Array[Any](0, 5.5, 32.3f)),
        InternalRow(Array[Any](2, 8.9, 6.5f)),
        InternalRow(Array[Any](2, 3.9, 6.5f)),
        InternalRow(Array[Any](2, 8.9, 6.5f))
      ),
      childrenStructTypes(1)
    )

    val child2 = InternalBlock(
      Array(
        InternalRow(Array[Any]("shoes", 2.5)),
        InternalRow(Array[Any]("shoes", 2.5)),
        InternalRow(Array[Any]("shirts", 5.5)),
        InternalRow(Array[Any]("shirts", 5.5)),
        InternalRow(Array[Any]("shirts", 5.5)),
        InternalRow(Array[Any]("shirts", 5.5)),
        InternalRow(Array[Any]("shirts", 6.5)),
        InternalRow(Array[Any]("trousers", 8.9)),
        InternalRow(Array[Any]("trousers", 8.9))
      ),
      childrenStructTypes(2)
    )

    val child3 = InternalBlock(
      Array(
        InternalRow(Array[Any](1, true)),
        InternalRow(Array[Any](1, true)),
        InternalRow(Array[Any](1, false)),
        InternalRow(Array[Any](1, false)),
        InternalRow(Array[Any](2, true)),
        InternalRow(Array[Any](2, false)),
        InternalRow(Array[Any](2, false)),
        InternalRow(Array[Any](3, true))
      ),
      childrenStructTypes(3)
    )

    val child4 = InternalBlock(
      Array(
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
      ),
      childrenStructTypes(4)
    )

    val child5 = InternalBlock(
      Array(
        InternalRow(Array[Any]("trousers", 8.9, false)),
        InternalRow(Array[Any]("shorts", 7.2, true)),
        InternalRow(Array[Any]("shorts", 8.8, true)),
        InternalRow(Array[Any]("shorts", 8.8, true)),
        InternalRow(Array[Any]("sportswear", 6.5, false)),
        InternalRow(Array[Any]("sportswear", 6.5, false))
      ),
      childrenStructTypes(5)
    )

    val child6 = InternalBlock(
      Array(
        InternalRow(Array[Any](8.9, false)),
        InternalRow(Array[Any](8.9, false)),
        InternalRow(Array[Any](8.9, true)),
        InternalRow(Array[Any](8.9, true)),
        InternalRow(Array[Any](7.2, true)),
        InternalRow(Array[Any](7.2, true)),
        InternalRow(Array[Any](8.8, true)),
        InternalRow(Array[Any](6.5, false))
      ),
      childrenStructTypes(6)
    )

    blocks = Array(child0, child1, child2, child3, child4, child5, child6)

  }

  def showNextAndResults(iter: AggregateIterator): Unit = {
    while (iter.hasNext) {
      println("aggregateIter.next(): " + iter.next())
    }
    println("aggregateIter.results(): " + iter.results())
    println()
  }

  test("AggregateExec") {
    val childIndices = ArrayBuffer[Int]()
    var groupingFunctionsSeq: Seq[Array[NamedExpression]] =
      Seq[Array[NamedExpression]]()
    var aggregateFunctionsSeq: Seq[Array[AggregateFunction]] =
      Seq[Array[AggregateFunction]]()

    childIndices.append(0)
//    val groupingExpressions0 = Array[NamedExpression](childrenSchemas.head.head)
    val groupingExpressions0 = Array.empty[NamedExpression]
    groupingFunctionsSeq = groupingFunctionsSeq :+ groupingExpressions0
    val aggregateFunctions0 =
      Array[AggregateFunction](Max(childrenSchemas.head(4)))
    aggregateFunctionsSeq = aggregateFunctionsSeq :+ aggregateFunctions0

    childIndices.append(1)
    val groupingExpressions1 = Array[NamedExpression](childrenSchemas(1).head)
    groupingFunctionsSeq = groupingFunctionsSeq :+ groupingExpressions1
    val aggregateFunctions1 =
      Array[AggregateFunction](Min(childrenSchemas(1)(1)))
    aggregateFunctionsSeq = aggregateFunctionsSeq :+ aggregateFunctions1

    childIndices.append(2)
    val groupingExpressions2 = Array[NamedExpression](childrenSchemas(2).head)
    groupingFunctionsSeq = groupingFunctionsSeq :+ groupingExpressions2
    val aggregateFunctions2 =
      Array[AggregateFunction](Count(childrenSchemas(2)(1)))
    aggregateFunctionsSeq = aggregateFunctionsSeq :+ aggregateFunctions2

    childIndices.append(3)
    val groupingExpressions3 = Array[NamedExpression](childrenSchemas(3).head)
    groupingFunctionsSeq = groupingFunctionsSeq :+ groupingExpressions3
    val aggregateFunctions3 =
      Array[AggregateFunction](Average(childrenSchemas(3)(1)))
    aggregateFunctionsSeq = aggregateFunctionsSeq :+ aggregateFunctions3

    childIndices.append(4)
    val groupingExpressions4 = Array[NamedExpression](childrenSchemas(4).head)
    groupingFunctionsSeq = groupingFunctionsSeq :+ groupingExpressions4
    val aggregateFunctions4 =
      Array[AggregateFunction](Average(childrenSchemas(4)(1)))
    aggregateFunctionsSeq = aggregateFunctionsSeq :+ aggregateFunctions4

    childIndices.append(5)
    val groupingExpressions5 = Array[NamedExpression](childrenSchemas(5).head)
    groupingFunctionsSeq = groupingFunctionsSeq :+ groupingExpressions5
    val aggregateFunctions5 =
      Array[AggregateFunction](Average(childrenSchemas(5)(1)))
    aggregateFunctionsSeq = aggregateFunctionsSeq :+ aggregateFunctions5

    childIndices.append(6)
    val groupingExpressions6 = Array[NamedExpression](childrenSchemas(6)(1))
    groupingFunctionsSeq = groupingFunctionsSeq :+ groupingExpressions6
    val aggregateFunctions6 =
      Array[AggregateFunction](Average(childrenSchemas(6).head))
    aggregateFunctionsSeq = aggregateFunctionsSeq :+ aggregateFunctions6

    try {
      for (childIdx <- childIndices) {
        val inputExec =
          BlockInputExec(childrenSchemas(childIdx), blocks(childIdx))
        println("in test, before HashJoinExecExec")
        val aggregateExec = deprecated.AggregateExec(
          groupingFunctionsSeq(childIdx),
          aggregateFunctionsSeq(childIdx),
          inputExec
        )
        println("in test, before PushBasedCodegenExec")
        val pushBasedCodegenExec = PushBasedCodegenExec(aggregateExec)(0)
        println("in test, before executeWithCodeGen()")
        resultIterList =
          resultIterList :+ pushBasedCodegenExec.executeWithCodeGen()
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  after {
    var count = 0
    for (resultIter <- resultIterList) {
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
