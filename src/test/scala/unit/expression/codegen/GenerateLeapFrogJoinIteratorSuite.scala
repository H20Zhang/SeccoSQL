package unit.expression.codegen

import org.apache.spark.secco.expression.{Ascending, Attribute, AttributeReference, BoundReference, SortOrder}
import org.apache.spark.secco.expression.codegen.{GenerateLeapFrogJoinIterator, GenerateOrdering}
import org.apache.spark.secco.types.{BooleanType, DoubleType, IntegerType, StructField}
import org.scalatest.FunSuite

class GenerateLeapFrogJoinIteratorSuite extends FunSuite{

  test("generate_ordering") {

    val names = Seq("id", "price", "gender", "weight")
    val types = Seq(IntegerType, DoubleType, BooleanType, DoubleType)
    //    val schema = StructType(names.zip(types).map( t => StructField(t._1, t._2)))

    val ordering = types.zipWithIndex.map {
      case(dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }

    GenerateOrdering.generate(ordering)

  }

  test("generate_leapFrogJoinUnaryIterator"){
    val names = Seq("id", "price", "gender", "weight")
    val types = Seq(IntegerType, DoubleType, BooleanType, DoubleType)

    val prefixAttributes : Seq[Attribute] = names.zip(types).map {
      case (name, dataType) => AttributeReference(name, dataType)().asInstanceOf[Attribute]
    }

    val childrenSchemaIndices: Seq[Seq[Int]] = Seq(
      Seq(1, 2, 3),
      Seq(0, 2),
      Seq(1, 3),
      Seq(0, 1, 2),
      Seq(0, 2, 3),
      Seq(2, 3)
    )

    val childrenSchemas: Seq[Seq[Attribute]] = childrenSchemaIndices.map(_.map {
      idx => prefixAttributes(idx)
    })

    val producer = GenerateLeapFrogJoinIterator.generate((prefixAttributes, childrenSchemas))

//    producer.getIterator()
  }

}
