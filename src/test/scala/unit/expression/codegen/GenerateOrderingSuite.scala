package unit.expression.codegen

//import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.secco.codegen.CodegenContext
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression
import org.apache.spark.secco.expression.codegen.{GenerateOrdering, GeneratePredicate}
import org.apache.spark.secco.expression.{Ascending, AttributeReference, BindReferences, BoundReference, SortOrder}
import org.apache.spark.secco.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import scala.util.Random

class GenerateOrderingSuite extends FunSuite {

  test("generate_ordering") {

    val names = Seq("id", "price", "gender", "weight")
    val types = Seq(IntegerType, DoubleType, BooleanType, DoubleType)
//    val schema = StructType(names.zip(types).map( t => StructField(t._1, t._2)))

    val ordering = types.zipWithIndex.map {
      case(dt, index) => SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }

    GenerateOrdering.generate(ordering)

  }

}
