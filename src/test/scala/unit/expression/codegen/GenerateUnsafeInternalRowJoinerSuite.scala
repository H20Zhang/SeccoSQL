package unit.expression.codegen

import org.apache.spark.secco.execution.storage.row.UnsafeInternalRow
import org.apache.spark.secco.expression.codegen.GenerateUnsafeInternalRowJoiner
import org.apache.spark.secco.types._
import org.scalatest.FunSuite

import java.lang.Thread.sleep
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class GenerateUnsafeInternalRowJoinerSuite extends FunSuite{

  test("generate_unsafeInternalRowJoiner") {

    val names1 = Seq("id", "price", "forGender", "weight", "comments")
    val types1 = Seq(IntegerType, DoubleType, BooleanType, DoubleType, StringType)
    val schema1 = StructType(names1.zip(types1).map {
      case (name, dt) => StructField(name, dt)
    })

    val names2 = Seq("id", "description", "region", "ratio")
    val types2 = Seq(IntegerType, StringType, StringType, FloatType)
    val schema2 = StructType(names2.zip(types2).map {
      case (name, dt) => StructField(name, dt)
    })

    val joiner = GenerateUnsafeInternalRowJoiner.generate((schema1, schema2))

    val row1 = new UnsafeInternalRow(5, true)
    row1.setInt(0, 1)
    row1.setDouble(1, 3.0)
    row1.setBoolean(2, value = true)
    row1.setDouble(3, 4.0)
    row1.setString(4, "This is a comment.")

    println("row1:")
    for (i <- types1.indices){
      println(row1.get(i, types1(i)))
    }
    println()

    val row2 = new UnsafeInternalRow(4, true)
    row2.setInt(0, 1)
    row2.setString(1, "version 3.0")
    row2.setString(2, "Hong Kong")
    row2.setFloat(3, 4.223f)

    println("row2")
    for (i <- types2.indices){
      println(row2.get(i, types2(i)))
    }
    println()

    val concated_types: Seq[DataType] = types1 ++ types2
    val out_row = joiner.join(row1, row2)
    for (i <- concated_types.indices){
      println(out_row.get(i, concated_types(i)))
    }
  }
}
