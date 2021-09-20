package unit.codegen

import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression
import org.apache.spark.secco.expression.BindReferences
import org.apache.spark.secco.types.IntegerType
import org.scalatest.FunSuite

class GenerateSafeProjectionSuite extends FunSuite {

  test("generate_safe_projection") {
    val A = AttributeReference("a", IntegerType, false)()
    val B = AttributeReference("b", IntegerType, false)()
    val schema = Seq(A, B)

    val multiplyExpr = expression.Multiply(A, B)
    val subtractExpr = expression.Subtract(A, B)
    println(multiplyExpr)

    val boundedExpr1 = BindReferences.bindReference(multiplyExpr, schema)
    val boundedExpr2 = BindReferences.bindReference(subtractExpr, schema)

    val projection =
      GenerateSafeProjection.generate(Seq(boundedExpr1, boundedExpr2))

    val row = InternalRow(10, 5)

    println(projection.apply(row))

    println(boundedExpr1.eval(row))
    println(boundedExpr2.eval(row))

  }
}
