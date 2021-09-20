package org.apache.spark.dolphin.playground

import org.apache.spark.dolphin.codegen.{CodeGenerator, CodegenContext}
import org.apache.spark.dolphin.expression._
import org.apache.spark.dolphin.types.DataTypes
import org.scalatest.FunSuite

class CodeGeneratorPlayGround extends FunSuite {

  test("CodegenContext") {
    val ctx = new CodegenContext

    println(s"===Operations on CodegenContext===")
    println(ctx.freshName("name"))
    println(ctx.freshVariable("str", DataTypes.StringType))
    println(ctx.addReferenceObj("value", "123213"))
    println(
      ctx.addBufferedState(
        DataTypes.StringType,
        ctx.freshName("str"),
        ctx.INPUT_ROW
      )
    )
    println(
      ctx.addImmutableStateIfNotExists(
        CodeGenerator.javaType(DataTypes.StringType),
        ctx.freshName("str"),
        { str => s"$str =  '1' " }
      )
    )

    println(s"===CodegenContext Internal States===")
    println(ctx.currentVars)
    println(ctx.references)
    println(ctx.inlinedMutableStates)
    println(ctx.mutableStateInitCode)

    println(s"===Gen Code for States===")
    println(ctx.declareMutableStates())
    println(ctx.declareAddedFunctions())
    println(ctx.initMutableStates())
    println(ctx.initPartition())

  }

  test("basic") {
    val notExpr = Not(Literal(false, DataTypes.BooleanType))
    val literalExpr = Literal("1312424", DataTypes.StringType)

    val ctx = new CodegenContext
    println(literalExpr.genCode(ctx))
  }

}
