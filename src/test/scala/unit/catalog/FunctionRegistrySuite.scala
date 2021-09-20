package unit.catalog

import org.apache.spark.secco.catalog.FunctionRegistry
import org.apache.spark.secco.expression.{Add, Expression, Literal}
import util.{SeccoFunSuite, UnitTestTag}

class FunctionRegistrySuite extends SeccoFunSuite {

  test("basic", UnitTestTag) {

    // --------------------------
    // built in function registry
    // --------------------------
    val functionRegistry = FunctionRegistry.newBuiltin

    // --------------------------
    // look up
    // --------------------------
    val funcNames = functionRegistry.listFunction()
    val builtInFunctionNames = List(
      "!",
      "%",
      "*",
      "+",
      "-",
      "/",
      "<",
      "<=",
      "=",
      "==",
      ">",
      ">=",
      "and",
      "average",
      "count",
      "in",
      "max",
      "min",
      "negative",
      "not",
      "or",
      "positive",
      "sum"
    )
    assert(funcNames.toSet == builtInFunctionNames.toSet)

    val addExprFuncBuilder = functionRegistry.lookUpFunctionBuilder("+").get
    val addExpr = addExprFuncBuilder(Literal(1) :: Literal(1) :: Nil)
    assert(addExpr.toString == "(1 + 1)")
    assert(addExpr.sql == "(1 + 1)")

    // --------------------------
    // register
    // --------------------------
    functionRegistry.registerFunction(
      "test",
      { e: Seq[Expression] => Add(Literal(1), Literal(1)) }
    )
    assert(functionRegistry.lookUpFunctionBuilder("test").isDefined)

    // --------------------------
    // drop
    // --------------------------
    functionRegistry.dropFunction("test")
    assert(functionRegistry.lookUpFunctionBuilder("test").isEmpty)

    functionRegistry.clear()
    assert(functionRegistry.listFunction().isEmpty)
  }

}
