package unit.expression

import org.apache.spark.secco.expression.{Expression, Literal}
import org.apache.spark.secco.expression.{Add, EqualTo, Multiply}
import org.apache.spark.secco.expression.AttributeReference
import org.apache.spark.secco.expression.{Alias, Not}
import org.apache.spark.secco.analysis.{
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedStar
}
import org.apache.spark.secco.types.IntegerType
import util.{SeccoFunSuite, UnitTestTag}

class ExpressionSuite extends SeccoFunSuite {

  test("basic", UnitTestTag) {

    //literal, attribute, arithmetic, comparison
    val literal1 = Literal(1, IntegerType)
    val literal2 = Literal(2, IntegerType)
    val attrA = AttributeReference("A", IntegerType)(qualifier = Some("R1"))
    val attrB = AttributeReference("B", IntegerType)(qualifier = Some("R2"))
    val addExpr = Add(literal1, literal2)
    val multiExpr = Multiply(addExpr, attrA)
    val equalExpr = EqualTo(multiExpr, attrB)
    val notExpr = Not(equalExpr)
    val aliasExpr = Alias(notExpr, "C")()

    println(addExpr)
    println(multiExpr)
    println(equalExpr)
    println(notExpr)
    println(aliasExpr.sql)

    //unresolved
    val unresolvedStar = UnresolvedStar(Some(Seq("R1")))
    val unresolvedAttribute = UnresolvedAttribute(Seq("R1", "A"))
    val unresolvedAlias =
      UnresolvedAlias(Literal(1, IntegerType), Some((x: Expression) => "A"))
    val unresolvedFunc =
      UnresolvedFunction("sum", Seq(unresolvedAttribute), false)

    println(unresolvedStar)
    println(unresolvedAttribute)
    println(unresolvedAlias)
    println(unresolvedFunc)

  }
}
