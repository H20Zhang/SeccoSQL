package org.apache.spark.dolphin.expression.codegen

import org.apache.spark.dolphin.codegen.CodegenContext
import org.apache.spark.dolphin.execution.storage.row.InternalRow
import org.apache.spark.dolphin.expression
import org.apache.spark.dolphin.expression.{AttributeReference, BindReferences}
import org.apache.spark.dolphin.types.IntegerType
import org.netlib.lapack.Sstev
import org.scalatest.FunSuite

import scala.util.Random

class GeneratePredicateSuite extends FunSuite {

  test("generate_predicates") {
    val A = AttributeReference("a", IntegerType, false)()
    val B = AttributeReference("b", IntegerType, false)()
    val schema = Seq(A, B)

    val eqExpr = expression.EqualTo(A, B)
    println(eqExpr)

    val ltExpr = expression.LessThan(A, B)
    println(ltExpr)

    val gtExpr = expression.GreaterThan(A, B)
    println(gtExpr)

    val lteqExpr = expression.LessThanOrEqual(A, B)
    println(lteqExpr)

    val gteqExpr = expression.GreaterThanOrEqual(A, B)
    println(gteqExpr)

    val notExpr = expression.Not(eqExpr)
    println(notExpr)

    val andExpr = expression.And(lteqExpr, gteqExpr)
    println(andExpr)

    val orExpr = expression.Or(lteqExpr, gteqExpr)
    println(orExpr)

    val exprs =
      Seq(eqExpr, ltExpr, gtExpr, lteqExpr, gteqExpr, notExpr, andExpr, orExpr)

    //bound expression to a schema
    val boundedExprs =
      exprs.map(expr => BindReferences.bindReference(expr, schema))

    //generate PredicateFuncs of the expressions
    val predicates = exprs.map(expr => GeneratePredicate.generate(expr, schema))

    //generate test data
    val rows =
      Range(0, 10000000).map(f =>
        InternalRow(Random.nextInt(), Random.nextInt())
      )

    //compare time of the code generated PredicateFuncs and the interpreted ones.
    val time1 = System.currentTimeMillis()
    for ((predicate, boundedExpr) <- predicates.zip(boundedExprs)) {
      for (row <- rows) {
        predicate.eval(row)
      }
    }

    val time2 = System.currentTimeMillis()
    for ((predicate, boundedExpr) <- predicates.zip(boundedExprs)) {
      for (row <- rows) {
        boundedExpr.eval(row)
      }
    }
    val time3 = System.currentTimeMillis()

    println(s"""
         |== Time Comparison Between Codegen and Interpretation execution ==
         |Codegen: ${time2 - time1} ms
         |Interpretation: ${time3 - time1} ms
         |""".stripMargin)

    //validate if the code generated PredicateFuncs return same results as the interpreted ones.
    for ((predicate, boundedExpr) <- predicates.zip(boundedExprs)) {
      for (row <- rows) {
        assert(predicate.eval(row) == boundedExpr.eval(row))
      }
    }

  }

}
