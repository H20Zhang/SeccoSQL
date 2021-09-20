package playground

import org.apache.spark.secco.execution.planning.SeccoPlanner
import org.apache.spark.secco.optimization.SeccoOptimizer
import util.{SeccoFunSuite, TestCase}

class CartesianProductPlay extends SeccoFunSuite {

  import org.apache.spark.secco.analysis.RelationAlgebraWithAnalysis._
  import util.Extension._

  val optimizer = new SeccoOptimizer
  val planner = new SeccoPlanner

  test("cartesian_product") {

    val tuplesOfR = "R" -> Array(
      Array(0, 1),
      Array(1, 2)
    ).toDoubleArray()

    val tuplesOfS = "S" -> Array(
      Array(2, 3),
      Array(3, 4)
    ).toDoubleArray()

    val tuplesOfT = "T" -> Array(
      Array(4, 5),
      Array(5, 6)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR, tuplesOfS, tuplesOfT)

    val expr =
      TestCase.assignArrayData(
        cartesianProduct("T(E, F)", cartesianProduct("R(A, B)", "S(C, D)")),
        tupleMap
      )

    pprint.pprintln(s"--------------unoptimized expr-------------")
    println(expr)

    val optimizedExpr = optimizer.execute(expr)
    pprint.pprintln(s"--------------optimized expr-------------")
    println(optimizedExpr)

    val plannedExpr = planner.plan(optimizedExpr).next()
    pprint.pprintln(s"--------------planned expr-------------")
    println(plannedExpr)

    val res = plannedExpr.collectSeq()
    pprint.pprintln(s"--------------res-------------")
    pprint.pprintln(res)
  }

  test("theta_join") {

    val tuplesOfR = "R" -> Array(
      Array(0, 1),
      Array(7, 2)
    ).toDoubleArray()

    val tuplesOfS = "S" -> Array(
      Array(2, 3),
      Array(3, 4)
    ).toDoubleArray()

    val tuplesOfT = "T" -> Array(
      Array(4, 5),
      Array(5, 6)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR, tuplesOfS, tuplesOfT)

    val expr =
      TestCase.assignArrayData(
        select(
          "A>E",
          cartesianProduct("T(E, F)", cartesianProduct("R(A, B)", "S(C, D)"))
        ),
        tupleMap
      )

    pprint.pprintln(s"--------------unoptimized expr-------------")
    println(expr)

    val optimizedExpr = optimizer.execute(expr)
    pprint.pprintln(s"--------------optimized expr-------------")
    println(optimizedExpr)

    val plannedExpr = planner.plan(optimizedExpr).next()
    pprint.pprintln(s"--------------planned expr-------------")
    println(plannedExpr)

    val res = plannedExpr.collectSeq()
    pprint.pprintln(s"--------------res-------------")
    pprint.pprintln(res)
  }
}
