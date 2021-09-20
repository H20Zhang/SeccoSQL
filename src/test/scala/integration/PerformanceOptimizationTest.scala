package integration

import org.apache.spark.secco.execution.InternalRow
import org.apache.spark.secco.execution.planning.SeccoPlanner
import org.apache.spark.secco.optimization.SeccoOptimizer
import org.apache.spark.rdd.RDD
import util.TestQuery.TestQuery
import util.{SeccoFunSuite, TestCase, TestData, TestQuery}

class PerformanceOptimizationTest extends SeccoFunSuite {
  val optimizer = new SeccoOptimizer
  val planner = new SeccoPlanner

  def genPlan(
      testQuery: TestQuery,
      dataOfScans: Map[String, RDD[InternalRow]] = Map()
  ) = {

    pprint.pprintln(s"------------testing ${testQuery} ------------")
    val expr = TestCase.queryWithRDDInput(testQuery, dataOfScans)
    println()
    pprint.pprintln("original expr")
    println(s"${expr}")

    val optimizedExpr = optimizer.execute(expr)
    pprint.pprintln("optimized expr")
    println(s"${optimizedExpr}")

    val plannedExpr = planner.plan(optimizedExpr).next()
    pprint.pprintln("planned expr")
    println(s"${plannedExpr}")

    plannedExpr
  }

  test("loadData") {
    val euData = TestData.data("eu")
    pprint.pprintln(euData.count())
  }

  lazy val euData = TestData.data("eu")
  lazy val wikiData = TestData.data("wiki")
  lazy val debugData = TestData.data("debug")

  test("simpleCyclicJoin") {

    val testData = wikiData

    val tuplesOfR1 = "R1" -> testData
    val tuplesOfR2 = "R2" -> testData
    val tuplesOfR3 = "R3" -> testData

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfR3)
    val plannedExpr = genPlan(TestQuery.simpleCyclicJoin, tupleMap)
    val res = plannedExpr.count()

    pprint.pprintln("executed plan:")
    println(plannedExpr)
    pprint.pprintln(res)
  }

  test("simpleAcyclicJoin") {

    val testData = wikiData

    val tuplesOfR1 = "R1" -> testData
    val tuplesOfR2 = "R2" -> testData
//    val tuplesOfR3 = "R3" -> testData

    val tupleMap = Map(tuplesOfR1, tuplesOfR2)
    val plannedExpr = genPlan(TestQuery.simpleAcyclicJoin, tupleMap)
    val res = plannedExpr.count()

    pprint.pprintln("executed plan:")
    println(plannedExpr)
    pprint.pprintln(res)
  }

  test("complexCyclicJoin3") {

    val testData = wikiData

    val tuplesOfR1 = "R1" -> testData
    val tuplesOfR2 = "R2" -> testData
    val tuplesOfR3 = "R3" -> testData
    val tuplesOfR4 = "R4" -> testData
    val tuplesOfR5 = "R5" -> testData
    val tuplesOfR6 = "R6" -> testData

    val tupleMap = Map(
      tuplesOfR1,
      tuplesOfR2,
      tuplesOfR3,
      tuplesOfR4,
      tuplesOfR5,
      tuplesOfR6
    )
    val plannedExpr = genPlan(TestQuery.complexCyclicJoin3, tupleMap)
    val res = plannedExpr.count()

    pprint.pprintln("executed plan:")
    println(plannedExpr)
    pprint.pprintln(res)
  }

  test("joinWithCounting2") {

    val testData = wikiData

    val tuplesOfR1 = "R1" -> testData
    val tuplesOfR2 = "R2" -> testData
    val tuplesOfR3 = "R3" -> testData
    val tuplesOfR4 = "R4" -> testData
    val tuplesOfR5 = "R5" -> testData
    val tuplesOfR6 = "R6" -> testData

    val tupleMap = Map(
      tuplesOfR1,
      tuplesOfR2,
      tuplesOfR3,
      tuplesOfR4,
      tuplesOfR5,
      tuplesOfR6
    )
    val plannedExpr = genPlan(TestQuery.joinWithCounting2, tupleMap)
    val res = plannedExpr.rdd().map(f => f(1)).sum().toLong
//    val res = plannedExpr.count()

    pprint.pprintln("executed plan:")
    println(plannedExpr)
    pprint.pprintln(res)
  }

  test("joinWithPKFK1") {

    val testData = wikiData
    val pkfkData = testData
      .map(f => (f(0), 1.0))
      .distinct()
      .map(f => Array(f._1, f._2))
      .cache()
    pkfkData.count()

    val tuplesOfR1 = "R1" -> testData
    val tuplesOfR2 = "R2" -> testData
    val tuplesOfR3 = "R3" -> pkfkData

    val tupleMap = Map(
      tuplesOfR1,
      tuplesOfR2,
      tuplesOfR3
    )
    val plannedExpr = genPlan(TestQuery.joinWithPKFK1, tupleMap)
    val res = plannedExpr.count()

    pprint.pprintln("executed plan:")
    println(plannedExpr)
    pprint.pprintln(res)
  }

  test("joinWithAggregateAndProject4") {

    val testData = wikiData
    val pkfkData = testData
      .map(f => (f(0), 1.0))
      .distinct()
      .map(f => Array(f._1, f._2))
      .cache()
    pkfkData.count()

    val tuplesOfR1 = "R1" -> testData
    val tuplesOfR2 = "R2" -> testData
    val tuplesOfR3 = "R3" -> pkfkData

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfR3)

    val plannedExpr = genPlan(TestQuery.joinWithAggregateAndProject4, tupleMap)

    val res = plannedExpr.count()

    pprint.pprintln("executed plan:")
    println(plannedExpr)
    pprint.pprintln(res)
  }

  test("simpleGraphAnalytic1") {

    val testData = wikiData
    val degreeData = testData
      .map(f => (f(0), 1.0))
      .reduceByKey(_ + _)
      .map(f => Array(f._1, f._2))
      .cache()
    degreeData.count()
    val deltaWData = testData.map(f => Array(f(0), 1.0)).cache()
    deltaWData.count()

    val tuplesOfG = "G" -> testData
    val tuplesOfDegree = "Degree" -> degreeData
    val tuplesOfDeltaW = "DeltaW" -> deltaWData

    val tupleMap = Map(tuplesOfG, tuplesOfDegree, tuplesOfDeltaW)

    val plannedExpr = genPlan(TestQuery.simpleGraphAnalytic1, tupleMap)

    val res = plannedExpr.count()

    pprint.pprintln("executed plan:")
    println(plannedExpr)
    pprint.pprintln(res)
  }

//  test("complexGraphAnalytic1") {
//
//    val tuplesOfR1 = "R1" -> Array(
//      Array(0, 1),
//      Array(1, 2),
//      Array(1, 3),
//      Array(1, 4),
//      Array(2, 3)
//    ).toDoubleArray()
//
//    val tuplesOfR2 = "R2" -> Array(
//      Array(0, 1),
//      Array(1, 2),
//      Array(1, 3),
//      Array(1, 4),
//      Array(2, 3)
//    ).toDoubleArray()
//
//    val tuplesOfDegree = "Degree" -> Array(
//      Array(1.0, 1.0),
//      Array(2.0, 1.0),
//      Array(3.0, 0.5),
//      Array(4.0, 1.0)
//    )
//
//    val tuplesOfDeltaW = "DeltaW" -> Array(
//      Array(1.0, 1.0),
//      Array(2.0, 1.0),
//      Array(3.0, 1.0),
//      Array(4.0, 1.0)
//    )
//
//    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfDegree, tuplesOfDeltaW)
//
//    val plannedExpr = genPlan(TestQuery.complexGraphAnalytic1, tupleMap)
//
//    val res = plannedExpr.collectSeq()
//
//    pprint.pprintln(res)
//
//    val groundTruth = Seq(
//      Seq(0.0, 2.275),
//      Seq(1.0, 0.575)
//    )
//
//    assert(res.toSet == groundTruth.toSet)
//  }

}
