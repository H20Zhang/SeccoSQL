package integration

import org.apache.spark.secco.execution.InternalRow
import org.apache.spark.secco.execution.planning.SeccoPlanner
import org.apache.spark.secco.optimization.SeccoOptimizer
import util.TestQuery.TestQuery
import util.{SeccoFunSuite, TestCase, TestQuery}

class SeccoPlannerIntegrationTest extends SeccoFunSuite {
  val optimizer = new SeccoOptimizer
  val planner = new SeccoPlanner

  def genPlan(
      testQuery: TestQuery,
      dataOfScans: Map[String, Array[InternalRow]] = Map()
  ) = {

    pprint.pprintln(s"------------testing ${testQuery} ------------")
    val expr = TestCase.queryWithArrayInput(testQuery, dataOfScans)
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

  def executePlan(
      testQuery: TestQuery,
      dataOfScans: Map[String, Array[InternalRow]] = Map()
  ) = {

    pprint.pprintln(s"------------testing ${testQuery} ------------")
    val expr = TestCase.queryWithArrayInput(testQuery, dataOfScans)
    println()
    pprint.pprintln("original expr")
    println(s"${expr}")

    val optimizedExpr = optimizer.execute(expr)
    pprint.pprintln("optimized expr")
    println(s"${optimizedExpr}")

    val plannedExpr = planner.plan(optimizedExpr).next()
    pprint.pprintln("planned expr")
    println(s"${plannedExpr}")

    val res = plannedExpr.collectSeq()
    pprint.pprintln(s"executed expr")
    println(s"$plannedExpr)")

    res
  }

  import util.Extension._

//  test("assignDataOfScans") {
//
//    val tuplesOfR1 = "R1" -> Array(
//      Array(0, 1),
//      Array(1, 2),
//      Array(2, 3)
//    ).toDoubleArray()
//
//    val tuplesOfR2 = "R2" -> Array(
//      Array(0, 1),
//      Array(1, 2),
//      Array(2, 3)
//    ).toDoubleArray()
//
//    val tupleMap = Map(tuplesOfR1, tuplesOfR2)
//
//    val plannedExpr = genPlan(TestQuery.simpleAcyclicJoin, tupleMap)
//
//    val scans = plannedExpr.collect {
//      case s: ScanExec => s
//    }
//
//    val scan0 = scans(0)
//    val scan1 = scans(1)
//
//    assert(
//      scan0.toSeq() == scan1.toSeq()
//    )
//
//    scans.foreach { scan =>
//      println()
//      pprint.pprintln(scan.toSeq())
//    }
//  }

  test("simpleAcyclicJoin") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(1, 2),
      Array(2, 3),
      Array(3, 4)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR1, tuplesOfR2)

    val res = executePlan(TestQuery.simpleAcyclicJoin, tupleMap)

    val groundTruth = Array(
      Array(0, 1, 2),
      Array(1, 2, 3),
      Array(2, 3, 4)
    ).toDoubleArray()

    assert(res.toSet == groundTruth.toSet)
  }

  test("simpleCyclicJoin") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 2),
      Array(4, 3),
      Array(2, 4)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 3)
    ).toDoubleArray()

    val tuplesOfR3 = "R3" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 3),
      Array(4, 2),
      Array(2, 3)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfR3)

    val res = executePlan(TestQuery.simpleCyclicJoin, tupleMap)

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 0, 0),
      Array(1, 1, 1),
      Array(1, 2, 3),
      Array(4, 3, 2),
      Array(2, 4, 3)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)
  }

  test("complexCyclicJoin1") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 2),
      Array(4, 3),
      Array(2, 4)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 3)
    ).toDoubleArray()

    val tuplesOfR3 = "R3" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(3, 4),
      Array(2, 1),
      Array(3, 1)
    ).toDoubleArray()

    val tuplesOfR4 = "R4" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(4, 1),
      Array(1, 4),
      Array(1, 2)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfR3, tuplesOfR4)

    val plannedExpr = genPlan(TestQuery.complexCyclicJoin1, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 0, 0, 0),
      Array(1, 1, 1, 1),
      Array(1, 2, 3, 4),
      Array(4, 3, 2, 1),
      Array(2, 4, 3, 1),
      Array(1, 2, 3, 1)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)
  }

  test("complexCyclicJoin2") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 2),
      Array(4, 3),
      Array(2, 4)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 3)
    ).toDoubleArray()

    val tuplesOfR3 = "R3" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(3, 4),
      Array(2, 1),
      Array(3, 1)
    ).toDoubleArray()

    val tuplesOfR4 = "R4" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(4, 3),
      Array(1, 4),
      Array(1, 5)
    ).toDoubleArray()

    val tuplesOfR5 = "R5" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(4, 4),
      Array(2, 5)
    ).toDoubleArray()

    val tuplesOfR6 = "R6" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 3),
      Array(4, 2),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfR7 = "R7" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 4),
      Array(4, 1),
      Array(2, 1)
    ).toDoubleArray()

    val tupleMap = Map(
      tuplesOfR1,
      tuplesOfR2,
      tuplesOfR3,
      tuplesOfR4,
      tuplesOfR5,
      tuplesOfR6,
      tuplesOfR7
    )

    val plannedExpr = genPlan(TestQuery.complexCyclicJoin2, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 0, 0, 0, 1),
      Array(1, 1, 1, 1, 2),
      Array(1, 2, 3, 4, 3),
      Array(4, 3, 2, 1, 4),
      Array(2, 4, 3, 1, 5),
      Array(1, 2, 3, 1, 2)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)
  }

  test("complexCyclicJoin3") {

    //R1(A, B)
    val tuplesOfR1 = "R1" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 2),
      Array(4, 3),
      Array(2, 4)
    ).toDoubleArray()

    //R2(B, C)
    val tuplesOfR2 = "R2" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 3)
    ).toDoubleArray()

    //R3(C, D)
    val tuplesOfR3 = "R3" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(3, 4),
      Array(2, 1),
      Array(3, 1)
    ).toDoubleArray()

    //R4(D, E)
    val tuplesOfR4 = "R4" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(4, 3),
      Array(1, 4),
      Array(1, 5)
    ).toDoubleArray()

    //R5(A, E)
    val tuplesOfR5 = "R5" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(4, 4),
      Array(2, 5)
    ).toDoubleArray()

    //R6(C, E)
    val tuplesOfR6 = "R6" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(3, 3),
      Array(2, 4),
      Array(3, 5)
    ).toDoubleArray()

    val tupleMap = Map(
      tuplesOfR1,
      tuplesOfR2,
      tuplesOfR3,
      tuplesOfR4,
      tuplesOfR5,
      tuplesOfR6
    )

    val plannedExpr = genPlan(TestQuery.complexCyclicJoin3, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 0, 0, 0, 1),
      Array(1, 1, 1, 1, 2),
      Array(1, 2, 3, 4, 3),
      Array(4, 3, 2, 1, 4),
      Array(2, 4, 3, 1, 5)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)
  }

  test("joinWithCounting1") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(1, 2),
      Array(2, 3),
      Array(3, 4)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR1, tuplesOfR2)

    val groundTruth = Array(
      Array(1, 1),
      Array(2, 1),
      Array(3, 1)
    ).toDoubleSeq()

    val res = executePlan(TestQuery.joinWithCounting1, tupleMap)
    assert(res.toSet == groundTruth.toSet)

  }

  test("joinWithCounting2") {
    //R1(A, B)
    val tuplesOfR1 = "R1" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 2),
      Array(4, 3),
      Array(2, 4)
    ).toDoubleArray()

    //R2(B, C)
    val tuplesOfR2 = "R2" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 3)
    ).toDoubleArray()

    //R3(C, D)
    val tuplesOfR3 = "R3" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(3, 4),
      Array(2, 1),
      Array(3, 1)
    ).toDoubleArray()

    //R4(D, E)
    val tuplesOfR4 = "R4" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(4, 3),
      Array(1, 4),
      Array(1, 5)
    ).toDoubleArray()

    //R5(A, E)
    val tuplesOfR5 = "R5" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(4, 4),
      Array(2, 5)
    ).toDoubleArray()

    //R6(C, E)
    val tuplesOfR6 = "R6" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(3, 3),
      Array(2, 4),
      Array(3, 5)
    ).toDoubleArray()

    val tupleMap = Map(
      tuplesOfR1,
      tuplesOfR2,
      tuplesOfR3,
      tuplesOfR4,
      tuplesOfR5,
      tuplesOfR6
    )

    val plannedExpr = genPlan(TestQuery.joinWithCounting2, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 1),
      Array(1, 2),
      Array(4, 1),
      Array(2, 1)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)
  }

  test("joinWithPKFK1") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(1, 2),
      Array(2, 3),
      Array(3, 4)
    ).toDoubleArray()

    val tuplesOfR3 = "R3" -> Array(
      Array(2, 2),
      Array(3, 3),
      Array(4, 4)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfR3)

    val plannedExpr = genPlan(TestQuery.joinWithPKFK1, tupleMap)

    val groundTruth = Array(
      Array(0, 1, 2, 2),
      Array(1, 2, 3, 3),
      Array(2, 3, 4, 4)
    ).toDoubleSeq()

    assert(plannedExpr.collectSeq().toSet == groundTruth.toSet)
  }

  test("joinWithPKFK2") {
    val tuplesOfR1 = "R1" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 2),
      Array(4, 3),
      Array(2, 4)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 3)
    ).toDoubleArray()

    val tuplesOfR3 = "R3" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(3, 4),
      Array(2, 1),
      Array(3, 1)
    ).toDoubleArray()

    val tuplesOfR4 = "R4" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(4, 1),
      Array(1, 4),
      Array(1, 2)
    ).toDoubleArray()

    val tuplesOfR5 = "R5" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(4, 4)
    ).toDoubleArray()

    val tupleMap =
      Map(tuplesOfR1, tuplesOfR2, tuplesOfR3, tuplesOfR4, tuplesOfR5)

    val plannedExpr = genPlan(TestQuery.joinWithPKFK2, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 0, 0, 0, 0),
      Array(1, 1, 1, 1, 1),
      Array(1, 2, 3, 4, 4),
      Array(4, 3, 2, 1, 1),
      Array(2, 4, 3, 1, 1),
      Array(1, 2, 3, 1, 1)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)

  }

  //TODO: test below
  test("joinWithAggregateAndProject1") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 2),
      Array(4, 3),
      Array(2, 4)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 3)
    ).toDoubleArray()

    val tuplesOfR3 = "R3" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(3, 4),
      Array(2, 1),
      Array(3, 1)
    ).toDoubleArray()

    val tuplesOfR4 = "R4" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(4, 1),
      Array(1, 4),
      Array(1, 2)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfR3, tuplesOfR4)

    val plannedExpr = genPlan(TestQuery.joinWithAggregateAndProject1, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 1),
      Array(1, 2),
      Array(4, 1),
      Array(2, 1)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)
  }

  test("joinWithAggregateAndProject2") {
    val tuplesOfR1 = "R1" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 2),
      Array(4, 3),
      Array(2, 4)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 3)
    ).toDoubleArray()

    val tuplesOfR3 = "R3" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(3, 4),
      Array(2, 1),
      Array(3, 1)
    ).toDoubleArray()

    val tuplesOfR4 = "R4" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(4, 1),
      Array(1, 4),
      Array(1, 2)
    ).toDoubleArray()

    val tuplesOfR5 = "R5" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 3),
      Array(4, 2),
      Array(2, 3)
    ).toDoubleArray()

    val tupleMap =
      Map(tuplesOfR1, tuplesOfR2, tuplesOfR3, tuplesOfR4, tuplesOfR5)

    val plannedExpr = genPlan(TestQuery.joinWithAggregateAndProject2, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 1),
      Array(1, 1),
      Array(2, 2),
      Array(3, 1),
      Array(4, 1)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)
  }

  test("joinWithAggregateAndProject3") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 2),
      Array(4, 3),
      Array(2, 4)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 3),
      Array(3, 2),
      Array(4, 3)
    ).toDoubleArray()

    val tuplesOfR3 = "R3" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(1, 3),
      Array(4, 2),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfR4 = "R4" -> Array(
      Array(0, 0),
      Array(1, 1),
      Array(2, 2),
      Array(3, 3)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfR3, tuplesOfR4)

    val plannedExpr = genPlan(TestQuery.joinWithAggregateAndProject3, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 1),
      Array(1, 1),
      Array(2, 1),
      Array(3, 1),
      Array(4, 1)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)
  }

  test("joinWithAggregateAndProject4") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(1, 4),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(1, 2),
      Array(2, 3),
      Array(3, 3),
      Array(4, 4),
      Array(3, 4)
    ).toDoubleArray()

    val tuplesOfR3 = "R3" -> Array(
      Array(2, 2),
      Array(3, 3),
      Array(4, 4)
    ).toDoubleArray()

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfR3)

    val plannedExpr = genPlan(TestQuery.joinWithAggregateAndProject4, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Array(
      Array(0, 2),
      Array(1, 7),
      Array(2, 7)
    ).toDoubleSeq()

    assert(res.toSet == groundTruth.toSet)

//    genPlan(TestQuery.joinWithAggregateAndProject4)
  }

  test("simpleGraphAnalytic1") {

    val tuplesOfG = "G" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(1, 4),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfDegree = "Degree" -> Array(
      Array(1.0, 1.0),
      Array(2.0, 1.0),
      Array(3.0, 0.5),
      Array(4.0, 1.0)
    )

    val tuplesOfDeltaW = "DeltaW" -> Array(
      Array(1.0, 1.0),
      Array(2.0, 1.0),
      Array(3.0, 1.0),
      Array(4.0, 1.0)
    )

    val tupleMap = Map(tuplesOfG, tuplesOfDegree, tuplesOfDeltaW)

    val plannedExpr = genPlan(TestQuery.simpleGraphAnalytic1, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Seq(
      Seq(0.0, 1.0),
      Seq(1.0, 2.275),
      Seq(2.0, 0.575)
    )

    assert(res.toSet == groundTruth.toSet)
  }

  test("simpleGraphAnalytic2") {

    val tuplesOfG = "G" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(1, 4),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfDeltaW = "DeltaW" -> Array(
      Array(1.0, 1.0),
      Array(2.0, 2.0),
      Array(3.0, 3.0),
      Array(4.0, 4.0)
    )

    val tupleMap = Map(tuplesOfG, tuplesOfDeltaW)

    val plannedExpr = genPlan(TestQuery.simpleGraphAnalytic2, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Seq(
      Seq(0.0, 1.0),
      Seq(1.0, 2.0),
      Seq(2.0, 3.0)
    )

    assert(res.toSet == groundTruth.toSet)
  }

  test("simpleGraphAnalytic3") {

    val tuplesOfG = "G" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(1, 4),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfDeltaW = "DeltaW" -> Array(
      Array(1.0, 0.0)
    )

    val tupleMap = Map(tuplesOfG, tuplesOfDeltaW)

    val plannedExpr = genPlan(TestQuery.simpleGraphAnalytic3, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Seq(
      Seq(0.0, 1.0)
    )

    assert(res.toSet == groundTruth.toSet)
  }

  test("complexGraphAnalytic1") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(1, 4),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(1, 4),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfDegree = "Degree" -> Array(
      Array(1.0, 1.0),
      Array(2.0, 1.0),
      Array(3.0, 0.5),
      Array(4.0, 1.0)
    )

    val tuplesOfDeltaW = "DeltaW" -> Array(
      Array(1.0, 1.0),
      Array(2.0, 1.0),
      Array(3.0, 1.0),
      Array(4.0, 1.0)
    )

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfDegree, tuplesOfDeltaW)

    val plannedExpr = genPlan(TestQuery.complexGraphAnalytic1, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Seq(
      Seq(0.0, 2.275),
      Seq(1.0, 0.575)
    )

    assert(res.toSet == groundTruth.toSet)
  }

  test("complexGraphAnalytic2") {

    val tuplesOfR1 = "R1" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(1, 4),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfR2 = "R2" -> Array(
      Array(0, 1),
      Array(1, 2),
      Array(1, 3),
      Array(1, 4),
      Array(2, 3)
    ).toDoubleArray()

    val tuplesOfDegree = "Degree" -> Array(
      Array(1.0, 1.0),
      Array(2.0, 1.0),
      Array(3.0, 0.5),
      Array(4.0, 1.0)
    )

    val tuplesOfDeltaW = "DeltaW" -> Array(
      Array(1.0, 1.0),
      Array(2.0, 1.0),
      Array(3.0, 1.0),
      Array(4.0, 1.0)
    )

    val tupleMap = Map(tuplesOfR1, tuplesOfR2, tuplesOfDegree, tuplesOfDeltaW)

    val plannedExpr = genPlan(TestQuery.complexGraphAnalytic2, tupleMap)

    val res = plannedExpr.collectSeq()

    pprint.pprintln(res)

    val groundTruth = Seq(
      Seq(0.0, 2.275),
      Seq(1.0, 0.575)
    )

    assert(res.toSet == groundTruth.toSet)
  }
}
