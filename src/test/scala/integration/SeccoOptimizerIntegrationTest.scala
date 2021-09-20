package integration

import org.apache.spark.secco.optimization.SeccoOptimizer
import util.TestQuery.TestQuery
import util.{SeccoFunSuite, TestCase, TestQuery}

class SeccoOptimizerIntegrationTest extends SeccoFunSuite {

  val optimizer = new SeccoOptimizer

  def optimize(testQuery: TestQuery) = {

    pprint.pprintln(s"------------testing ${testQuery} ------------")
    val expr = TestCase.queryWithArrayInput(testQuery)
    println()
    println(expr)
    val optimizedExpr = optimizer.execute(expr)
    println(optimizedExpr)

//    val lops = optimizedExpr.collect { case l: LOp => l }
//    lops.foreach { lop =>
//      println(lop.rootPlan)
//    }
  }

  test("simpleAcyclicJoin") {
    optimize(TestQuery.simpleAcyclicJoin)
  }

  test("simpleCyclicJoin") {
    optimize(TestQuery.simpleCyclicJoin)
  }

  test("complexCyclicJoin1") {
    optimize(TestQuery.complexCyclicJoin1)
  }

  test("complexCyclicJoin2") {
    optimize(TestQuery.complexCyclicJoin2)
  }

  test("complexCyclicJoin3") {
    optimize(TestQuery.complexCyclicJoin3)
  }

  test("joinWithCounting1") {
    optimize(TestQuery.joinWithCounting1)
  }

  test("joinWithCounting2") {
    optimize(TestQuery.joinWithCounting2)
  }

  test("joinWithPKFK1") {
    optimize(TestQuery.joinWithPKFK1)
  }

  test("joinWithPKFK2") {
    optimize(TestQuery.joinWithPKFK2)
  }

  //fixme: there are some problems with the constraints.
  test("joinWithAggregateAndProject1") {
    optimize(TestQuery.joinWithAggregateAndProject1)
  }

  test("joinWithAggregateAndProject2") {
    optimize(TestQuery.joinWithAggregateAndProject2)
  }

  test("joinWithAggregateAndProject3") {
    optimize(TestQuery.joinWithAggregateAndProject3)
  }

  test("joinWithAggregateAndProject4") {
    optimize(TestQuery.joinWithAggregateAndProject4)
  }

  test("simpleGraphAnalytic1") {
    optimize(TestQuery.simpleGraphAnalytic1)
  }

  test("simpleGraphAnalytic2") {
    optimize(TestQuery.simpleGraphAnalytic2)
  }

  test("simpleGraphAnalytic3") {
    optimize(TestQuery.simpleGraphAnalytic3)
  }

  test("complexGraphAnalytic1") {
    optimize(TestQuery.complexGraphAnalytic1)
  }

  test("complexGraphAnalytic2") {
    optimize(TestQuery.complexGraphAnalytic2)
  }

}
