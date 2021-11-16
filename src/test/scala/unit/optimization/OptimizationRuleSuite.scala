package unit.optimization

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.rules.{
  BooleanSimplification,
  ConstantFolding,
  ConstantPropagation,
  MarkJoinIntegrityConstraintProperty,
  MarkJoinPredicateProperty,
  MergeLimit,
  MergeProjection,
  MergeSelection,
  MergeUnion,
  OptimizePKFKJoin,
  PushDownProjection,
  PushDownSelection,
  PushSelectionIntoJoin,
  PushSelectionThroughJoin,
  RemoveDispensableExpressions,
  RemoveRedundantProjection,
  RemoveRedundantSelection,
  ReorderAssociativeOperator,
  SimplifyBinaryComparison
}
import util.SeccoFunSuite

import scala.util.Try

class OptimizationRuleSuite extends SeccoFunSuite {
  override def setupDB(): Unit = {
    val catalog = seccoSession.sessionState.catalog
    catalog.createTable(
      CatalogTable("R1", CatalogColumn("a") :: CatalogColumn("b") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R2", CatalogColumn("b") :: CatalogColumn("c") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R3", CatalogColumn("c") :: CatalogColumn("d") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R4", CatalogColumn("d") :: CatalogColumn("e") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R5", CatalogColumn("a") :: CatalogColumn("b") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R6", CatalogColumn("a") :: CatalogColumn("b") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R7", CatalogColumn("a") :: CatalogColumn("b") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R8", CatalogColumn("e") :: CatalogColumn("a") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R9", CatalogColumn("a") :: CatalogColumn("c") :: Nil)
    )
    catalog.createTable(
      CatalogTable(
        "R10",
        CatalogColumn("b") :: CatalogColumn("f") :: Nil,
        CatalogColumn("b") :: Nil
      )
    )
    catalog.createTable(
      CatalogTable(
        "R11",
        CatalogColumn("f") :: CatalogColumn("g") :: Nil,
        CatalogColumn("f") :: Nil
      )
    )

  }

  test("operator_push_down") {
    val sqlQuery =
      s"""
         |select R1.b, R2.c, R3.d
         |from R1, R2, R3, R4
         |where R1.b=R2.b and R2.c=R3.c and R3.d=R4.d and R1.a>10
         |""".stripMargin

    assert(Try {
      var plan = seccoSession.sql(sqlQuery).queryExecution.analyzedPlan
      plan = PushDownSelection(plan)
      plan = PushSelectionIntoJoin(plan)
      plan = PushSelectionThroughJoin(plan)
      plan = PushDownProjection(plan)
    }.isSuccess)

  }

  test("operator_combine") {
    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")
    val R5 = seccoSession.table("R5")
    val R6 = seccoSession.table("R6")
    val R7 = seccoSession.table("R7")

    assert(Try {
      var plan = R1
        .unionAll(R5)
        .unionAll(R6)
        .unionAll(R7)
        .join(R2, "R1.b=R2.b")
        .join(R3)
        .project("R1.a, R2.b, (R3.c + R3.d) as w")
        .project("R1.a, w")
        .select("R1.a > 10")
        .select("w > 100")
        .select("w < 1000")
        .limit(1000)
        .limit(100)
        .queryExecution
        .analyzedPlan

      plan = MergeUnion(plan)
      plan = MergeProjection(plan)
      plan = MergeSelection(MergeSelection(plan))
      plan = MergeLimit(plan)
    }.isSuccess)

  }

  test("remove_redundant") {
    val R1 = seccoSession.table("R1")

    assert(Try {
      var plan = R1.select("true").project("a, b").queryExecution.analyzedPlan

      plan = RemoveRedundantSelection(plan)
      plan = RemoveRedundantProjection(plan)
    }.isSuccess)
  }

  test("optimize_expression") {
    val sqlText =
      """
        |select a, b, (1+1) as c , (1+a+1) as d, +(a) as e
        |from R1
        |where a=1 and b=a+1 and not (true and false and b=1) and a>a
        |""".stripMargin

    assert(Try {
      var plan = seccoSession.sql(sqlText).queryExecution.analyzedPlan
      plan = ConstantPropagation(plan)
      plan = ConstantFolding(plan)
      plan = ReorderAssociativeOperator(plan)
      plan = BooleanSimplification(plan)
      plan = SimplifyBinaryComparison(plan)
      plan = RemoveDispensableExpressions(plan)
    }.isSuccess)

  }

  test("join_optimization") {

    val sqlText =
      """
        |select *
        |from R1 natural join R2 natural join R3 natural join R4 natural join R8
        |     natural join R9 natural join R10 natural join R11
        |""".stripMargin

//    val sqlText =
//      """
//        |select *
//        |from R1 natural join R2 natural join R10 natural join R11
//        |""".stripMargin

    var plan = seccoSession.sql(sqlText).queryExecution.analyzedPlan

    println(plan)

    plan = OptimizePKFKJoin(plan)

    println(plan)

  }

}
