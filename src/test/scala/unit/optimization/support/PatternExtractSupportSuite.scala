package unit.optimization.support

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.ExecMode
import org.apache.spark.secco.optimization.plan.{
  BinaryJoin,
  CyclicJoinProperty,
  JoinProperty,
  JoinType,
  PrimaryKeyForeignKeyJoinConstraintProperty
}
import org.apache.spark.secco.optimization.support.ExtractRequiredProjectJoins
import util.SeccoFunSuite

class PatternExtractSupportSuite extends SeccoFunSuite {

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
  }

  test("ExtractRequiredProjectJoins") {

    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")
    val R4 = seccoSession.table("R4")

    // plan without hinder operator
    val plan1 = R1
      .join(R2, "R1.a = R2.b")
      .project("R1.a, R1.b, R2.c")
      .join(R3, "R2.c = R3.c")
      .queryExecution
      .analyzedPlan

    // plan with hinder operator
    val plan2 = R1
      .join(R2, "R1.a = R2.b")
      .project("R1.a, R1.b, R2.c")
      .select("R1.a < 10")
      .join(R3, "R2.c = R3.c")
      .queryExecution
      .analyzedPlan

    // plan with complex conditions
    val plan3 = R1
      .join(R2, "R1.a = R2.b and R1.b < R2.c and R1.b != 0")
      .project("R1.a, R1.b, R2.c")
      .join(R3, "R2.c = R3.c and R1.b < R3.d and R3.d = 1000")
      .queryExecution
      .analyzedPlan

    // plan with different joinTypes and joinProperties
    val plan4 = R1
      .join(R2, "R1.b = R2.b")
      .join(R3, "R2.c = R3.c")
      .join(R4, "R3.d = R4.d")
      .queryExecution
      .analyzedPlan

    print(plan4)

    // test resetRequirement
    ExtractRequiredProjectJoins.setRequiredJoinType(JoinType("outer"))
    ExtractRequiredProjectJoins.setRequiredJoinProperties(
      Seq(PrimaryKeyForeignKeyJoinConstraintProperty, CyclicJoinProperty)
    )
    ExtractRequiredProjectJoins.setRequiredExecMode(ExecMode.DelayComputation)

    ExtractRequiredProjectJoins.resetRequirement()
    assert(
      ExtractRequiredProjectJoins.requiredJoinType == JoinType(
        "inner"
      ) && ExtractRequiredProjectJoins.requiredExecMode == ExecMode.Coupled
        && ExtractRequiredProjectJoins.requiredJoinProperties == Set(),
      "the resetRequirement function is not working properly"
    )

    // test extraction with default on plan

    ExtractRequiredProjectJoins.resetRequirement()
    val (inputs1, conditions1, projectionList1, mode1) =
      ExtractRequiredProjectJoins.unapply(plan1).get

    assert(
      inputs1.toString() == "List(Relation[R1, Atomic]-> (a#0,b#1)\n, Relation[R2, Atomic]-> (b#2,c#3)\n, Relation[R3, Atomic]-> (c#4,d#5)\n)"
    )
    assert(conditions1.toString() == "List((a#0 = b#2), (c#3 = c#4))")
    assert(projectionList1.toString() == "List(a#0, b#1, c#3, c#4, d#5)")
    assert(mode1 == ExecMode.Coupled)

    // test extract with default on complex plans
    val (inputs2, _, _, _) =
      ExtractRequiredProjectJoins.unapply(plan2).get

    assert(
      inputs2.toString() == """List(Filter[(a#0 < 10), Coupled]-> (a#0,b#1,c#3)
                              |+- [0] Project[[a#0, b#1, c#3], Coupled]-> (a#0,b#1,c#3)
                              |   +- [0] BinaryJoin[Inner, (a#0 = b#2), Coupled]-> (a#0,b#1,b#2,c#3)
                              |      :- [0] Relation[R1, Atomic]-> (a#0,b#1)
                              |      +- [1] Relation[R2, Atomic]-> (b#2,c#3)
                              |, Relation[R3, Atomic]-> (c#4,d#5)
                              |)""".stripMargin
    )

    val (_, condition3, _, _) =
      ExtractRequiredProjectJoins.unapply(plan3).get

    assert(
      condition3
        .toString() == "List((a#0 = b#2), (b#1 < c#3), NOT (b#1 = 0), (c#3 = c#4), (b#1 < d#5), (d#5 = 1000))"
    )

    // test extract with different requirements

    ExtractRequiredProjectJoins.resetRequirement()
    assert(ExtractRequiredProjectJoins.unapply(plan4).nonEmpty)

    ExtractRequiredProjectJoins.setRequiredJoinType(JoinType("outer"))
    assert(ExtractRequiredProjectJoins.unapply(plan4).isEmpty)

    assert(
      ExtractRequiredProjectJoins
        .unapply(plan4 transform { case j: BinaryJoin =>
          j.copy(joinType = JoinType("outer"))
        })
        .nonEmpty
    )

    ExtractRequiredProjectJoins.resetRequirement()
    ExtractRequiredProjectJoins.setRequiredJoinProperties(
      Seq(CyclicJoinProperty, PrimaryKeyForeignKeyJoinConstraintProperty)
    )
    assert(ExtractRequiredProjectJoins.unapply(plan4).isEmpty)
    assert(
      ExtractRequiredProjectJoins
        .unapply(plan4 transform { case j: BinaryJoin =>
          j.copy(property = Set(JoinProperty("cyclic")))
        })
        .isEmpty
    )

    assert(
      ExtractRequiredProjectJoins
        .unapply(plan4 transform { case j: BinaryJoin =>
          j.copy(property = Set(JoinProperty("cyclic"), JoinProperty("pkfk")))
        })
        .nonEmpty
    )

    assert(
      ExtractRequiredProjectJoins
        .unapply(plan4 transform { case j: BinaryJoin =>
          j.copy(property =
            Set(
              JoinProperty("cyclic"),
              JoinProperty("pkfk"),
              JoinProperty("equi")
            )
          )
        })
        .nonEmpty
    )

  }

}
