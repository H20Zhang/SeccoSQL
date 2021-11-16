package unit.optimization.support

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.ExecMode
import org.apache.spark.secco.optimization.plan.PrimaryKeyForeignKeyJoinConstraintProperty
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
  }

  test("ExtractRequiredProjectJoins") {

    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")

    val plan1 = R1
      .join(R2, "R1.a = R2.b")
      .project("R1.a, R1.b, R2.c")
      .join(R3, "R2.c = R3.c")
      .queryExecution
      .analyzedPlan

    ExtractRequiredProjectJoins.resetRequirement()
    assert(ExtractRequiredProjectJoins.unapply(plan1).nonEmpty)
    val (inputs, conditions, projectionList, mode) =
      ExtractRequiredProjectJoins.unapply(plan1).get

    assert(
      inputs.toSet == Set(
        R1.queryExecution.analyzedPlan,
        R2.queryExecution.analyzedPlan,
        R3.queryExecution.analyzedPlan
      )
    )

    ExtractRequiredProjectJoins.resetRequirement()
    ExtractRequiredProjectJoins.setRequiredExecMode(ExecMode.Computation)
    assert(ExtractRequiredProjectJoins.unapply(plan1).isEmpty)

    ExtractRequiredProjectJoins.resetRequirement()
    ExtractRequiredProjectJoins.setRequiredJoinProperties(
      Seq(PrimaryKeyForeignKeyJoinConstraintProperty)
    )
    assert(ExtractRequiredProjectJoins.unapply(plan1).isEmpty)

  }

}
