package unit.optimization.utils.ghd

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.plan.BinaryJoin
import org.apache.spark.secco.optimization.support.ExtractRequiredProjectJoins
import org.apache.spark.secco.optimization.util.ghd.JoinHyperGraph
import org.apache.spark.secco.util.DebugUtils
import util.SeccoFunSuite

import scala.util.Try

//TODO: test the JoinHyperGraph
class JoinHyperGraphSuite extends SeccoFunSuite {

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
      CatalogTable("R4", CatalogColumn("d") :: CatalogColumn("a") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R5", CatalogColumn("a") :: CatalogColumn("c") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R6", CatalogColumn("c") :: CatalogColumn("f") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R7", CatalogColumn("c") :: CatalogColumn("f") :: Nil)
    )
    catalog.createTable(
      CatalogTable(
        "R8",
        CatalogColumn("a") :: CatalogColumn("b") :: CatalogColumn("c") :: Nil
      )
    )
    catalog.createTable(
      CatalogTable(
        "R9",
        CatalogColumn("b") :: CatalogColumn("c") :: CatalogColumn("d") :: Nil
      )
    )
    catalog.createTable(
      CatalogTable(
        "R10",
        CatalogColumn("a") :: CatalogColumn("b") :: CatalogColumn(
          "c"
        ) :: CatalogColumn("d") :: Nil
      )
    )

  }

  test("joinHyperGraph_operation") {

    seccoSession.sessionState.conf.setVerboseOutput(false)

    val sqlText =
      """
        |select *
        |from R1 natural join R2 natural join R3 natural join R4 natural join R5
        |""".stripMargin

    val plan = seccoSession.sql(sqlText).queryExecution.analyzedPlan
    val binaryJoinPlan = plan.collectFirst { case s: BinaryJoin => s }.get

    val (inputs, conditions, projectionList, mode) =
      ExtractRequiredProjectJoins.unapply(binaryJoinPlan).get

    val joinHyperGraph = JoinHyperGraph(inputs, conditions)

    println(joinHyperGraph.toString)
    assert(joinHyperGraph.isCyclic() == false)
    assert(
      DebugUtils.relaxedStringEqual(
        joinHyperGraph.toPlan().toString,
        """BinaryJoin[Inner, ((a#8 = a#0) && (c#4 = c#3)), Coupled]
                                                         |:- [0] MultiwayJoin[[(a#14 = a#0), (c#15 = c#3), (b#1 = b#2)], {CYCLIC, EQUI-JOIN}, Coupled]
                                                         |:  :- [0] Project[[a#14, c#15], Coupled]
                                                         |:  :  +- [0] Relation[R5, Atomic]
                                                         |:  :- [1] Relation[R1, Atomic]
                                                         |:  +- [2] Relation[R2, Atomic]
                                                         |+- [1] MultiwayJoin[[(d#6 = d#5), (a#7 = a#8), (c#4 = c#9)], {CYCLIC, EQUI-JOIN}, Coupled]
                                                         |   :- [0] Relation[R4, Atomic]
                                                         |   :- [1] Relation[R3, Atomic]
                                                         |   +- [2] Relation[R5, Atomic]""".stripMargin
      )
    )
  }

  test("joinHyperGraph_wrt_complex_query") {

    seccoSession.sessionState.conf.setVerboseOutput(false)

    // join with relations some of whose relation does not participate in join
    val plan1 = seccoSession
      .sql("""
                                   |select *
                                   |from R1 natural join R2 natural join R6
                                   |""".stripMargin)
      .queryExecution
      .analyzedPlan

    // join with relations with same output attribute
    val plan2 = seccoSession
      .sql("""
                                   |select *
                                   |from R1 natural join R2 natural join R6 natural join R7
                                   |""".stripMargin)
      .queryExecution
      .analyzedPlan

    // acyclic join
    val plan3 = seccoSession
      .sql("""
                                   |select *
                                   |from R1 natural join R2 natural join R3
                                   |""".stripMargin)
      .queryExecution
      .analyzedPlan

    // cyclic join
    val plan4 = seccoSession
      .sql("""
                                   |select *
                                   |from R1 natural join R2 natural join R3 natural join R4
                                   |""".stripMargin)
      .queryExecution
      .analyzedPlan

    // join with multiple condition between two relation
    val plan5 = seccoSession
      .sql("""
                                   |select *
                                   |from R8 natural join R9 natural join R10
                                   |""".stripMargin)
      .queryExecution
      .analyzedPlan

    // join with theta-join condition (should fail during testing)
    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")

    val plan6 = R1.join(R2, "R1.a < R2.b").join(R3).queryExecution.analyzedPlan

    // join with self-join condition (should fail during testing)
    val plan7 = R1
      .join(R2, "R1.a = R2.b")
      .join(R3, "R3.c = R2.c and R3.c = R3.d")
      .queryExecution
      .analyzedPlan

    val (inputs1, conditions1, _, _) =
      ExtractRequiredProjectJoins
        .unapply(plan1.collectFirst { case s: BinaryJoin => s }.get)
        .get

    assert(
      Try(
        JoinHyperGraph(inputs1, conditions1)
          .toPlan()
      ).isSuccess
    )

    val (inputs2, conditions2, _, _) =
      ExtractRequiredProjectJoins
        .unapply(plan2.collectFirst { case s: BinaryJoin => s }.get)
        .get

    assert(
      Try(
        JoinHyperGraph(inputs2, conditions2)
          .toPlan()
      ).isSuccess
    )

    val (inputs3, conditions3, _, _) =
      ExtractRequiredProjectJoins
        .unapply(plan3.collectFirst { case s: BinaryJoin => s }.get)
        .get

    assert(
      Try(
        JoinHyperGraph(inputs3, conditions3)
          .toPlan()
      ).isSuccess
    )

    val (inputs4, conditions4, _, _) =
      ExtractRequiredProjectJoins
        .unapply(plan4.collectFirst { case s: BinaryJoin => s }.get)
        .get

    assert(
      Try(
        JoinHyperGraph(inputs4, conditions4)
          .toPlan()
      ).isSuccess
    )

    val (inputs5, conditions5, _, _) =
      ExtractRequiredProjectJoins
        .unapply(plan5.collectFirst { case s: BinaryJoin => s }.get)
        .get

    assert(
      Try(
        JoinHyperGraph(inputs5, conditions5)
          .toPlan()
      ).isSuccess
    )

    val (inputs6, conditions6, _, _) =
      ExtractRequiredProjectJoins
        .unapply(plan6.collectFirst { case s: BinaryJoin => s }.get)
        .get

    assert(
      Try(
        JoinHyperGraph(inputs6, conditions6)
          .toPlan()
      ).isFailure
    )

    val (inputs7, conditions7, _, _) =
      ExtractRequiredProjectJoins
        .unapply(plan7.collectFirst { case s: BinaryJoin => s }.get)
        .get

    assert(
      Try(
        JoinHyperGraph(inputs7, conditions7)
          .toPlan()
      ).isFailure
    )
  }

}
