package unit.optimization.utils.joingraph

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.plan.{BinaryJoin, Inner}
import org.apache.spark.secco.optimization.support.ExtractRequiredProjectJoins
import org.apache.spark.secco.optimization.util.joingraph.{JoinGraph}
import util.SeccoFunSuite

import scala.util.Try

class JoinGraphSuite extends SeccoFunSuite {

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
      CatalogTable("R4", CatalogColumn("c") :: CatalogColumn("d") :: Nil)
    )
    catalog.createTable(
      CatalogTable("R5", CatalogColumn("d") :: CatalogColumn("e") :: Nil)
    )
  }

  test("join_graph_operation") {
    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")
    val R4 = seccoSession.table("R4")

    val plan1 = R1
      .join(R2, "R1.a = R2.b")
      .join(R3, "R2.c = R3.c")
      .join(R4, "R3.c = R4.c")
      .queryExecution
      .analyzedPlan

    val (inputs, conditions, projectionList, mode) =
      ExtractRequiredProjectJoins.unapply(plan1).get

    //print join graph
    val joinGraph = JoinGraph(inputs, conditions)

    assert(
      joinGraph.toString
        == """nodes:{1:Relation[R1, Atomic],2:Relation[R2, Atomic],3:Relation[R3, Atomic],4:Relation[R4, Atomic]}
             |edges:{[1,2]:(R1.`a` = R2.`b`),[2,3]:(R2.`c` = R3.`c`),[3,4]:(R3.`c` = R4.`c`)}""".stripMargin
    )

    //test toPlan
    assert(
      joinGraph
        .toPlan()
        .toString
        .dropRight(1)
        == """BinaryJoin[Inner, (c#4 = c#6), Coupled]-> (b#2,c#3,a#0,b#1,c#4,d#5,c#6,d#7)
             |:- [0] BinaryJoin[Inner, (c#3 = c#4), Coupled]-> (b#2,c#3,a#0,b#1,c#4,d#5)
             |:  :- [0] BinaryJoin[Inner, (a#0 = b#2), Coupled]-> (b#2,c#3,a#0,b#1)
             |:  :  :- [0] Relation[R2, Atomic]-> (b#2,c#3)
             |:  :  +- [1] Relation[R1, Atomic]-> (a#0,b#1)
             |:  +- [1] Relation[R3, Atomic]-> (c#4,d#5)
             |+- [1] Relation[R4, Atomic]-> (c#6,d#7)""".stripMargin
    )

    //test mergeNode
    val edgeToMerge = joinGraph.edges.head
    assert(
      Try(joinGraph.mergeNode(edgeToMerge.lNode, edgeToMerge.rNode)).isSuccess
    )

    //test mergeNodes, use connected traversal to merge nodes, result in left-deep join tree

    assert(
      Try(
        joinGraph
          .mergeNodes(
            scoreFunction = { case (lastMergedNodeOpt, edge) =>
              if (lastMergedNodeOpt.isEmpty) {
                (0, None)
              } else {
                val lastMergedNode = lastMergedNodeOpt.get

                if (
                  edge.lNode == lastMergedNode || edge.rNode == lastMergedNode
                ) {
                  (1.0, None)
                } else {
                  (0.0, None)
                }
              }
            },
            mergeFunction = { case (l, r, condition, token) =>
              if (l.isInstanceOf[BinaryJoin]) {
                BinaryJoin(l, r, Inner, condition)
              } else {
                BinaryJoin(r, l, Inner, condition)
              }
            }
          )
          .nodes
          .head
          .plan
      ).isSuccess
    )
  }

  test("join_graph_wrt_complex_query") {
    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")
    val R4 = seccoSession.table("R4")
    val R5 = seccoSession.table("R5")

    // join with duplicate relations
    val plan1 = R1
      .join(R2, "R1.a = R2.b")
      .join(R3, "R2.c = R3.c")
      .join(R4, "R3.c = R4.c")
      .join(R5, "R4.d = R5.d")
      .queryExecution
      .analyzedPlan

    // join with multiple condition between two relation
    val plan2 = R1
      .join(R2, "R1.a = R2.b and R1.a = R2.c")
      .join(R3, "R2.c = R3.c and R2.b = R3.d")
      .join(R4, "R3.c = R4.c and R3.d = R4.d")
      .join(R5, "R4.d = R5.d and R4.c = R5.e")
      .queryExecution
      .analyzedPlan

    // join with condition on itself (should throw exception during testing)
    val plan3 = R1
      .join(R2, "R1.a = R2.b and R1.a = R1.b")
      .join(R3, "R2.c = R3.c and R2.b = R3.d")
      .queryExecution
      .analyzedPlan

    // join with theta-join condition (should throw exception during testing)
    val plan4 = R1
      .join(R2, "R1.a = R2.b and R1.a = R1.b")
      .join(R3, "R2.c = R3.c and R2.b < R3.d")
      .queryExecution
      .analyzedPlan

    // join with cartesian product
    val plan5 = R1
      .join(R4)
      .join(R2, "R1.a = R2.b")
      .join(R3, "R2.c = R3.c")
      .queryExecution
      .analyzedPlan

    val (inputs1, conditions1, _, _) =
      ExtractRequiredProjectJoins.unapply(plan1).get
    val joinGraph1 = JoinGraph(inputs1, conditions1)

    assert(
      joinGraph1
        .toPlan()
        .toString
        .dropRight(1) == """BinaryJoin[Inner, (d#15 = d#16), Coupled]-> (b#10,c#11,a#8,b#9,c#12,d#13,c#14,d#15,d#16,e#17)
                                 |:- [0] BinaryJoin[Inner, (c#12 = c#14), Coupled]-> (b#10,c#11,a#8,b#9,c#12,d#13,c#14,d#15)
                                 |:  :- [0] BinaryJoin[Inner, (c#11 = c#12), Coupled]-> (b#10,c#11,a#8,b#9,c#12,d#13)
                                 |:  :  :- [0] BinaryJoin[Inner, (a#8 = b#10), Coupled]-> (b#10,c#11,a#8,b#9)
                                 |:  :  :  :- [0] Relation[R2, Atomic]-> (b#10,c#11)
                                 |:  :  :  +- [1] Relation[R1, Atomic]-> (a#8,b#9)
                                 |:  :  +- [1] Relation[R3, Atomic]-> (c#12,d#13)
                                 |:  +- [1] Relation[R4, Atomic]-> (c#14,d#15)
                                 |+- [1] Relation[R5, Atomic]-> (d#16,e#17)""".stripMargin
    )

    val (inputs2, conditions2, _, _) =
      ExtractRequiredProjectJoins.unapply(plan2).get
    val joinGraph2 = JoinGraph(inputs2, conditions2)
    assert(
      joinGraph2.toPlan().toString.dropRight(1) == """BinaryJoin[Inner, ((d#15 = d#16) && (c#14 = e#17)), Coupled]-> (b#10,c#11,a#8,b#9,c#12,d#13,c#14,d#15,d#16,e#17)
                                             |:- [0] BinaryJoin[Inner, ((c#12 = c#14) && (d#13 = d#15)), Coupled]-> (b#10,c#11,a#8,b#9,c#12,d#13,c#14,d#15)
                                             |:  :- [0] BinaryJoin[Inner, ((c#11 = c#12) && (b#10 = d#13)), Coupled]-> (b#10,c#11,a#8,b#9,c#12,d#13)
                                             |:  :  :- [0] BinaryJoin[Inner, ((a#8 = b#10) && (a#8 = c#11)), Coupled]-> (b#10,c#11,a#8,b#9)
                                             |:  :  :  :- [0] Relation[R2, Atomic]-> (b#10,c#11)
                                             |:  :  :  +- [1] Relation[R1, Atomic]-> (a#8,b#9)
                                             |:  :  +- [1] Relation[R3, Atomic]-> (c#12,d#13)
                                             |:  +- [1] Relation[R4, Atomic]-> (c#14,d#15)
                                             |+- [1] Relation[R5, Atomic]-> (d#16,e#17)""".stripMargin
    )

    val (inputs3, conditions3, _, _) =
      ExtractRequiredProjectJoins.unapply(plan3).get
    assert(Try(JoinGraph(inputs3, conditions3)).isFailure)

    val (inputs4, conditions4, _, _) =
      ExtractRequiredProjectJoins.unapply(plan4).get
    assert(Try(JoinGraph(inputs4, conditions4)).isFailure)

    val (inputs5, conditions5, _, _) =
      ExtractRequiredProjectJoins.unapply(plan5).get
    val joinGraph5 = JoinGraph(inputs5, conditions5)
    assert(
      joinGraph5.toPlan().toString.dropRight(1) == """BinaryJoin[Inner, Coupled]-> (b#10,c#11,a#8,b#9,c#12,d#13,c#14,d#15)
                                                           |:- [0] BinaryJoin[Inner, (c#11 = c#12), Coupled]-> (b#10,c#11,a#8,b#9,c#12,d#13)
                                                           |:  :- [0] BinaryJoin[Inner, (a#8 = b#10), Coupled]-> (b#10,c#11,a#8,b#9)
                                                           |:  :  :- [0] Relation[R2, Atomic]-> (b#10,c#11)
                                                           |:  :  +- [1] Relation[R1, Atomic]-> (a#8,b#9)
                                                           |:  +- [1] Relation[R3, Atomic]-> (c#12,d#13)
                                                           |+- [1] Relation[R4, Atomic]-> (c#14,d#15)""".stripMargin
    )

  }

}
