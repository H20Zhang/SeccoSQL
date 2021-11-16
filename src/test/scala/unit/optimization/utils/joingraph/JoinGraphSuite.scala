package unit.optimization.utils.joingraph

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.plan.{BinaryJoin, Inner}
import org.apache.spark.secco.optimization.support.ExtractRequiredProjectJoins
import org.apache.spark.secco.optimization.util.joingraph.{
  JoinGraph,
  JoinGraphConstructor
}
import util.SeccoFunSuite

import scala.util.Try

//TODO: add more testing
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
      joinGraph.toString == "nodes:{1:Relation[R1, Atomic],2:Relation[R2, Atomic],3:Relation[R3, Atomic],4:Relation[R4, Atomic]}" +
        "\nedges:{[2,1]:(R1.`a` = R2.`b`),[2,3]:(R2.`c` = R3.`c`),[4,3]:(R3.`c` = R4.`c`)}"
    )

    //test toPlan
    assert(
      joinGraph
        .toPlan()
        .toString
        .dropRight(1)
        == """BinaryJoin[Inner, (c#3 = c#4), Coupled]-> (b#2,c#3,a#0,b#1,c#6,d#7,c#4,d#5)
                                  |:- [0] BinaryJoin[Inner, (a#0 = b#2), Coupled]-> (b#2,c#3,a#0,b#1)
                                  |:  :- [0] Relation[R2, Atomic]-> (b#2,c#3)
                                  |:  +- [1] Relation[R1, Atomic]-> (a#0,b#1)
                                  |+- [1] BinaryJoin[Inner, (c#4 = c#6), Coupled]-> (c#6,d#7,c#4,d#5)
                                  |   :- [0] Relation[R4, Atomic]-> (c#6,d#7)
                                  |   +- [1] Relation[R3, Atomic]-> (c#4,d#5)""".stripMargin
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
            rankFunction = { case (lastMergedNodeOpt, edge) =>
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

}
