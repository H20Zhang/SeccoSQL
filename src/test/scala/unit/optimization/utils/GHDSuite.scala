//package unit.optimization.utils
//
//import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
//import org.apache.spark.secco.optimization.util.ghd._
//import util.{SeccoFunSuite, UnitTestTag}
//
//import scala.reflect.ClassTag
//
//class GHDSuite extends SeccoFunSuite {
//
//  test("Graph", UnitTestTag) {
//
//    case class BinaryEdge[V <: Node](src: V, dst: V)(implicit tag: ClassTag[V])
//        extends Edge[V] {
//      override def nodes: Array[V] = Array(src, dst)
//    }
//
//    case class SimpleNode(id: Int) extends Node {}
//
//    val nodes =
//      Array(SimpleNode(1), SimpleNode(2), SimpleNode(3), SimpleNode(4))
//    val edges1 = Array(
//      BinaryEdge(SimpleNode(1), SimpleNode(2)),
//      BinaryEdge(SimpleNode(2), SimpleNode(3)),
//      BinaryEdge(SimpleNode(3), SimpleNode(4)),
//      BinaryEdge(SimpleNode(4), SimpleNode(1))
//    )
//
//    val edges2 = Array(BinaryEdge(SimpleNode(1), SimpleNode(2)))
//
//    val g1 = Graph(nodes, edges1)
//    val g2 = Graph(nodes, edges2)
//
//    assert(g1.nodes == nodes && g1.edges == edges1)
//    assert(g2.nodes == nodes && g2.edges == edges2)
//
//    assert(!g1.isEmpty())
//    assert(g1.isWeaklyConnected())
//    assert(g1.containAnyNodes(nodes))
//    assert(nodes.forall(n => g1.containNode(n)))
//    assert(edges1.forall(e => g1.containEdge(e)))
//
//    assert(!g2.isEmpty())
//    assert(!g2.isWeaklyConnected())
//    assert(g2.containAnyNodes(nodes))
//    assert(nodes.forall(n => g2.containNode(n)))
//    assert(edges1.drop(1).forall(e => !g2.containEdge(e)))
//
//    assert(g1.width == 2.0)
//    assert(
//      g1.nodeInducedSubgraph(nodes.dropRight(1)).nodes.toSeq == nodes
//        .dropRight(
//          1
//        )
//        .toSeq && g1
//        .nodeInducedSubgraph(nodes.dropRight(1))
//        .edges
//        .toSeq == edges1
//        .dropRight(
//          2
//        )
//        .toSeq
//    )
//  }
//
//  test("RelationGraph", UnitTestTag) {
//    val nodes =
//      Array(
//        RelationNode("A"),
//        RelationNode("B"),
//        RelationNode("C"),
//        RelationNode("D")
//      )
//    val edges1 = Array(
//      RelationEdge(Set("A", "B", "C")),
//      RelationEdge(Set("A", "B")),
//      RelationEdge(Set("B", "C")),
//      RelationEdge(Set("C", "D")),
//      RelationEdge(Set("D", "A"))
//    )
//
//    val edges2 = Array(RelationEdge(Set("A", "B", "C")))
//
//    val g1 = RelationGraph(nodes, edges1)
//    val g2 = RelationGraph(nodes, edges2)
//
//    assert(g1.nodes == nodes && g1.edges == edges1)
//    assert(g2.nodes == nodes && g2.edges == edges2)
//
//    assert(!g1.isEmpty())
//    assert(g1.isWeaklyConnected())
//    assert(g1.containAnyNodes(nodes))
//    assert(nodes.forall(n => g1.containNode(n)))
//    assert(edges1.forall(e => g1.containEdge(e)))
//
//    assert(!g2.isEmpty())
//    assert(!g2.isWeaklyConnected())
//    assert(g2.containAnyNodes(nodes))
//    assert(nodes.forall(n => g2.containNode(n)))
//    assert(edges1.drop(1).forall(e => !g2.containEdge(e)))
//
//    assert(g1.width == 2.0)
//    assert(g1.containEdge(RelationEdge(Set("B", "A", "C"))))
//  }
//
//  test("GHDDecomposer", UnitTestTag) {
//    //register columns and schemas
//    val colA = CatalogColumn("A")
//    val colB = CatalogColumn("B")
//    val colC = CatalogColumn("C")
//    val colD = CatalogColumn("D")
//    val colE = CatalogColumn("E")
//    val colF = CatalogColumn("F")
//
//    val colG = CatalogColumn("G")
//    val colH = CatalogColumn("H")
//
//    val R1 = CatalogTable("R1", Seq(colA, colB))
//    val R2 = CatalogTable("R2", Seq(colB, colC))
//    val R3 = CatalogTable("R3", Seq(colC, colD))
//    val R4 = CatalogTable("R4", Seq(colD, colA))
//    val R5 = CatalogTable("R5", Seq(colA, colE))
//    val R6 = CatalogTable("R6", Seq(colD, colE))
//    val R7 = CatalogTable("R7", Seq(colA, colC))
//    val R8 = CatalogTable("R8", Seq(colB, colE))
//    val R9 = CatalogTable("R9", Seq(colC, colE))
//    val R10 = CatalogTable("R10", Seq(colC, colF))
//    val R11 = CatalogTable("R11", Seq(colE, colF))
//
//    val R12 = CatalogTable("R12", Seq(colA, colG))
//    val R13 = CatalogTable("R13", Seq(colG, colF))
//
//    //square
//    val q1 = R1 :: R2 :: R3 :: R4 :: Nil
//    //house
//    val q2 = R1 :: R2 :: R3 :: R4 :: R5 :: R6 :: Nil
//    //threeTriangle
//    val q3 = R1 :: R2 :: R3 :: R4 :: R5 :: R6 :: R7 :: Nil
//    //solarSquare
//    val q4 = R1 :: R2 :: R3 :: R4 :: R5 :: R6 :: R8 :: R9 :: Nil
//    //twoSquare
//    val q5 = R1 :: R2 :: R3 :: R4 :: R6 :: R10 :: R11 :: Nil
//
//    val decomposer = GHDDecomposer.defaultDecomposer
//
//    assert(decomposer.decomposeTree(q1).head.fhtw == 2.0)
//    assert(decomposer.decomposeTree(q2).head.fhtw == 2.0)
//    assert(decomposer.decomposeTree(q3).head.fhtw == 1.5)
//    assert(decomposer.decomposeTree(q4).head.fhtw == 2.0)
//    assert(decomposer.decomposeTree(q5).head.fhtw == 2.0)
//  }
//}
