package unit.optimization.utils.ghd

import org.apache.spark.secco.analysis.UnresolvedAttribute
import org.apache.spark.secco.optimization.util.ghd._
import org.apache.spark.secco.optimization.util._
import util.{SeccoFunSuite, UnitTestTag}

import scala.reflect.ClassTag

class GHDSuite extends SeccoFunSuite {

  test("Graph", UnitTestTag) {

    case class BinaryEdge[V <: Node](src: V, dst: V)(implicit tag: ClassTag[V])
        extends Edge[V] {
      override def nodes: Array[V] = Array(src, dst)
    }

    case class SimpleNode(id: Int) extends Node {}

    val nodes =
      Array(SimpleNode(1), SimpleNode(2), SimpleNode(3), SimpleNode(4))
    val edges1 = Array(
      BinaryEdge(SimpleNode(1), SimpleNode(2)),
      BinaryEdge(SimpleNode(2), SimpleNode(3)),
      BinaryEdge(SimpleNode(3), SimpleNode(4)),
      BinaryEdge(SimpleNode(4), SimpleNode(1))
    )

    val edges2 = Array(BinaryEdge(SimpleNode(1), SimpleNode(2)))

    val g1 = Graph(nodes, edges1)
    val g2 = Graph(nodes, edges2)

    assert(g1.nodes == nodes && g1.edges == edges1)
    assert(g2.nodes == nodes && g2.edges == edges2)

    assert(!g1.isEmpty())
    assert(g1.isWeaklyConnected())
    assert(g1.containAnyNodes(nodes))
    assert(nodes.forall(n => g1.containNode(n)))
    assert(edges1.forall(e => g1.containEdge(e)))

    assert(!g2.isEmpty())
    assert(!g2.isWeaklyConnected())
    assert(g2.containAnyNodes(nodes))
    assert(nodes.forall(n => g2.containNode(n)))
    assert(edges1.drop(1).forall(e => !g2.containEdge(e)))

    assert(g1.width == 2.0)
    assert(
      g1.nodeInducedSubgraph(nodes.dropRight(1)).nodes.toSeq == nodes
        .dropRight(
          1
        )
        .toSeq && g1
        .nodeInducedSubgraph(nodes.dropRight(1))
        .edges
        .toSeq == edges1
        .dropRight(
          2
        )
        .toSeq
    )
  }

  test("RelationGraph", UnitTestTag) {

    val AttrA = UnresolvedAttribute("A" :: Nil)
    val AttrB = UnresolvedAttribute("B" :: Nil)
    val AttrC = UnresolvedAttribute("C" :: Nil)
    val AttrD = UnresolvedAttribute("D" :: Nil)

    val nodes =
      Array(
        JoinHyperGraphNode(AttrA),
        JoinHyperGraphNode(AttrB),
        JoinHyperGraphNode(AttrC),
        JoinHyperGraphNode(AttrD)
      )
    val edges1 = Array(
      JoinHyperGraphEdge(Set(AttrA, AttrB, AttrC)),
      JoinHyperGraphEdge(Set(AttrA, AttrB)),
      JoinHyperGraphEdge(Set(AttrB, AttrC)),
      JoinHyperGraphEdge(Set(AttrC, AttrD)),
      JoinHyperGraphEdge(Set(AttrD, AttrA))
    )

    val edges2 = Array(JoinHyperGraphEdge(Set(AttrA, AttrB, AttrC)))

    val g1 = JoinHyperGraph(nodes, edges1)
    val g2 = JoinHyperGraph(nodes, edges2)

    assert(g1.nodes == nodes && g1.edges == edges1)
    assert(g2.nodes == nodes && g2.edges == edges2)

    assert(!g1.isEmpty())
    assert(g1.isWeaklyConnected())
    assert(g1.containAnyNodes(nodes))
    assert(nodes.forall(n => g1.containNode(n)))
    assert(edges1.forall(e => g1.containEdge(e)))

    assert(!g2.isEmpty())
    assert(!g2.isWeaklyConnected())
    assert(g2.containAnyNodes(nodes))
    assert(nodes.forall(n => g2.containNode(n)))
    assert(edges1.drop(1).forall(e => !g2.containEdge(e)))

    assert(g1.width == 2.0)
    assert(g1.containEdge(JoinHyperGraphEdge(Set(AttrB, AttrA, AttrC))))
  }

  test("GHDDecomposer", UnitTestTag) {
    //register columns and schemas

    val AttrA = UnresolvedAttribute("A" :: Nil)
    val AttrB = UnresolvedAttribute("B" :: Nil)
    val AttrC = UnresolvedAttribute("C" :: Nil)
    val AttrD = UnresolvedAttribute("D" :: Nil)
    val AttrE = UnresolvedAttribute("E" :: Nil)
    val AttrF = UnresolvedAttribute("F" :: Nil)

    val R1 = AttrA :: AttrB :: Nil
    val R2 = AttrB :: AttrC :: Nil
    val R3 = AttrC :: AttrD :: Nil
    val R4 = AttrD :: AttrA :: Nil
    val R5 = AttrA :: AttrE :: Nil
    val R6 = AttrD :: AttrE :: Nil
    val R7 = AttrA :: AttrC :: Nil
    val R8 = AttrB :: AttrE :: Nil
    val R9 = AttrC :: AttrE :: Nil
    val R10 = AttrC :: AttrF :: Nil
    val R11 = AttrE :: AttrF :: Nil

    //square
    val q1 = R1 :: R2 :: R3 :: R4 :: Nil
    //house
    val q2 = R1 :: R2 :: R3 :: R4 :: R5 :: R6 :: Nil
    //threeTriangle
    val q3 = R1 :: R2 :: R3 :: R4 :: R5 :: R6 :: R7 :: Nil
    //solarSquare
    val q4 = R1 :: R2 :: R3 :: R4 :: R5 :: R6 :: R8 :: R9 :: Nil
    //twoSquare
    val q5 = R1 :: R2 :: R3 :: R4 :: R6 :: R10 :: R11 :: Nil

    assert(GHDDecomposer.decomposeTree(q1).head.fhtw == 2.0)
    assert(GHDDecomposer.decomposeTree(q2).head.fhtw == 2.0)
    assert(GHDDecomposer.decomposeTree(q3).head.fhtw == 1.5)
    assert(GHDDecomposer.decomposeTree(q4).head.fhtw == 2.0)
    assert(GHDDecomposer.decomposeTree(q5).head.fhtw == 2.0)
  }
}
