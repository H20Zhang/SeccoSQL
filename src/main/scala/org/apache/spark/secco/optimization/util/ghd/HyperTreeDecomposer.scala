package org.apache.spark.secco.optimization.util.ghd

import org.apache.spark.secco.optimization.util.{
  GHDHyperNode,
  GHDHyperTree,
  JoinHyperGraphEdge,
  JoinHyperGraph,
  JoinHyperGraphNode
}

import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.spark.secco.util.`extension`.SeqExtension

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** A hypertree decomposer that decomposes a [[JoinHyperGraph]] into [[GHDHyperTree]] */
object HyperTreeDecomposer {

  /** Find all GHD of a [[JoinHyperGraph]]
    *
    * @param g the relation graph
    * @return an [[Array]] of [[GHDHyperTree]]
    */
  def genAllGHDs(g: JoinHyperGraph): Array[GHDHyperTree] = {
    //    if (g.E().size > (g.V().size + 2)) {
    genAllGHDsByEnumeratingNode(g)
    //    } else {
    //      genAllGHDsByEnumeratingEdge(g)
    //    }
  }

  /** Find all GHD by enumerating nodes of [[GHDHyperTree]].
    *
    * @param g the relation graph
    * @return an [[Array]] of [[GHDHyperTree]]
    */
  private def genAllGHDsByEnumeratingNode(g: JoinHyperGraph) = {

    val numEdges = g.edges.size
    val numNodes = g.nodes.size
    val potentialConnectedInducedSubgraphs =
      computeConnectedNodeInducedSubgraphs(g)

    var extendableTree = new Array[(GHDHyperTree, Array[JoinHyperGraphNode])](1)
    val GHDs = new ConcurrentLinkedQueue[GHDHyperTree]()
    extendableTree(0) = ((GHDHyperTree(Array(), Array()), Array()))

    while (!extendableTree.isEmpty) {

      val newExtendableTree =
        new ConcurrentLinkedQueue[(GHDHyperTree, Array[JoinHyperGraphNode])]()
      extendableTree
        .filter { case (hypertree, coveredNodes) =>
          if (coveredNodes.size == numNodes) {

            val coveredEdgeSets = mutable.HashSet[JoinHyperGraphEdge]()
            hypertree.nodes.foreach { hv =>
              val E = hv.g.edges
              E.foreach(e => coveredEdgeSets.add(e))
            }

            if (coveredEdgeSets.size == numEdges) {
              GHDs.add(hypertree)
            }

            false
          } else {
            true
          }
        }
        .foreach { case (hypertree, coveredNodes) =>
          val newNodes =
            genPotentialHyperNodesByEnumeratingNode(
              g,
              hypertree,
              coveredNodes,
              potentialConnectedInducedSubgraphs
            )

          newNodes.foreach { case (hyperNode, coveredNodes) =>
            val hypertrees = hypertree.addHyperNode(hyperNode)
            hypertrees.foreach { hypertree =>
              newExtendableTree.add((hypertree, coveredNodes))
            }
          }
        }

      newExtendableTree.toArray
      val it = newExtendableTree.iterator()
      val buffer = ArrayBuffer[(GHDHyperTree, Array[JoinHyperGraphNode])]()
      while (it.hasNext) {
        buffer += it.next()
      }
      extendableTree = buffer.toArray

    }

    val it = GHDs.iterator()
    val buffer = ArrayBuffer[GHDHyperTree]()
    while (it.hasNext) {
      buffer += it.next()
    }
    buffer.toArray
  }

  //  Find the hyper-nodes, the graph inside a hyper-node must be connected and node induced graph
  private def genPotentialHyperNodesByEnumeratingNode(
      basedGraph: JoinHyperGraph,
      hypertree: GHDHyperTree,
      coveredNodes: Array[JoinHyperGraphNode],
      connectedNodeInducedSubgraphs: Array[JoinHyperGraph]
  ): Array[(GHDHyperNode, Array[JoinHyperGraphNode])] = {

    var potentialGraphs = connectedNodeInducedSubgraphs

    if (coveredNodes.isEmpty) {
      potentialGraphs = potentialGraphs.filter { g =>
        g.nodes.contains(basedGraph.nodes.head)
      }
    }

    potentialGraphs = potentialGraphs
      .filter { g =>
        hypertree.nodes
          .forall(n =>
            g.nodes.diff(n.g.nodes).nonEmpty && n.g.nodes.diff(g.nodes).nonEmpty
          )
      }

    potentialGraphs
      .map { g =>
        val newCoveredNodes = (coveredNodes ++ g.nodes).distinct
        (GHDHyperNode(g), newCoveredNodes)
      }
  }

  private def computeConnectedNodeInducedSubgraphs(
      basedGraph: JoinHyperGraph
  ) = {
    val potentialNodeSets: Array[Array[JoinHyperGraphNode]] =
      SeqExtension.subset(basedGraph.nodes).map(_.toArray).toArray

    val potentialGraphs = potentialNodeSets
      .map(nodeSet => basedGraph.nodeInducedSubgraph(nodeSet))
      .filter { g =>
        g.isWeaklyConnected()
      }

    potentialGraphs
  }

  //  //  Find all GHD decomposition
  //  def genAllGHDsByEnumeratingEdge(g: RelationGraph) = {
  //
  //    var extendableTree = ArrayBuffer[(HyperTree, Array[RelationEdge])]()
  //    val GHDs = ArrayBuffer[HyperTree]()
  //    extendableTree += ((HyperTree(Array(), Array()), g.edges))
  //    var counter = 0
  //
  //    while (!extendableTree.isEmpty) {
  //
  //      counter += 1
  //
  //      val (hypertree, remainingEdges) = extendableTree.last
  //      extendableTree = extendableTree.dropRight(1)
  //
  //      if (remainingEdges.isEmpty) {
  //        GHDs += hypertree
  //      } else {
  //        val newNodes =
  //          genPotentialHyperNodesByEnumeratingEdge(g, hypertree, remainingEdges)
  //        var i = 0
  //        val end = newNodes.size
  //        while (i < end) {
  //          val (hyperNode, remainingEdges) = newNodes(i)
  //          val hypertrees = hypertree.addHyperNode(hyperNode)
  //          hypertrees.foreach { hypertree =>
  //            extendableTree += ((hypertree, remainingEdges))
  //          }
  //          i += 1
  //        }
  //      }
  //    }
  //    GHDs.toArray
  //  }

  //  //  Find the hyper-nodes, the graph inside a hyper-node must be connected and node induced graph
  //  private def genPotentialHyperNodesByEnumeratingEdge(
  //      basedGraph: RelationGraph,
  //      hypertree: HyperTree,
  //      remainEdges: Array[RelationEdge]
  //  ): Array[(HyperNode, Array[RelationEdge])] = {
  //
  //    var potentialEdgeSets: Array[Array[RelationEdge]] = null
  //    if (remainEdges.size == basedGraph.edges.size) {
  //      potentialEdgeSets = SeqUtil.subset(remainEdges).map(_.toArray).toArray
  //      potentialEdgeSets =
  //        potentialEdgeSets.filter(arr => arr.contains(basedGraph.edges.head))
  //    } else {
  //      potentialEdgeSets = SeqUtil.subset(remainEdges).map(_.toArray).toArray
  //    }
  //
  //    var potentialGraphs = potentialEdgeSets.par
  //      .map(edgeSet =>
  //        RelationGraph(edgeSet.flatMap(f => f.attrs).distinct, edgeSet)
  //      )
  //
  //    //    hypernodes must be connected and
  //    //    previous node in hypertree must not contain new node as subgraph
  //    potentialGraphs = potentialGraphs
  //      .filter { g =>
  //        val inducedG = g.toInducedGraph(basedGraph)
  //        g.isConnected() && hypertree.nodes
  //          .forall(n =>
  //            !inducedG.containSubgraph(n.g) && !n.g
  //              .containSubgraph(inducedG)
  //          )
  //      }
  //
  //    potentialGraphs
  //      .map { g =>
  //        val inducedG = g.toInducedGraph(basedGraph)
  //        val inducedEdges = inducedG.E()
  //        val remainingEdges = remainEdges.diff(g.E())
  //        (
  //          HyperNode(
  //            RelationGraph(inducedEdges.flatMap(_.attrs).distinct, inducedEdges)
  //          ),
  //          remainingEdges
  //        )
  //      }
  //  }.toArray

}
