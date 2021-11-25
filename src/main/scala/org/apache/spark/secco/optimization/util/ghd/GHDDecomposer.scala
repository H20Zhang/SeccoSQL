package org.apache.spark.secco.optimization.util.ghd

import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.optimization.util.{Edge, Graph, HasID, Node}

import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.spark.secco.util.`extension`.SeqExtension

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// Relation Graph.
// We assume RelationGraph is undirected graph, thus the nodes in edges won't distinguish directions.

/** A node in the [[GHDHyperGraph]], which actually represents an attribute.
  *
  * @param id id of the node
  * @param attr attributes of the node
  */
case class GHDHyperGraphNode(id: Int, attr: Attribute) extends Node

object GHDHyperGraphNode {

  private var counter = 0
  private val attrToId = scala.collection.mutable.HashMap[Attribute, Int]()

  def apply(attr: Attribute): GHDHyperGraphNode = {
    attrToId.get(attr) match {
      case Some(id) => GHDHyperGraphNode(id, attr)
      case None =>
        counter += 1; attrToId(attr) = counter;
        GHDHyperGraphNode(counter, attr)
    }
  }
}

/** A edge in the [[GHDHyperGraph]], which actually represents a relation.
  *
  * @param attrs schemas of the relation.
  */
case class GHDHyperGraphEdge(attrs: Set[Attribute])
    extends Edge[GHDHyperGraphNode] {
  override val nodes: Array[GHDHyperGraphNode] =
    attrs.map(attr => GHDHyperGraphNode(attr)).toArray
}

/** A graph that represents a join query, where each node is an attribute and each edge is an relation.
  * @param id id of the graph
  * @param nodes nodes of the graph
  * @param edges edges of the graph
  */
case class GHDHyperGraph(
    id: Int,
    override val nodes: Array[GHDHyperGraphNode],
    override val edges: Array[GHDHyperGraphEdge]
) extends Graph(nodes, edges)
    with HasID {

  override def nodeInducedSubgraph(
      nodes: Array[GHDHyperGraphNode]
  ): GHDHyperGraph = {
    val g = super.nodeInducedSubgraph(nodes)
    GHDHyperGraph(g.nodes, g.edges)
  }

  override def toString: String = {
    s"id:${id}\n" + super.toString
  }
}

object GHDHyperGraph {
  private var counter = 999
  def apply(
      nodes: Array[GHDHyperGraphNode],
      edges: Array[GHDHyperGraphEdge]
  ): GHDHyperGraph = {
    counter += 1
    val graph = new GHDHyperGraph(counter, nodes, edges)
    graph
  }

  def apply(schemas: Array[Array[Attribute]]): GHDHyperGraph = {
    val nodes = schemas.flatten.distinct.map(GHDHyperGraphNode(_))
    val edges = schemas.map(f => GHDHyperGraphEdge(f.toSet))
    GHDHyperGraph(nodes, edges)
  }
}

//  HyperNodes that are isomoprhic are given the same id

/** A hypernode of the [[GHDHyperTree]], where contains join query represented in [[GHDHyperGraph]].
  *
  * @param g the join graph
  */
case class GHDHyperTreeNode(g: GHDHyperGraph) extends Node {

  val id = g.id

  /** Construct induced hyper-node according to the nodeset of current hypernode */
  def toInducedHyperNode(supG: GHDHyperGraph): GHDHyperTreeNode = {
    GHDHyperTreeNode(supG.nodeInducedSubgraph(g.nodes))
  }

  override def toString: String = {
    s"${g.id}[${g.nodes.mkString("[", ",", "]")},${g.edges.mkString("[", ",", "]")}]"
  }
}

/** A hyperedge of the [[GHDHyperTree]].
  *
  * @param src source node
  * @param dst destination node
  */
case class GHDHyperTreeEdge(src: GHDHyperTreeNode, dst: GHDHyperTreeNode)
    extends Edge[GHDHyperTreeNode] {
  override def nodes: Array[GHDHyperTreeNode] = Array(src, dst)
}

// We regard GHD as a special kinds of hypertree

/** A hypertree
  * @param nodes hypernodes
  * @param edges hyperedges
  */
case class GHDHyperTree(
    override val nodes: Array[GHDHyperTreeNode],
    override val edges: Array[GHDHyperTreeEdge]
) extends Graph(nodes, edges) {

  /** fractional tree width */
  lazy val fractionalHyperNodeWidth = nodes.map(_.g.width).max

  /** By adding one hypernode to existing hypertree with one edge connected,
    * we ensure the result graph is always a hypertree.
    *
    * @param newNode new hypernode to be added to the tree.
    * @return possible new [[GHDHyperTree]] with the given newNode added, if no such [[GHDHyperTree]] exists,
    *         an empty array is returned.
    */
  def addHyperNode(newNode: GHDHyperTreeNode): Array[GHDHyperTree] = {
    if (isEmpty()) {
      return Array(GHDHyperTree(nodes :+ newNode, edges))
    }

    var i = 0
    val end = nodes.size
    val hypertreeBuffer = ArrayBuffer[GHDHyperTree]()
    while (i < end) {
      val node = nodes(i)
      if (node.g.containAnyNodes(newNode.g.nodes)) {
        val newEdge = GHDHyperTreeEdge(node, newNode)
        val newTree = GHDHyperTree(nodes :+ newNode, edges :+ newEdge)
        if (newTree.isGHD()) {
          hypertreeBuffer += newTree
        }
      }
      i += 1
    }

    hypertreeBuffer.toArray
  }

  /** Determine whether current GHD satisfies running path property.
    *
    * Note that: We assume all tree are constructed using function `addHyperTreeNode`, which means
    * tree condition is automatically satisfied
    */
  def isGHD(): Boolean = {

    //  return the subgraph of the hypertree based on the running path induced subgraph
    def runningPathSubGraph(relationNode: GHDHyperGraphNode) = {
      val relevantHyperNodes = nodes.filter { hypernode =>
        hypernode.g.containNode(relationNode)
      }

      val g = nodeInducedSubgraph(relevantHyperNodes)
      g
    }

    val nodeSet = nodes.flatMap(hypernode => hypernode.g.nodes).distinct

    nodeSet.forall { nodeId =>
      runningPathSubGraph(nodeId).isWeaklyConnected()
    }
  }

}

/** A hypertree decomposer that decomposes a [[GHDHyperGraph]] into [[GHDHyperTree]] */
object GHDDecomposer {

  /** Find all GHD of a [[GHDHyperGraph]]
    *
    * @param g the relation graph
    * @return an [[Array]] of [[GHDHyperTree]]
    */
  def genAllGHDs(g: GHDHyperGraph): Array[GHDHyperTree] = {
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
  private def genAllGHDsByEnumeratingNode(g: GHDHyperGraph) = {

    val numEdges = g.edges.size
    val numNodes = g.nodes.size
    val potentialConnectedInducedSubgraphs =
      computeConnectedNodeInducedSubgraphs(g)

    var extendableTree = new Array[(GHDHyperTree, Array[GHDHyperGraphNode])](1)
    val GHDs = new ConcurrentLinkedQueue[GHDHyperTree]()
    extendableTree(0) = ((GHDHyperTree(Array(), Array()), Array()))

    while (!extendableTree.isEmpty) {

      val newExtendableTree =
        new ConcurrentLinkedQueue[(GHDHyperTree, Array[GHDHyperGraphNode])]()
      extendableTree
        .filter { case (hypertree, coveredNodes) =>
          if (coveredNodes.size == numNodes) {

            val coveredEdgeSets = mutable.HashSet[GHDHyperGraphEdge]()
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
      val buffer = ArrayBuffer[(GHDHyperTree, Array[GHDHyperGraphNode])]()
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
      basedGraph: GHDHyperGraph,
      hypertree: GHDHyperTree,
      coveredNodes: Array[GHDHyperGraphNode],
      connectedNodeInducedSubgraphs: Array[GHDHyperGraph]
  ): Array[(GHDHyperTreeNode, Array[GHDHyperGraphNode])] = {

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
        (GHDHyperTreeNode(g), newCoveredNodes)
      }
  }

  private def computeConnectedNodeInducedSubgraphs(
      basedGraph: GHDHyperGraph
  ) = {
    val potentialNodeSets: Array[Array[GHDHyperGraphNode]] =
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
