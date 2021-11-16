package org.apache.spark.secco.optimization.util

import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.optimization.util.ghd.WidthCalculator

import scala.collection.mutable.ArrayBuffer

/** A trait for classes that have id. */
trait HasID {
  def id: Int
}

/** A trait for the node of the graph. */
trait Node extends HasID {
  def id: Int

  def simpleString: String = s"$id"

  def verboseString: String = simpleString

  override def toString: String = verboseString
}

/** A trait for the edge of the graph. */
trait Edge[V <: Node] {
  def nodes: Array[V]
  lazy val nodeSet: Set[V] = Set(nodes: _*)
  lazy val nodeIDSet: Set[Int] = nodeSet.map(_.id)

  def simpleString: String =
    nodes.map(f => f.simpleString).mkString("[", ",", "]")

  def verboseString: String = simpleString

  override def toString: String = verboseString
}

/** A graph that with nodes and edges.
  * @param nodes nodes of the graph
  * @param edges edges of the graph
  * @tparam V node type
  * @tparam E edge type
  */
class Graph[V <: Node, E <: Edge[V]](val nodes: Array[V], val edges: Array[E]) {

  lazy val nodeSet = nodes.toSet
  lazy val edgeSet = edges.toSet

  /** Return the node induced subgraph.
    * @param nodes nodes of the subgraph
    * @return a node induced subgraph
    */
  def nodeInducedSubgraph(nodes: Array[V]): Graph[V, E] = {

    val nodeSet = nodes.toSet
    val inducedEdges = edges.filter { edge =>
      edge.nodeSet.subsetOf(nodeSet)
    }

    Graph(nodes, inducedEdges)
  }

  /** test if the graph is empty. */
  def isEmpty(): Boolean = {
    nodes.isEmpty && edges.isEmpty
  }

  /** test if the undirected version of the graph is connected */
  def isWeaklyConnected(): Boolean = {

    import scala.collection.mutable

    val visited = mutable.Set[V]()
    val next = mutable.Set[V]()

    //find the next for first node
    next += nodes.head

    while (next.nonEmpty) {
      val cur = next.head
      visited += cur
      next -= cur
      next ++= findNext(cur)
    }

    def findNext(node: V): mutable.HashSet[V] = {
      val nextSet = mutable.HashSet[V]()
      var i = 0
      val end = edges.size
      while (i < end) {
        val e = edges(i)
        if (e.nodeSet.contains(node)) {
          e.nodes.foreach { nextNode =>
            if (!visited.contains(nextNode)) {
              nextSet += nextNode
            }
          }
        }
        i += 1
      }

      nextSet
    }

    nodes.toSet.diff(visited).isEmpty
  }

  /** Test if the given node exists in this graph.
    * @param node the node to test
    * @return whether the given node is in the graph
    */
  def containNode(node: V): Boolean = nodeSet.contains(node)

  /** Test if the given edge exists in this graph.
    * @param edge the node to test
    * @return whether the given edge is in the graph
    */
  def containEdge(edge: E): Boolean = edgeSet.contains(edge)

  /** Test if all given nodes exists in this graph.
    * @param nodes all nodes to test
    * @return whether all given nodes is in the graph
    */
  def containAnyNodes(nodes: Array[V]): Boolean = nodes.exists(containNode)

  /** Test if the given subgraph is contained in this graph.
    * @param subgraph the subgraph to test
    * @return whether the given subgraph is contained in the graph
    */
  def containSubgraph(subgraph: Graph[V, E]): Boolean = {
    subgraph.nodes.forall(containNode) && subgraph.edges.forall(containEdge)
  }

  /** Width (fractional edge cover number) of the graph. */
  lazy val width: Double = WidthCalculator.width(this)

  override def toString: String = {
    s"V:${nodes.mkString("[", ",", "]")}\nE:${edges.mkString("[", ",", "]")}"
  }
}

object Graph {
  def apply[V <: Node, E <: Edge[V]](
      nodes: Array[V],
      edges: Array[E]
  ): Graph[V, E] = new Graph(nodes, edges)
}

// Relation Graph.
// We assume RelationGraph is undirected graph, thus the nodes in edges won't distinguish directions.

/** A node in the [[JoinHyperGraph]], which actually represents an attribute.
  *
  * @param id id of the node
  * @param attr attributes of the node
  */
case class JoinHyperGraphNode(id: Int, attr: Attribute) extends Node

object JoinHyperGraphNode {

  private var counter = 0
  private val attrToId = scala.collection.mutable.HashMap[Attribute, Int]()

  def apply(attr: Attribute): JoinHyperGraphNode = {
    attrToId.get(attr) match {
      case Some(id) => JoinHyperGraphNode(id, attr)
      case None =>
        counter += 1; attrToId(attr) = counter;
        JoinHyperGraphNode(counter, attr)
    }
  }
}

/** A edge in the [[JoinHyperGraph]], which actually represents a relation.
  *
  * @param attrs schemas of the relation.
  */
case class JoinHyperGraphEdge(attrs: Set[Attribute])
    extends Edge[JoinHyperGraphNode] {
  override val nodes: Array[JoinHyperGraphNode] =
    attrs.map(attr => JoinHyperGraphNode(attr)).toArray
}

/** A graph that represents a join query, where each node is an attribute and each edge is an relation.
  * @param id id of the graph
  * @param nodes nodes of the graph
  * @param edges edges of the graph
  */
case class JoinHyperGraph(
    id: Int,
    override val nodes: Array[JoinHyperGraphNode],
    override val edges: Array[JoinHyperGraphEdge]
) extends Graph(nodes, edges)
    with HasID {

  override def nodeInducedSubgraph(
      nodes: Array[JoinHyperGraphNode]
  ): JoinHyperGraph = {
    val g = super.nodeInducedSubgraph(nodes)
    JoinHyperGraph(g.nodes, g.edges)
  }

  override def toString: String = {
    s"id:${id}\n" + super.toString
  }
}

object JoinHyperGraph {
  private var counter = 999
  def apply(
      nodes: Array[JoinHyperGraphNode],
      edges: Array[JoinHyperGraphEdge]
  ): JoinHyperGraph = {
    counter += 1
    val graph = new JoinHyperGraph(counter, nodes, edges)
    graph
  }

  def apply(schemas: Array[Array[Attribute]]): JoinHyperGraph = {
    val nodes = schemas.flatten.distinct.map(JoinHyperGraphNode(_))
    val edges = schemas.map(f => JoinHyperGraphEdge(f.toSet))
    JoinHyperGraph(nodes, edges)
  }
}

//  HyperNodes that are isomoprhic are given the same id

/** A hypernode of the [[GHDHyperTree]], where contains join query represented in [[JoinHyperGraph]].
  *
  * @param g the join graph
  */
case class GHDHyperNode(g: JoinHyperGraph) extends Node {

  val id = g.id

  /** Construct induced hyper-node according to the nodeset of current hypernode */
  def toInducedHyperNode(supG: JoinHyperGraph): GHDHyperNode = {
    GHDHyperNode(supG.nodeInducedSubgraph(g.nodes))
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
case class GHDHyperEdge(src: GHDHyperNode, dst: GHDHyperNode)
    extends Edge[GHDHyperNode] {
  override def nodes: Array[GHDHyperNode] = Array(src, dst)
}

// We regard GHD as a special kinds of hypertree

/** A hypertree
  * @param nodes hypernodes
  * @param edges hyperedges
  */
case class GHDHyperTree(
    override val nodes: Array[GHDHyperNode],
    override val edges: Array[GHDHyperEdge]
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
  def addHyperNode(newNode: GHDHyperNode): Array[GHDHyperTree] = {
    if (isEmpty()) {
      return Array(GHDHyperTree(nodes :+ newNode, edges))
    }

    var i = 0
    val end = nodes.size
    val hypertreeBuffer = ArrayBuffer[GHDHyperTree]()
    while (i < end) {
      val node = nodes(i)
      if (node.g.containAnyNodes(newNode.g.nodes)) {
        val newEdge = GHDHyperEdge(node, newNode)
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
    def runningPathSubGraph(relationNode: JoinHyperGraphNode) = {
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
