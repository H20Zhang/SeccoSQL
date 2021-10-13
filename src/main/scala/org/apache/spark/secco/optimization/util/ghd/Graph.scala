package org.apache.spark.secco.optimization.util.ghd

import org.apache.spark.secco.expression.Attribute

import scala.collection.mutable.ArrayBuffer

/** A trait for classes that have id. */
trait HasID {
  def id: Int
}

/** A trait for the node of the graph. */
trait Node extends HasID {
  def id: Int

  override def toString: String = {
    s"$id"
  }
}

/** A trait for the edge of the graph. */
trait Edge[V <: Node] {
  def nodes: Array[V]
  lazy val nodeSet: Set[V] = Set(nodes: _*)
  lazy val nodeIDSet: Set[Int] = nodeSet.map(_.id)

  override def toString: String = {
    nodes.map(f => f.id).mkString("[", ",", "]")
  }
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

/** A node in the [[RelationGraph]], which actually represents an attribute.
  * @param id id of the node
  * @param attr attributes of the node
  */
case class RelationNode(id: Int, attr: Attribute) extends Node

object RelationNode {

  private var counter = 0
  private val attrToId = scala.collection.mutable.HashMap[Attribute, Int]()

  def apply(attr: Attribute): RelationNode = {
    attrToId.get(attr) match {
      case Some(id) => RelationNode(id, attr)
      case None =>
        counter += 1; attrToId(attr) = counter; RelationNode(counter, attr)
    }
  }
}

/** A edge in the [[RelationGraph]], which actually represents a relation.
  * @param attrs schemas of the relation.
  */
case class RelationEdge(attrs: Set[Attribute]) extends Edge[RelationNode] {
  override val nodes: Array[RelationNode] =
    attrs.map(attr => RelationNode(attr)).toArray
}

/** A graph that represents a join query, where each node is an attribute and each edge is an relation.
  * @param id id of the graph
  * @param nodes nodes of the graph
  * @param edges edges of the graph
  */
case class RelationGraph(
    id: Int,
    override val nodes: Array[RelationNode],
    override val edges: Array[RelationEdge]
) extends Graph(nodes, edges)
    with HasID {

  override def nodeInducedSubgraph(
      nodes: Array[RelationNode]
  ): RelationGraph = {
    val g = super.nodeInducedSubgraph(nodes)
    RelationGraph(g.nodes, g.edges)
  }

  override def toString: String = {
    s"id:${id}\n" + super.toString
  }
}

object RelationGraph {
  private var counter = 999
  def apply(
      nodes: Array[RelationNode],
      edges: Array[RelationEdge]
  ): RelationGraph = {
    counter += 1
    val graph = new RelationGraph(counter, nodes, edges)
    graph
  }

  def apply(schemas: Array[Array[Attribute]]): RelationGraph = {
    val nodes = schemas.flatten.distinct.map(RelationNode(_))
    val edges = schemas.map(f => RelationEdge(f.toSet))
    RelationGraph(nodes, edges)
  }
}

//  HyperNodes that are isomoprhic are given the same id

/** A hypernode of the [[HyperTree]], where contains join query represented in [[RelationGraph]].
  * @param g the join graph
  */
case class HyperNode(g: RelationGraph) extends Node {

  val id = g.id

  /** Construct induced hyper-node according to the nodeset of current hypernode */
  def toInducedHyperNode(supG: RelationGraph): HyperNode = {
    HyperNode(supG.nodeInducedSubgraph(g.nodes))
  }

  override def toString: String = {
    s"${g.id}[${g.nodes.mkString("[", ",", "]")},${g.edges.mkString("[", ",", "]")}]"
  }
}

/** A hyperedge of the [[HyperTree]].
  * @param src source node
  * @param dst destination node
  */
case class HyperEdge(src: HyperNode, dst: HyperNode) extends Edge[HyperNode] {
  override def nodes: Array[HyperNode] = Array(src, dst)
}

// We regard GHD as a special kinds of hypertree

/** A hypertree
  * @param nodes hypernodes
  * @param edges hyperedges
  */
case class HyperTree(
    override val nodes: Array[HyperNode],
    override val edges: Array[HyperEdge]
) extends Graph(nodes, edges) {

  /** fractional tree width */
  lazy val fractionalHyperNodeWidth = nodes.map(_.g.width).max

  /** By adding one hypernode to existing hypertree with one edge connected,
    * we ensure the result graph is always a hypertree.
    * @param newNode new hypernode to be added to the tree.
    * @return possible new [[HyperTree]] with the given newNode added, if no such [[HyperTree]] exists,
    *         an empty array is returned.
    */
  def addHyperNode(newNode: HyperNode): Array[HyperTree] = {
    if (isEmpty()) {
      return Array(HyperTree(nodes :+ newNode, edges))
    }

    var i = 0
    val end = nodes.size
    val hypertreeBuffer = ArrayBuffer[HyperTree]()
    while (i < end) {
      val node = nodes(i)
      if (node.g.containAnyNodes(newNode.g.nodes)) {
        val newEdge = HyperEdge(node, newNode)
        val newTree = HyperTree(nodes :+ newNode, edges :+ newEdge)
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
    def runningPathSubGraph(relationNode: RelationNode) = {
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
