package org.apache.spark.secco.optimization.util

import org.apache.spark.secco.expression.Attribute

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
  lazy val width: Double = FractionalEdgeCoverNumberCalculator.width(this)

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
