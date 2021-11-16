package org.apache.spark.secco.optimization.util.joingraph

import org.apache.spark.secco.expression.{
  And,
  Attribute,
  EqualTo,
  Expression,
  PredicateHelper
}
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.optimization.plan.{BinaryJoin, Inner}
import org.apache.spark.secco.optimization.util.{Edge, Graph, Node}

/** A node in the [[JoinGraph]], which represents an [[org.apache.spark.secco.optimization.LogicalPlan]].
  *
  * @param id id of the node
  * @param plan an logical plan
  */
case class JoinGraphNode(id: Int, plan: LogicalPlan) extends Node {
  override def verboseString: String = {
    s"${id}:${plan.simpleString}"
  }
}

object JoinGraphNode {

  private var counter = 0
  private val planToId = scala.collection.mutable.HashMap[LogicalPlan, Int]()

  def apply(plan: LogicalPlan): JoinGraphNode = {
    planToId.get(plan) match {
      case Some(id) => JoinGraphNode(id, plan)
      case None =>
        counter += 1; planToId(plan) = counter; JoinGraphNode(counter, plan)
    }
  }
}

/** A edge in the [[JoinGraph]], where it encode the join condition between two [[LogicalPlan]].
  * @param lPlan left logical plan
  * @param rPlan right logical plan
  * @param condition the join condition between two [[LogicalPlan]]
  */
case class JoinGraphEdge(
    lNode: JoinGraphNode,
    rNode: JoinGraphNode,
    condition: Option[Expression]
) extends Edge[JoinGraphNode] {

  val lPlan: LogicalPlan = lNode.plan
  val rPlan: LogicalPlan = rNode.plan

  override val nodes: Array[JoinGraphNode] = Array(lNode, rNode)

  override def verboseString: String = {
    s"$simpleString:${condition.map(_.sql).getOrElse("()")}"

  }
}

object JoinGraphEdge {

  def apply(
      lPlan: LogicalPlan,
      rPlan: LogicalPlan,
      condition: Option[Expression]
  ): JoinGraphEdge = {
    val lNode = JoinGraphNode(lPlan)
    val rNode = JoinGraphNode(rPlan)

    if (lNode.id < rNode.id) {
      new JoinGraphEdge(lNode, rNode, condition)
    } else {
      new JoinGraphEdge(rNode, lNode, condition)
    }
  }

}

/** A [[JoinGraph]] represents the inner join conditions between a set of [[LogicalPlan]]
  * @param nodes nodes of the graph
  * @param edges edges of the graph
  */
case class JoinGraph(
    override val nodes: Array[JoinGraphNode],
    override val edges: Array[JoinGraphEdge]
) extends Graph[JoinGraphNode, JoinGraphEdge](nodes, edges) {

  assert(nodes.size > 0, "node set of the join graph should > 0")

  /** Merge two nodes into a single node
    * @param lNode node of left [[LogicalPlan]]
    * @param rNode node of right [[LogicalPlan]]
    * @return (new mergedNode, new [[JoinGraph]] with mergedNode)
    */
  def mergeNode(
      lNode: JoinGraphNode,
      rNode: JoinGraphNode,
      mergeFunction: (
          LogicalPlan,
          LogicalPlan,
          Option[Expression],
          Option[Any]
      ) => LogicalPlan = {
        (
            lPlan: LogicalPlan,
            rPlan: LogicalPlan,
            joinCondition: Option[Expression],
            token: Option[Any]
        ) =>
          BinaryJoin(lPlan, rPlan, Inner, joinCondition)
      },
      token: Option[Any] = None
  ): (JoinGraphNode, JoinGraph) = {

    val joinCondition = edges
      .filter(edge => edge.nodes.toSet == Set(lNode, rNode))
      .headOption
      .map(_.condition)
      .getOrElse(None)

    val mergedNode = JoinGraphNode(
      mergeFunction(lNode.plan, rNode.plan, joinCondition, token)
    )

    var newEdges = edges
    val newNodes = nodes.diff(Seq(lNode, rNode)) :+ mergedNode

    // remove edge with (lNode, rNode)
    newEdges = newEdges.filterNot(edge => edge.nodes.toSet == Set(lNode, rNode))

    val edgesStartWithLNode = newEdges.filter(edge => edge.lNode == lNode)
    val edgesEndWithLNode = newEdges.filter(edge => edge.rNode == lNode)
    val edgesStartWithRNode = newEdges.filter(edge => edge.lNode == rNode)
    val edgesEndWithRNode = newEdges.filter(edge => edge.rNode == rNode)
    val newEdgesStartWithLNode = edgesStartWithLNode.map(edge =>
      JoinGraphEdge(mergedNode.plan, edge.rPlan, edge.condition)
    )
    val newEdgesEndWithLNode = edgesEndWithLNode.map(edge =>
      JoinGraphEdge(edge.lPlan, mergedNode.plan, edge.condition)
    )
    val newEdgesStartWithRNode = edgesStartWithRNode.map(edge =>
      JoinGraphEdge(mergedNode.plan, edge.rPlan, edge.condition)
    )
    val newEdgesEndWithRNode = edgesEndWithRNode.map(edge =>
      JoinGraphEdge(edge.lPlan, mergedNode.plan, edge.condition)
    )

    val edgesToRemove =
      edgesStartWithLNode ++ edgesEndWithLNode ++ edgesStartWithRNode ++ edgesEndWithRNode

    val edgesToAdd =
      (newEdgesStartWithLNode ++ newEdgesEndWithRNode ++ newEdgesStartWithRNode ++ newEdgesEndWithLNode)
        .map(edge => //reorder edges by node id
          if (edge.lNode.id > edge.rNode.id) {
            JoinGraphEdge(edge.rNode, edge.lNode, edge.condition)
          } else {
            edge
          }
        )
        .groupBy(edge => (edge.lNode, edge.rNode))
        .map { case (nodePair, edges) => //merge conjunctive conditions together
          val mergedCondition =
            edges.map(_.condition).flatMap(f => f).reduceOption(And)
          JoinGraphEdge(nodePair._1, nodePair._2, mergedCondition)
        }

    // replace edge start/end with lNode/rNode by mergedNode
    newEdges = newEdges.diff(edgesToRemove) ++ edgesToAdd

    (mergedNode, new JoinGraph(newNodes, newEdges))
  }

  /** Iteratively merge nodes together.
    *
    * At each round, the rankFunction ranks the node pairs, and the mergeFunction merge node pair with the highest rank.
    *
    * By default, i.e., with allowCartesianProduct setting to true, we will merge all nodes together regardless of
    * whether there are connected or not. The connected nodes will be merged first,then we merge non-connected nodes.
    *
    * @param mergeFunction function to merge two nodes
    * @param rankFunction function to rank each [[JoinGraphEdge]] with the previous merged [[JoinGraphNode]] as an
    *                     auxiliary arguments, the rank function can output an token which can be seen in mergeFunction.
    * @param allowCartesianProduct whether merge nodes that are not connected edges, which will result in cartesian
    *                              product between [[LogicalPlan]]
    * @return a new [[JoinGraph]]
    */
  def mergeNodes(
      rankFunction: (Option[JoinGraphNode], JoinGraphEdge) => (
          Double,
          Option[Any]
      ),
      mergeFunction: (
          LogicalPlan,
          LogicalPlan,
          Option[Expression],
          Option[Any]
      ) => LogicalPlan = {
        (
            lPlan: LogicalPlan,
            rPlan: LogicalPlan,
            joinCondition: Option[Expression],
            token: Option[Any]
        ) =>
          BinaryJoin(lPlan, rPlan, Inner, joinCondition)
      },
      allowCartesianProduct: Boolean = true
  ): JoinGraph = {

    var currentJoinGraph = this
    var lastMergedNode: Option[JoinGraphNode] = None

    while (currentJoinGraph.edges.nonEmpty) {

//      println(s"[debug]: joinGraph:${currentJoinGraph} \n")

      //select edge with maximum rank
      val (selectedEdge, (rank, token)) = currentJoinGraph.edges
        .map(edge => (edge, rankFunction(lastMergedNode, edge)))
        .maxBy(_._2._1)

      //merge lNode and rNode of the selected edge
      val (newMergedNode, newJoinGraph) = currentJoinGraph.mergeNode(
        selectedEdge.lNode,
        selectedEdge.rNode,
        mergeFunction,
        token
      )

      //update
      currentJoinGraph = newJoinGraph
      lastMergedNode = Some(newMergedNode)
    }

    if (allowCartesianProduct) {
      while (currentJoinGraph.nodes.size > 1) {

        println(s"[debug]: joinGraph:${currentJoinGraph} \n")

        //select cartesian node pair with maximum rank
        val (selectedEdge, (rank, token)) = currentJoinGraph.nodes
          .combinations(2)
          .map { case Array(lNode, rNode) =>
            (
              (lNode, rNode),
              rankFunction(
                lastMergedNode,
                JoinGraphEdge(lNode.plan, rNode.plan, None)
              )
            )
          }
          .maxBy(_._2._1)

        //merge lNode and rNode of the selected edge
        val (newMergedNode, newJoinGraph) = currentJoinGraph.mergeNode(
          selectedEdge._1,
          selectedEdge._2,
          mergeFunction,
          token
        )

        //update
        currentJoinGraph = newJoinGraph
        lastMergedNode = Some(newMergedNode)
      }
    }

    currentJoinGraph
  }

  /** Return the [[LogicalPlan]] represented by this [[JoinGraph]]
    *
    * Note that: by default, we just randomly merges connected nodes together to form new nodes until there is only one
    * [[JoinGraphNode]] in the [[JoinGraph]], then we return [[LogicalPlan]] of that [[JoinGraphNode]]
    */
  def toPlan(): LogicalPlan = {
    mergeNodes((_, _) => (1.0, None)).nodes.head.plan
  }

  override def toString: String = {
    s"nodes:${nodes.mkString("{", ",", "}")}\nedges:${edges.mkString("{", ",", "}")}"
  }

}

object JoinGraph extends PredicateHelper {
  def apply(plans: Seq[LogicalPlan], conditions: Seq[Expression]): JoinGraph = {

    val splittedConditions = conditions
      .flatMap(expr => splitConjunctivePredicates(expr))

    assert(
      splittedConditions.forall { expr =>
        expr match {
          case EqualTo(a: Attribute, b: Attribute) => true
          case _                                   => false
        }
      },
      s"join graph can only be constructed for equi-join, invalid conditions:${conditions}"
    )

    val nodes = plans.map(plan => JoinGraphNode(plan))
    val edges = splittedConditions
      .map { expr =>
        expr match {
          case EqualTo(a: Attribute, b: Attribute) =>
            val l = plans.find(plan => plan.outputSet.contains(a)).get
            val r = plans.find(plan => plan.outputSet.contains(b)).get

            //give the edge an orientation to avoid duplication due to edge in both direction
            if (l.hashCode() < r.hashCode()) {
              (l, r)
            } else {
              (r, l)
            }

          case c: Any => throw new Exception(s"invalid non-equi condition:${c}")
        }
      }
      .distinct
      .map { case (l, r) =>
        //find all conditions for the given plan pair and merge then into a single join condition
        val joinCondition = splittedConditions
          .filter { expr =>
            expr match {
              case EqualTo(a: Attribute, b: Attribute) =>
                (l.outputSet.contains(a) && r.outputSet.contains(
                  b
                )) || (l.outputSet.contains(b) && r.outputSet.contains(a))
              case _ => false
            }
          }
          .reduceOption(And)
        val edge = JoinGraphEdge(l, r, joinCondition)
        edge
      }

    new JoinGraph(nodes.toArray, edges.toArray)
  }
}
