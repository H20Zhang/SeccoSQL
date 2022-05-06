package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.catalog.TableIdentifier
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.expression.{
  Attribute,
  Expression,
  Literal,
  NamedExpression
}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}

//abstract class GraphLogicalPlan extends LogicalPlan {}

trait GraphSupport {
  self: LogicalPlan =>
}

trait Node {
  self: LogicalPlan =>
  def idAttr: Attribute
  def nodeLabelAttr: Option[Attribute]
  def nodePropertyAttrs: Seq[Attribute]
  def nodeAttrs: Seq[Attribute] =
    Seq(idAttr) ++ nodeLabelAttr ++ nodePropertyAttrs
}

trait Edge {
  self: LogicalPlan =>

  def srcAttr: Attribute
  def dstAttr: Attribute
  def edgeLabelAttr: Option[Attribute]
  def edgePropertyAttrs: Seq[Attribute]
  def edgeAttrs: Seq[Attribute] =
    Seq(srcAttr, dstAttr) ++ edgeLabelAttr ++ edgeAttrs
}

trait Graph extends Node with Edge {
  self: LogicalPlan =>
}

/** The logical plan that represents the node set of a graph.
  * @param child the data of the node set
  * @param idAttr the attribute for node id
  * @param nodeLabelAttr the attribute for node label
  * @param nodePropertyAttrs the attributes for node properties
  * @param mode the execution mode
  */
case class NodeRelation(
    child: LogicalPlan,
    idAttr: Attribute,
    nodeLabelAttr: Option[Attribute] = None,
    nodePropertyAttrs: Seq[Attribute] = Seq(),
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode
    with Node
    with GraphSupport {

  override def output: Seq[Attribute] =
    Seq(idAttr) ++ nodeLabelAttr.toSeq ++ nodePropertyAttrs
}

/** The logical plan that represents the edge set of a graph.
  * @param child the data of the edge set
  * @param srcAttr the attribute for source node id
  * @param dstAttr the attribute for destination node id
  * @param edgeLabelAttr the attribute for edge label
  * @param edgePropertyAttrs the attributes for edge properties
  * @param mode the execution mode
  */
case class EdgeRelation(
    child: LogicalPlan,
    srcAttr: Attribute,
    dstAttr: Attribute,
    edgeLabelAttr: Option[Attribute] = None,
    edgePropertyAttrs: Seq[Attribute] = Seq(),
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode
    with Edge
    with GraphSupport {

  override def output: Seq[Attribute] =
    Seq(srcAttr, dstAttr) ++ edgeLabelAttr.toSeq ++ edgePropertyAttrs

}

/** The logical plan that represents a graph, which consists of node relation and edge relation.
  * @param nodeRelation the logical plan that stores node relation
  * @param edgeRelation the logical plan that stores edge relation
  * @param mode the execution mode
  */
case class GraphRelation(
    nodeRelation: NodeRelation,
    edgeRelation: EdgeRelation,
    mode: ExecMode = ExecMode.Atomic
) extends BinaryNode
    with Graph
    with GraphSupport {

  def srcAttr: Attribute = edgeRelation.srcAttr
  def dstAttr: Attribute = edgeRelation.dstAttr
  def idAttr: Attribute = nodeRelation.idAttr
  def nodeLabelAttr: Option[Attribute] = nodeRelation.nodeLabelAttr
  def edgeLabelAttr: Option[Attribute] = edgeRelation.edgeLabelAttr
  def nodePropertyAttrs: Seq[Attribute] = nodeRelation.nodePropertyAttrs
  def edgePropertyAttrs: Seq[Attribute] = edgeRelation.edgePropertyAttrs

  override def left: LogicalPlan = nodeRelation

  override def right: LogicalPlan = edgeRelation

  //TODO: the output of GraphRelation needs further discussion.
  override def output: Seq[Attribute] =
    edgeRelation.output ++ nodeRelation.output
}

/** The logical plan that represents a data copy of the edges to be matched
  * @param graph the graph to be matched
  * @param output the output attributes for the matching edge
  * @param attrMap the map from output attributes to graph's output
  * @param srcNodeLabels labels of the source node
  * @param dstNodeLabels labels of the destination node
  * @param srcNodeCondition property conditions on the source node
  * @param dstNodeCondition property conditions on the destination node
  * @param edgeLabels labels of the edge
  * @param edgeCondition filtering conditions on edge
  * @param mode the execution mode
  */
case class MatchingEdgeRelation(
    graph: GraphRelation,
    override val output: Seq[Attribute],
    attrMap: AttributeMap[Attribute],
    srcNodeLabels: Seq[Literal],
    dstNodeLabels: Seq[Literal],
    srcNodeCondition: Option[Expression] = None,
    dstNodeCondition: Option[Expression] = None,
    edgeLabels: Seq[Literal],
    edgeCondition: Option[Expression] = None,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def child: LogicalPlan = graph
}

/** The logical plan that represents a subgraph of the graph
  * @param graph the graph
  * @param nodeCondition the filtering conditions on node
  * @param edgeCondition the filtering conditions on edge
  * @param mode te execution mode
  */
case class SubgraphRelation(
    graph: GraphRelation,
    nodeCondition: Option[Expression] = None,
    edgeCondition: Option[Expression] = None,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def child: LogicalPlan = graph

  //TODO: the output of subgraph relation needs further discussion.
  override def output: Seq[Attribute] = graph.output
}

/** The logical plan that represents message passing in the graph.
  * @param child The graph
  * @param initialState the initial state of the node
  * @param message the messages computed from the node
  * @param mergeFunction the merge function to merge the messages gathered
  * @param updateFunction the update function to compute the new state
  * @param mode the execution mode
  */
case class MessagePassing(
    child: LogicalPlan with Graph,
    initialState: Option[NamedExpression],
    message: NamedExpression,
    mergeFunction: NamedExpression,
    updateFunction: NamedExpression,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def output: Seq[Attribute] =
    child.idAttr :: updateFunction.toAttribute :: Nil
}

/** The logical plan that incurs the child plan repeatedly up to `round` times
  * @param child the child logical plan
  * @param round the numbers of round to invoke child logical plan
  * @param mode the execution mode
  */
case class Recursion(
    child: LogicalPlan,
    round: Int,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}

//case class IndexedNodeRelation(
//    tableIdentifier: TableIdentifier,
//    idAttr: Attribute,
//    nodeLabelAttr: Attribute,
//    nodePropertyAttrs: Seq[Attribute],
//    mode: ExecMode = ExecMode.Atomic
//) extends BaseRelation
//    with Node
//    with Indexed {
//  override def newInstance(): LogicalPlan = copy()
//
//  override def keys: Seq[Seq[Attribute]] = ???
//}
//
//case class IndexedEdgeRelation(
//    tableIdentifier: TableIdentifier,
//    srcAttr: Attribute,
//    dstAttr: Attribute,
//    edgeLabelAttr: Attribute,
//    edgePropertyAttrs: Seq[Attribute],
//    mode: ExecMode = ExecMode.Atomic
//) extends BaseRelation
//    with Edge
//    with Indexed {
//  override def newInstance(): LogicalPlan = copy()
//
//  override def keys: Seq[Seq[Attribute]] = ???
//}

//case class IndexedGraphRelation(
//    tableIdentifier: TableIdentifier,
//    srcAttr: Attribute,
//    dstAttr: Attribute,
//    idAttr: Attribute,
//    nodeLabelAttr: Attribute,
//    edgeLabelAttr: Attribute,
//    nodePropertyAttrs: Seq[Attribute],
//    edgePropertyAttrs: Seq[Attribute],
//    mode: ExecMode = ExecMode.Atomic
//) extends BaseRelation
//    with Graph
//    with Indexed {
//  override def newInstance(): LogicalPlan = copy()
//
//  override def keys: Seq[Seq[Attribute]] = ???
//}
