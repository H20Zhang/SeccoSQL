package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.catalog.TableIdentifier
import org.apache.spark.secco.expression.{
  Attribute,
  Expression,
  NamedExpression
}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}

//abstract class GraphLogicalPlan extends LogicalPlan {}

trait Indexed {
  self: LogicalPlan =>
  def keys: Seq[Seq[Attribute]]
}

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

case class NodeRelation(
    child: LogicalPlan,
    idAttr: Attribute,
    nodeLabelAttr: Option[Attribute] = None,
    nodePropertyAttrs: Seq[Attribute] = Seq(),
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode
    with Node
    with GraphSupport {}

case class EdgeRelation(
    child: LogicalPlan,
    srcAttr: Attribute,
    dstAttr: Attribute,
    edgeLabelAttr: Option[Attribute] = None,
    edgePropertyAttrs: Seq[Attribute] = Seq(),
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode
    with Edge
    with GraphSupport {}

case class GraphRelation(
    nodeRelation: NodeRelation,
    edgeRelation: EdgeRelation,
    nodeCondition: Option[Expression] = None,
    edgeCondition: Option[Expression] = None,
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
}

case class MessagePassing(
    child: LogicalPlan with Graph,
    initialMessage: Option[NamedExpression],
    message: NamedExpression,
    mergeFunction: NamedExpression,
    updateFunction: NamedExpression,
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {}

case class Recursion(
    child: LogicalPlan,
    round: Int,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {}

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
