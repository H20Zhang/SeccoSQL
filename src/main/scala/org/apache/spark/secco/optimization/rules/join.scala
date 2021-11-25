package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.{
  AbstractCatalogTable,
  Catalog,
  CatalogColumn,
  CatalogTable
}
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.expression.{
  And,
  Attribute,
  AttributeReference,
  EqualTo,
  Expression,
  PredicateHelper
}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan, Rule}
import org.apache.spark.secco.optimization.util.ghd.{
  JoinHyperGraph,
  RelationGHDTreeDecomposer
}
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.optimization.rules.MarkJoinIntegrityConstraintProperty.splitConjunctivePredicates
import org.apache.spark.secco.optimization.support.ExtractRequiredProjectJoins
import org.apache.spark.secco.optimization.util.joingraph.{
  JoinGraph,
  JoinGraphEdge,
  JoinGraphNode
}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*
 *  This files defines a set of rules for optimizing join
 *
 *  1. ReplaceBinaryJoinWithMultiwayJoin: a rule that replace consecutive binary join with a single multiway join, which
 *  may contains cyclic joins.
 *  2. OptimizePKFKJoin: a rule that reorders PKFKJoins.
 *  3. OptimizeMultiwayJoin: a rule that reorders cyclic FK-FK joins inside multiway join by
 *  GHD(generalized hypertree decomposition), where the join between GHDNode is acyclic binary join and
 *  the join inside GHDNode is cyclic multiway join.
 *  4. OptimizeJoinTree: a rule that reorders acyclic binary join tree.
 *
 */

/** A rule that replaces consecutive binary join (inner, equi, cyclic) with a single multiway join.
  */
object ReplaceBinaryJoinWithMultiwayJoin
    extends Rule[LogicalPlan]
    with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {

    //set behavior of the pattern extractor
    ExtractRequiredProjectJoins.resetRequirement()
    ExtractRequiredProjectJoins.setRequiredJoinProperties(
      ForeignKeyForeignKeyJoinConstraintProperty :: EquiJoinProperty :: CyclicJoinProperty :: Nil
    )

    plan transform {
      case ExtractRequiredProjectJoins(
            inputs,
            conditions,
            projectionList,
            mode
          ) =>
        val multiwayJoin = MultiwayJoin(
          inputs,
          conditions,
          Set(ForeignKeyForeignKeyJoinConstraintProperty, EquiJoinProperty),
          mode
        )

        Project(multiwayJoin, projectionList, mode)
    }
  }
}

/** A rule that optimizes PK-FK Join by reordering PK-FK join into consecutive sequence.
  *
  * Note that: this rule will remove the [[JoinProperty]] of joins, but it will also add [[JoinIntegrityConstraintProperty]]
  * to the reordered joins.
  */
object OptimizePKFKJoin extends Rule[LogicalPlan] with PredicateHelper {

  def reorderPKFKJoin(j: BinaryJoin, m: ExecMode): Option[LogicalPlan] = {

    //set extractor to extract input relations and conditions of inner, equi-join
    ExtractRequiredProjectJoins.resetRequirement()
    ExtractRequiredProjectJoins.setRequiredJoinProperties(
      JoinProperty("equi") :: JoinProperty("optimize") :: Nil
    )

    j match {
      case ExtractRequiredProjectJoins(
            inputs,
            condition,
            projectionList,
            mode
          ) =>
        val joinGraph = JoinGraph(inputs, condition)

//        println(s"[debug]: joinGraph:${joinGraph}")

        val rankFunction = {
          (lastMergedNodeOpt: Option[JoinGraphNode], edge: JoinGraphEdge) =>
            var rank = 1.0
            var token: Option[Any] = None

            //we prioritize edges that are connected by lastMergedNode
            lastMergedNodeOpt.foreach { lastMergedNode =>
              if (
                edge.lNode == lastMergedNode || edge.rNode == lastMergedNode
              ) {
                rank += 1.0
              }
            }

            //we prioritize edges that have primary-key foreign-key join condition
            edge.condition.foreach { condition =>
              val joinAttributePairs =
                splitConjunctivePredicates(condition).collect {
                  case EqualTo(
                        a: AttributeReference,
                        b: AttributeReference
                      ) =>
                    (a, b)
                }

//              println(
//                s"edge:${edge}, edge.lNode.plan:${edge.lNode.plan}, edge.rNode.plan:${edge.rNode.plan}"
//              )
//              println(
//                s"edge.lNode.plan.primaryKeySet:${edge.lNode.plan.primaryKeySet}, edge.rNode.plan.primaryKeySet:${edge.rNode.plan.primaryKeySet}, joinAttributePairs:${joinAttributePairs}"
//              )

              if (
                joinAttributePairs.exists { case (a, b) =>
                  edge.lNode.plan.primaryKeySet
                    .intersect(
                      AttributeSet(a :: b :: Nil)
                    )
                    .nonEmpty || edge.rNode.plan.primaryKeySet
                    .intersect(
                      AttributeSet(a :: b :: Nil)
                    )
                    .nonEmpty
                }
              ) {
                rank += 1
                token = Some(PrimaryKeyForeignKeyJoinConstraintProperty)
              }
            }

            (rank, token)
        }

        //we put join at left to result in a left-deep tree like plan.
        val mergeFunction = {
          (
              l: LogicalPlan,
              r: LogicalPlan,
              condition: Option[Expression],
              token: Option[Any]
          ) =>
            val properties = token match {
              case Some(PrimaryKeyForeignKeyJoinConstraintProperty) =>
                Set(
                  PrimaryKeyForeignKeyJoinConstraintProperty,
                  EquiJoinProperty
                )
              case None =>
                Set(
                  ForeignKeyForeignKeyJoinConstraintProperty,
                  EquiJoinProperty
                )
            }

            if (l.isInstanceOf[BinaryJoin]) {
              val newJoin = BinaryJoin(
                l,
                r,
                Inner,
                condition,
                properties.asInstanceOf[Set[JoinProperty]],
                mode
              )
              newJoin
            } else {
              val newJoin = BinaryJoin(
                r,
                l,
                Inner,
                condition,
                properties.asInstanceOf[Set[JoinProperty]],
                mode
              )
              newJoin
            }
        }

        val mergedJoinPlan =
          joinGraph.mergeNodes(rankFunction, mergeFunction).nodes.head.plan

        Some(Project(mergedJoinPlan, projectionList, mode))
      case _ => None
    }

  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    MarkJoinPredicateProperty(plan) transform {
      case j @ BinaryJoin(left, right, Inner, condition, property, mode) =>
        reorderPKFKJoin(j, mode).getOrElse(j)
    }
  }

}

/** A rule that reorders joins inside multiway join by GHD-based join reordering, which split cyclic join tree represented
  * by multiway join into a acyclic join tree whose node contains cyclic join.
  *
  * Something that need to take special care of
  * 1. we should order GHD join tree by groupingList or projectionList if upper node
  *    is an aggregation or projection.
  * 2. we should perform GHD join reorder only on Foreign Key tables, otherwise, the
  *    GHD may not be optimal.
  */
object OptimizeMultiwayJoin extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {

    // calibrate extractor
    ExtractRequiredProjectJoins.resetRequirement()
    ExtractRequiredProjectJoins.setRequiredJoinProperties(
      JoinProperty("optimize") :: JoinProperty("fkfk") :: JoinProperty(
        "equi"
      ) :: Nil
    )

    //TODO: consider GHDs that facilitate aggregation push-down
    plan transform {
      case ExtractRequiredProjectJoins(
            inputs,
            conditions,
            joinProjectionList,
            mode
          ) =>
        val joinHyperGraph = JoinHyperGraph(inputs, conditions)

//        pprint.pprintln(s"[debug] inputs:${inputs}")
//        pprint.pprintln(s"[debug] conditions:${conditions}")
//        pprint.pprintln(s"[debug]:joinHyperGraph:${joinHyperGraph}")

        val plan = Project(joinHyperGraph.toPlan(), joinProjectionList, mode)

        println("--------------------")
        println(plan)
        println("--------------------")

        plan
    }
  }

}

/** a rule that reorders acyclic binary join tree. */
//TODO: implement this rule.
object OptimizeJoinTree extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

//TODO: implement this rule
/** A rule that merges consecutive natural joins (JoinType is Natural or GHD) with same joinType and mode into one multiway join */
//object MergeDelayedJoin extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case j1 @ MultiwayJoin(children, joinType, mode, _)
//          if (joinType == JoinType.GHD || joinType == JoinType.Natural) => {
//        val joinInputs = children.flatMap { f =>
//          f match {
//            case j2 @ MultiwayJoin(grandsons, _, _, _)
//                if j2.joinType == joinType && j2.mode == ExecMode.CoupledWithComputationDelay =>
//              grandsons
//            case _ => f :: Nil
//          }
//        }
//        MultiwayJoin(joinInputs, joinType, mode)
//      }
//    }
//}

//TODO: implement this rule
/** A rule that merges consecutive joins into one multiway join */
//object MergeAllJoin extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform { case j1 @ MultiwayJoin(children, _, mode, _) =>
//      val joinInputs = children.flatMap { f =>
//        f match {
//          case j2 @ MultiwayJoin(grandsons, _, _, _) =>
//            grandsons
//          case _ => f :: Nil
//        }
//      }
//      MultiwayJoin(joinInputs, JoinType.Natural, mode)
//
//    }
//}

//TODO: implement this rule
/** A rule that reorder multi-way join into binary join such that the join is consecutive, i.e., two consecutive join
  * must shares some attributes, such that it makes the choice of [[MarkDelay]] wider.
  */
//object ConsecutiveJoinReorder extends Rule[LogicalPlan] {
//
//  def findConsecutiveJoin(join: MultiwayJoin): MultiwayJoin = {
//
//    //we assume there is no cartesian product and numbers of children > 2
//    assert(join.children.size > 2)
//
//    val leaf1 = join.children.head
//    val leaf2 = join.children
//      .drop(1)
//      .filter(_.outputOld.intersect(leaf1.outputOld).nonEmpty)
//      .head
//    val leafJoin = MultiwayJoin(
//      children = Seq(leaf1, leaf2),
//      joinType = join.joinType,
//      mode = join.mode
//    )
//
//    var remainingChildren =
//      join.children.filter(f => f != leaf1 && f != leaf2)
//    var rootJoin = leafJoin
//    while (remainingChildren.nonEmpty) {
//      val nextLeaf = remainingChildren
//        .filter(_.outputOld.intersect(rootJoin.outputOld).nonEmpty)
//        .head
//      rootJoin = MultiwayJoin(
//        children = Seq(rootJoin, nextLeaf),
//        joinType = join.joinType,
//        mode = join.mode
//      )
//      remainingChildren = remainingChildren.filter(f => f != nextLeaf)
//    }
//
//    rootJoin
//  }
//
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case j: MultiwayJoin if j.children.size > 2 =>
//        findConsecutiveJoin(j)
//    }
//}

//  private def constructGHDJoinTree(
//      multiwayJoin: MultiwayJoin,
//      rootAttributes: Seq[Attribute],
//      mode: ExecMode
//  ): LogicalPlan = {
//
//    //get the hypergraph of the multiway join
//    val (hypergraph, equiJoinAttr2naturalJoinAttr, schema2Plan) =
//      multiwayJoin.hypergraph()
//
//    //get the optimal ghd
//    val optimalGHD =
//      RelationGHDTreeDecomposer
//        .decomposeTree(hypergraph)
//        .filter { tree =>
//          tree.nodes.exists { case (_, schemas) =>
//            AttributeSet(rootAttributes.map(equiJoinAttr2naturalJoinAttr))
//              .subsetOf(AttributeSet(schemas.flatten))
//          }
//        }
//        .head
//
//    val rootNode = optimalGHD.nodes.find { case (_, schemas) =>
//      AttributeSet(rootAttributes.map(equiJoinAttr2naturalJoinAttr)).subsetOf(
//        AttributeSet(schemas.flatten)
//      )
//    }.get
//
//    val rootNodeID = rootNode._1
//    val edgeVisited = mutable.ArrayBuffer[(Int, Int)]()
//    val nodeVisited = mutable.ArrayBuffer[Int](rootNodeID)
//
//    val ghdEdges = optimalGHD.edges
//    val remainingEdges = mutable.Set(ghdEdges: _*)
//
//    while (remainingEdges.nonEmpty) {
//      val nextEdges = remainingEdges.filter(f =>
//        nodeVisited.contains(f._1) || nodeVisited.contains(f._2)
//      )
//      val edgeToVisit = nextEdges.head
//      val nodeToVisit = nodeVisited.contains(edgeToVisit._1) match {
//        case true  => edgeToVisit._2
//        case false => edgeToVisit._1
//      }
//
//      nodeVisited += nodeToVisit
//      edgeVisited += edgeToVisit
//      remainingEdges.remove(edgeToVisit)
//    }
//
//    val reverseEdgeVisited = edgeVisited.reverse
//
//    // the first node to traverse the hypertree in bottom-up direction
//    val firstNodeId = reverseEdgeVisited.nonEmpty match {
//      case true  => reverseEdgeVisited.head._1
//      case false => optimalGHD.nodes.head._1
//    }
//
//    // find join conditions of hypergraph attributes.
//    // the input is the hyperedges of the hypergraph, the output is the equi-join condition.
//    def findJoinConditions(l: Seq[Attribute], r: Seq[Attribute]) = {
//      l.intersect(r).map { attr =>
//        val lPlan = schema2Plan(l)
//        val rPlan = schema2Plan(r)
//        val attrInLPlan = equiJoinAttr2naturalJoinAttr
//          .find { case (condAttr, hypergraphAttr) =>
//            lPlan.outputSet.contains(condAttr) && hypergraphAttr == attr
//          }
//          .get
//          ._1
//
//        val attrInRPlan = equiJoinAttr2naturalJoinAttr
//          .find { case (condAttr, hypergraphAttr) =>
//            rPlan.outputSet.contains(condAttr) && hypergraphAttr == attr
//          }
//          .get
//          ._1
//
//        (attrInLPlan, attrInRPlan)
//      }
//    }
//
//    // function for constructing the multiway join from a ghd node
//    def constructMultiwayJoin(ghdNodeID: Int): MultiwayJoin = {
//      val ghdNode = optimalGHD.findNode(ghdNodeID)
//      val children = ghdNode.map(schema2Plan)
//      val joinedAttributes = ghdNode
//        .combinations(2)
//        .flatMap { case Seq(l, r) =>
//          findJoinConditions(l, r)
//        }
//        .toSeq
//
//      MultiwayJoin(
//        children,
//        joinedAttributes.map { case (a, b) => EqualTo(a, b) },
//        Set(CyclicJoinProperty),
//        mode
//      )
//    }
//
//    // function for constructing the join between a multiway join and a binary join
//    def constructBinaryJoinBetweenMultiwayJoinAndSubTree(
//        subtree: LogicalPlan,
//        ghdNode: MultiwayJoin
//    ): BinaryJoin = {
//      val hypergraphAttrsOfSubTree =
//        subtree.output.map(equiJoinAttr2naturalJoinAttr)
//      val hypergraphAttrsOfGHDNode =
//        ghdNode.output.map(equiJoinAttr2naturalJoinAttr)
//
//      val joinedConds =
//        findJoinConditions(hypergraphAttrsOfSubTree, hypergraphAttrsOfGHDNode)
//
//      BinaryJoin(
//        subtree,
//        ghdNode,
//        Inner,
//        Some(joinedConds.map { case (a, b) => EqualTo(a, b) }.reduce(And)),
//        Set(AcyclicJoinProperty),
//        mode
//      )
//
//    }
//
//    // transform the first ghd node to a multiway join which is also the first node in the subtree (join tree).
//    val subtree = constructMultiwayJoin(firstNodeId).asInstanceOf[LogicalPlan]
//    val visitedNode = ArrayBuffer[Int]()
//    visitedNode += firstNodeId
//
//    if (reverseEdgeVisited.nonEmpty) {
//      reverseEdgeVisited.foldLeft(subtree) { case (subTree, (x, y)) =>
//        val nextNodeToVisit = visitedNode.contains(x) match {
//          case true  => y
//          case false => x
//        }
//
//        visitedNode += nextNodeToVisit
//
//        constructBinaryJoinBetweenMultiwayJoinAndSubTree(
//          subTree,
//          constructMultiwayJoin(nextNodeToVisit)
//        )
//      }
//    } else {
//      subtree
//    }
//  }
//
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case p @ Project(
//            m: MultiwayJoin,
//            projectionList,
//            mode
//          ) if m.mode == mode =>
//        p.copy(child =
//          constructGHDJoinTree(m, projectionList.map(_.toAttribute), mode)
//        )
//      case a @ Aggregate(
//            m: MultiwayJoin,
//            _,
//            groupingExpressions,
//            mode
//          ) if m.mode == mode =>
//        a.copy(child =
//          constructGHDJoinTree(m, groupingExpressions.map(_.toAttribute), mode)
//        )
//      case m: MultiwayJoin =>
//        constructGHDJoinTree(m, Seq(), m.mode)
//    }
