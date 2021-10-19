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
import org.apache.spark.secco.optimization.util.ghd.GHDDecomposer
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.optimization.support.ExtractConsecutiveInnerJoins

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*
 *  This files defines a set of rules for optimizing join
 *
 *  1. MarkJoinPredicateProperty: a rule that marks predicate properties of the join.
 *  2. MarkJoinIntegrityConstraintProperty: a rule that marks integrity constraint property of the join.
 *  3. MarkJoinCyclicityProperty: a rule that marks cyclicity property of the join.
 *  4. ReplaceBinaryJoinWithMultiwayJoin: a rule that replace consecutive binary join with a single multiway join, which
 *  may contains cyclic joins.
 *  5. OptimizePKFKJoin: a rule that reorders PKFKJoins.
 *  6. OptimizeMultiwayJoin: a rule that reorders cyclic FK-FK joins inside multiway join by
 *  GHD(generalized hypertree decomposition), where the join between GHDNode is acyclic binary join and
 *  the join inside GHDNode is cyclic multiway join.
 *  7. OptimizeJoinTree: a rule that reorders acyclic binary join tree.
 *
 */

/** A rule that marks predicate properties of the join */
object MarkJoinPredicateProperty
    extends Rule[LogicalPlan]
    with PredicateHelper {

  private def identifyJoinPredicateProperties(
      j: BinaryJoin
  ): Option[JoinPredicatesProperty] = {
    j.condition match {
      case None => None
      case Some(condition) =>
        if (
          splitConjunctivePredicates(condition).forall(e =>
            e match {
              case EqualTo(a1: AttributeReference, a2: AttributeReference) =>
                true
              case _ => false
            }
          )
        ) {
          Some(EquiJoinProperty)
        } else {
          Some(ThetaJoinProperty)
        }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // only marks join predicate property for the ones that haven't been marked.
    case j @ BinaryJoin(_, _, _, _, property, mode)
        if property
          .intersect(
            Set(
              ThetaJoinProperty,
              EquiJoinProperty,
              GeneralizedEquiJoinProperty
            )
          )
          .isEmpty =>
      j.copy(property =
        property ++ identifyJoinPredicateProperties(j)
          .map(property => Set(property))
          .getOrElse(Set())
      )
  }
}

/** A rule that marks integrity constraint property of the join.
  *
  * Note that this rule should be called after [[MarkJoinPredicateProperty]], as it relies on [[EquiJoinProperty]]
  * marked by [[MarkJoinIntegrityConstraintProperty]].
  */
object MarkJoinIntegrityConstraintProperty
    extends Rule[LogicalPlan]
    with PredicateHelper {

  private def identifyJoinIntegrityProperties(
      j: BinaryJoin
  ): Option[JoinIntegrityConstraintProperty] = {
    j.condition match {
      case None => None
      case Some(condition) =>
        val joinAttributePairs = splitConjunctivePredicates(condition).map {
          f =>
            f match {
              case EqualTo(a: AttributeReference, b: AttributeReference) =>
                (a, b)
              case _ =>
                throw new Exception(
                  s"identifyJoinIntegrityProperties can be only called on " +
                    s"${EquiJoinProperty}.sql join "
                )
            }
        }

        if (
          joinAttributePairs.exists { case (a, b) =>
            j.left.primaryKeySet
              .contains(a) || j.right.primaryKeySet.contains(b)
          }
        ) {
          Some(PrimaryKeyForeignKeyJoinConstraintProperty)
        } else {
          Some(ForeignKeyForeignKeyJoinConstraintProperty)
        }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // only marks join integrity property for the ones that haven't been marked.
    case j @ BinaryJoin(_, _, _, _, property, mode)
        if property
          .intersect(
            Set(
              PrimaryKeyForeignKeyJoinConstraintProperty,
              ForeignKeyForeignKeyJoinConstraintProperty
            )
          )
          .isEmpty && property.contains(EquiJoinProperty) =>
      j.copy(property =
        property ++ identifyJoinIntegrityProperties(j)
          .map(property => Set(property))
          .getOrElse(Set())
      )
  }
}

/** A rule that marks cyclicity property of the join.
  *
  * Note that this rule only marks cyclicity between FK-FK inner Join. Also, this rule depends on the execution of
  * [[MarkJoinIntegrityConstraintProperty]].
  */
object MarkJoinCyclicityProperty
    extends Rule[LogicalPlan]
    with PredicateHelper {

  private def identifyJoinCyclicityProperties(
      j: BinaryJoin
  ): Option[JoinCyclicityProperty] = {

    //set behavior of the pattern extractor
    ExtractConsecutiveInnerJoins.clearRequiredJoinProperties()
    ExtractConsecutiveInnerJoins.setRequiredJoinProperties(
      ForeignKeyForeignKeyJoinConstraintProperty :: EquiJoinProperty :: Nil
    )

    j match {
      case ExtractConsecutiveInnerJoins(plans, mode) =>
        val leafNodes =
          plans.flatMap(child => child.children.filterNot(plans.contains))
        val conditions = plans.flatMap(child =>
          splitConjunctivePredicates(child.condition.get)
        )

        val multiwayJoin = MultiwayJoin(
          leafNodes,
          conditions,
          Set(ForeignKeyForeignKeyJoinConstraintProperty, EquiJoinProperty),
          mode
        )

        if (multiwayJoin.isCyclic()) {
          Some(CyclicJoinProperty)
        } else {
          Some(AcyclicJoinProperty)
        }

      case _ => None
    }

  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // only marks join cyclicity property for the ones that haven't been marked.
    case j @ BinaryJoin(_, _, joinType: InnerLike, _, property, mode)
        if property
          .intersect(
            Set(
              CyclicJoinProperty,
              AcyclicJoinProperty
            )
          )
          .isEmpty && Set(
          EquiJoinProperty,
          ForeignKeyForeignKeyJoinConstraintProperty
        ).asInstanceOf[Set[JoinProperty]].subsetOf(property) =>
      j.copy(property =
        property ++ identifyJoinCyclicityProperties(j)
          .map(property => Set(property))
          .getOrElse(Set())
      )
  }
}

/** A rule that replaces consecutive binary join (inner, equi, cyclic) with a single multiway join.
  */
object ReplaceBinaryJoinWithMultiwayJoin
    extends Rule[LogicalPlan]
    with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {

    //set behavior of the pattern extractor
    ExtractConsecutiveInnerJoins.clearRequiredJoinProperties()
    ExtractConsecutiveInnerJoins.setRequiredJoinProperties(
      ForeignKeyForeignKeyJoinConstraintProperty :: EquiJoinProperty :: CyclicJoinProperty :: Nil
    )

    plan transform { case ExtractConsecutiveInnerJoins(plans, mode) =>
      val leafNodes =
        plans.flatMap(child => child.children.filterNot(plans.contains))
      val conditions =
        plans.flatMap(child => splitConjunctivePredicates(child.condition.get))

      val multiwayJoin = MultiwayJoin(
        leafNodes,
        conditions,
        Set(ForeignKeyForeignKeyJoinConstraintProperty, EquiJoinProperty),
        mode
      )

      multiwayJoin
    }
  }
}

/** A rule that optimizes PK-FK Join by reordering PK-FK join into consecutive sequence.
  *
  * Note that: this rule will remove the [[JoinProperty]] of joins, but it will also add [[JoinIntegrityConstraintProperty]]
  * to the reordered joins.
  */
object OptimizePKFKJoin extends Rule[LogicalPlan] with PredicateHelper {

  // construct a join graph where each node is a input relation, and there exists an edge between two node iff there is
  // a join condition between them. We assume the join graph is a directed graph. For PK-FK join, the direction is
  // from input relation with foreign key to relation with primary key. For FK-FK join, we randomly gives the direction.
  def constructJoinGraph(
      inputs: Seq[LogicalPlan],
      conditions: Seq[Expression]
  ): (
      Seq[LogicalPlan],
      Seq[
        (LogicalPlan, LogicalPlan, Expression, JoinIntegrityConstraintProperty)
      ]
  ) = {
    val nodes = inputs
    val edges = conditions.map { cond =>
      cond match {
        case EqualTo(a: AttributeReference, b: AttributeReference) =>
          val srcNode = inputs.filter(f => f.outputSet.contains(a)).head
          val dstNode = inputs.filter(f => f.outputSet.contains(b)).head

          if (srcNode.primaryKeySet.contains(a)) {
            (dstNode, srcNode, cond, PrimaryKeyForeignKeyJoinConstraintProperty)
          } else if (dstNode.primaryKeySet.contains(b)) {
            (srcNode, dstNode, cond, PrimaryKeyForeignKeyJoinConstraintProperty)
          } else {
            (srcNode, dstNode, cond, ForeignKeyForeignKeyJoinConstraintProperty)
          }

        case cond: Expression =>
          throw new Exception(
            s"join graph cannot be constructed for ${cond}, it can only be constructed for equi-join conditions"
          )
      }
    }

    (nodes, edges)
  }

  def reorderPKFKJoin(j: BinaryJoin): Option[LogicalPlan] = {

    //extract input relations and conditions of equi-join
    ExtractConsecutiveInnerJoins.clearRequiredJoinProperties()
    ExtractConsecutiveInnerJoins.setRequiredJoinProperties(
      EquiJoinProperty :: Nil
    )
    val equiJoinsAndMode = j match {
      case ExtractConsecutiveInnerJoins(joins) => Some(joins)
      case _                                   => None
    }

    //fast return if no equi-join is found.
    if (equiJoinsAndMode.isEmpty) {
      return None
    }

    val (equiJoins, mode) = equiJoinsAndMode.get

    val inputs =
      equiJoins.flatMap(child => child.children.filterNot(equiJoins.contains))
    val conditions = equiJoins.flatMap(child =>
      splitConjunctivePredicates(child.condition.get)
    )

    //construct join graph
    val (nodes, edges) = constructJoinGraph(inputs, conditions)

    //we assume left side of equal is from left input relation, and vice verse
    val (pkfkEdges, fkfkEdges) = edges.partition {
      case (_, _, _, integrityConstraint) =>
        if (integrityConstraint == PrimaryKeyForeignKeyJoinConstraintProperty) {
          true
        } else {
          false
        }
    }

    //we call nodes that participate in PK-FK joins right side as fNodes, and nodes that participate
    // in PK-FK joins left side as P Nodes.

    val (fNodes, pNodes) = pkfkEdges.map { case (l, r, _, _) =>
      (l, r)
    }.unzip

    // we extract fNodes that are not pNodes
    val startNodeOfPKFKJoins = mutable.HashSet(fNodes.diff(pNodes): _*)
    val startNodeOfFKFKJoins = mutable.HashSet(nodes: _*)
    val remainingPKFKEdges = mutable.HashSet(pkfkEdges: _*)
    val remainingFKFKEdges = mutable.HashSet(fkfkEdges: _*)

    // we traverse the join graph to join the PK-FK joins first
    while (remainingPKFKEdges.nonEmpty) {
      val startOfFNodesList = startNodeOfPKFKJoins.toSeq
      startOfFNodesList.foreach { fNode =>
        remainingPKFKEdges.find(edge => edge._1 == fNode) match {
          case Some((_, rNode, condition, _)) =>
            val newFNode = BinaryJoin(
              fNode,
              rNode,
              Inner,
              Some(condition),
              Set(PrimaryKeyForeignKeyJoinConstraintProperty),
              mode
            )

            // with newFNode being added and old fNode and rNode being deleted, we need to maintain remainingFKFKEdges
            // and remainingPKFKEdges.
            val pkfkEdgesStartWithFNode =
              remainingPKFKEdges.filter(_._1 == fNode)
            val pkfkEdgesStartWithRNode =
              remainingPKFKEdges.filter(_._1 == rNode)
            val pkfkEdgesEndWithRNode = remainingPKFKEdges.filter(_._2 == rNode)

            val fkfkEdgesStartWithFNode =
              remainingFKFKEdges.filter(_._1 == fNode)
            val fkfkEdgesEndWithFNode =
              remainingFKFKEdges.filter(_._2 == fNode)
            val fkfkEdgesStartWithRNode =
              remainingFKFKEdges.filter(_._1 == rNode)
            val fkfkEdgesEndWithRNode = remainingFKFKEdges.filter(_._2 == rNode)

            val pkfkEdgesToRemove =
              pkfkEdgesStartWithFNode ++
                pkfkEdgesEndWithRNode ++
                pkfkEdgesStartWithRNode

            val fkfkEdgesToRemove =
              fkfkEdgesStartWithFNode ++
                fkfkEdgesEndWithFNode ++
                fkfkEdgesStartWithRNode ++
                fkfkEdgesEndWithRNode

            val pkfkEdgesToAdd =
              (pkfkEdgesStartWithFNode ++ pkfkEdgesStartWithRNode)
                .map(f => (newFNode, f._2, f._3, f._4))

            val fkfkEdgesToAdd =
              (fkfkEdgesStartWithFNode ++ fkfkEdgesStartWithRNode)
                .map(f =>
                  (newFNode, f._2, f._3, f._4)
                ) ++ (fkfkEdgesEndWithFNode ++ fkfkEdgesEndWithRNode)
                .map(f => (f._2, newFNode, f._3, f._4))

            startNodeOfPKFKJoins.remove(fNode)
            startNodeOfPKFKJoins.remove(rNode)
            startNodeOfFKFKJoins.remove(fNode)
            startNodeOfFKFKJoins.remove(rNode)

            startNodeOfPKFKJoins.add(newFNode)

            pkfkEdgesToRemove.foreach(remainingPKFKEdges.remove)
            pkfkEdgesToAdd.foreach(remainingPKFKEdges.add)
            fkfkEdgesToRemove.foreach(remainingFKFKEdges.remove)
            fkfkEdgesToAdd.foreach(remainingFKFKEdges.add)
          case None =>
        }
      }
    }

    // we traverse the join graph to join the remaining FK-FK joins
    while (remainingFKFKEdges.nonEmpty) {
      val startOfNodesList = startNodeOfFKFKJoins.toSeq
      startOfNodesList.foreach { fNode =>
        remainingFKFKEdges.find(edge => edge._1 == fNode) match {
          case Some((_, rNode, condition, _)) =>
            val newFNode = BinaryJoin(
              fNode,
              rNode,
              Inner,
              Some(condition),
              Set(ForeignKeyForeignKeyJoinConstraintProperty),
              mode
            )

            // we need to maintain remainingFKFKEdges.
            val fkfkEdgesStartWithFNode =
              remainingFKFKEdges.filter(_._1 == fNode)
            val fkfkEdgesEndWithFNode =
              remainingFKFKEdges.filter(_._2 == fNode)
            val fkfkEdgesStartWithRNode =
              remainingFKFKEdges.filter(_._1 == rNode)
            val fkfkEdgesEndWithRNode = remainingFKFKEdges.filter(_._2 == rNode)

            val fkfkEdgesToRemove =
              fkfkEdgesStartWithFNode ++
                fkfkEdgesEndWithFNode ++
                fkfkEdgesStartWithRNode ++
                fkfkEdgesEndWithRNode

            val fkfkEdgesToAdd =
              (fkfkEdgesStartWithFNode ++ fkfkEdgesStartWithRNode)
                .map(f =>
                  (newFNode, f._2, f._3, f._4)
                ) ++ (fkfkEdgesEndWithFNode ++ fkfkEdgesEndWithRNode)
                .map(f => (f._2, newFNode, f._3, f._4))

            startNodeOfFKFKJoins.remove(fNode)
            startNodeOfFKFKJoins.remove(rNode)

            fkfkEdgesToRemove.foreach(remainingFKFKEdges.remove)
            fkfkEdgesToAdd.foreach(remainingFKFKEdges.add)
          case None =>
        }
      }
    }

    assert(
      startNodeOfFKFKJoins.size == 1,
      "there should be only one logical plan left after reorderPKFKJoins"
    )

    Some(startNodeOfFKFKJoins.head)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case j @ BinaryJoin(left, right, Inner, condition, property, mode) =>
        reorderPKFKJoin(j).getOrElse(j)
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

  private def constructGHDJoinTree(
      multiwayJoin: MultiwayJoin,
      rootAttributes: Seq[Attribute],
      mode: ExecMode
  ): LogicalPlan = {

    //get the hypergraph of the multiway join
    val (hypergraph, equiJoinAttr2naturalJoinAttr, schema2Plan) =
      multiwayJoin.hypergraph()

    //get the optimal ghd
    val optimalGHD =
      GHDDecomposer
        .decomposeTree(hypergraph)
        .filter { tree =>
          tree.nodes.exists { case (_, schemas) =>
            AttributeSet(rootAttributes.map(equiJoinAttr2naturalJoinAttr))
              .subsetOf(AttributeSet(schemas.flatten))
          }
        }
        .head

    val rootNode = optimalGHD.nodes.find { case (_, schemas) =>
      AttributeSet(rootAttributes.map(equiJoinAttr2naturalJoinAttr)).subsetOf(
        AttributeSet(schemas.flatten)
      )
    }.get

    val rootNodeID = rootNode._1
    val edgeVisited = mutable.ArrayBuffer[(Int, Int)]()
    val nodeVisited = mutable.ArrayBuffer[Int](rootNodeID)

    val ghdEdges = optimalGHD.edges
    val remainingEdges = mutable.Set(ghdEdges: _*)

    while (remainingEdges.nonEmpty) {
      val nextEdges = remainingEdges.filter(f =>
        nodeVisited.contains(f._1) || nodeVisited.contains(f._2)
      )
      val edgeToVisit = nextEdges.head
      val nodeToVisit = nodeVisited.contains(edgeToVisit._1) match {
        case true  => edgeToVisit._2
        case false => edgeToVisit._1
      }

      nodeVisited += nodeToVisit
      edgeVisited += edgeToVisit
      remainingEdges.remove(edgeToVisit)
    }

    val reverseEdgeVisited = edgeVisited.reverse

    // the first node to traverse the hypertree in bottom-up direction
    val firstNodeId = reverseEdgeVisited.nonEmpty match {
      case true  => reverseEdgeVisited.head._1
      case false => optimalGHD.nodes.head._1
    }

    // find join conditions of hypergraph attributes.
    // the input is the hyperedges of the hypergraph, the output is the equi-join condition.
    def findJoinConditions(l: Seq[Attribute], r: Seq[Attribute]) = {
      l.intersect(r).map { attr =>
        val lPlan = schema2Plan(l)
        val rPlan = schema2Plan(r)
        val attrInLPlan = equiJoinAttr2naturalJoinAttr
          .find { case (condAttr, hypergraphAttr) =>
            lPlan.outputSet.contains(condAttr) && hypergraphAttr == attr
          }
          .get
          ._1

        val attrInRPlan = equiJoinAttr2naturalJoinAttr
          .find { case (condAttr, hypergraphAttr) =>
            rPlan.outputSet.contains(condAttr) && hypergraphAttr == attr
          }
          .get
          ._1

        (attrInLPlan, attrInRPlan)
      }
    }

    // function for constructing the multiway join from a ghd node
    def constructMultiwayJoin(ghdNodeID: Int): MultiwayJoin = {
      val ghdNode = optimalGHD.findNode(ghdNodeID)
      val children = ghdNode.map(schema2Plan)
      val joinedAttributes = ghdNode
        .combinations(2)
        .flatMap { case Seq(l, r) =>
          findJoinConditions(l, r)
        }
        .toSeq

      MultiwayJoin(
        children,
        joinedAttributes.map { case (a, b) => EqualTo(a, b) },
        Set(CyclicJoinProperty),
        mode
      )
    }

    // function for constructing the join between a multiway join and a binary join
    def constructBinaryJoinBetweenMultiwayJoinAndSubTree(
        subtree: LogicalPlan,
        ghdNode: MultiwayJoin
    ): BinaryJoin = {
      val hypergraphAttrsOfSubTree =
        subtree.output.map(equiJoinAttr2naturalJoinAttr)
      val hypergraphAttrsOfGHDNode =
        ghdNode.output.map(equiJoinAttr2naturalJoinAttr)

      val joinedConds =
        findJoinConditions(hypergraphAttrsOfSubTree, hypergraphAttrsOfGHDNode)

      BinaryJoin(
        subtree,
        ghdNode,
        Inner,
        Some(joinedConds.map { case (a, b) => EqualTo(a, b) }.reduce(And)),
        Set(AcyclicJoinProperty),
        mode
      )

    }

    // transform the first ghd node to a multiway join which is also the first node in the subtree (join tree).
    val subtree = constructMultiwayJoin(firstNodeId).asInstanceOf[LogicalPlan]
    val visitedNode = ArrayBuffer[Int]()
    visitedNode += firstNodeId

    if (reverseEdgeVisited.nonEmpty) {
      reverseEdgeVisited.foldLeft(subtree) { case (subTree, (x, y)) =>
        val nextNodeToVisit = visitedNode.contains(x) match {
          case true  => y
          case false => x
        }

        visitedNode += nextNodeToVisit

        constructBinaryJoinBetweenMultiwayJoinAndSubTree(
          subTree,
          constructMultiwayJoin(nextNodeToVisit)
        )
      }
    } else {
      subtree
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case p @ Project(
            m: MultiwayJoin,
            projectionList,
            mode
          ) if m.mode == mode =>
        p.copy(child =
          constructGHDJoinTree(m, projectionList.map(_.toAttribute), mode)
        )
      case a @ Aggregate(
            m: MultiwayJoin,
            _,
            groupingExpressions,
            mode
          ) if m.mode == mode =>
        a.copy(child =
          constructGHDJoinTree(m, groupingExpressions.map(_.toAttribute), mode)
        )
      case m: MultiwayJoin =>
        constructGHDJoinTree(m, Seq(), m.mode)
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
