package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.{
  AbstractCatalogTable,
  Catalog,
  CatalogColumn,
  CatalogTable
}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan, Rule}
import org.apache.spark.secco.optimization.util.ghd.GHDDecomposer
import org.apache.spark.secco.optimization.plan._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*---------------------------------------------------------------------------------------------------------------------
 *  This files defines a set of rules for optimizing join
 *
 *  0. MergeDelayedJoin: a rule that merges consecutive binary (multi-way) join into single multi-way join
 *  1. MergeAllJoin: a rule that merges consecutive joins into one multiway join
 *  2. ExtractPFKFJoin: a rule that extracts PK-FK join from the rest of the join
 *  3. GHDBasedJoinReordering: a rule that transforms join into GHDTree by GHD(generalized hypertree decomposition),
 *       where the join between GHDNode is acyclic join and the join inside GHDNode is cyclic join.
 *  4. ConsecutiveJoinReorder: A rule that reorders multi-way join into binary join such that the join is consecutive,
 *       i.e., two consecutive join must shares some attributes.
 *  5. ExpandGHDNode: a rule that expands GHDNode into join.
 * ---------------------------------------------------------------------------------------------------------------------
 */

/** A rule that merges consecutive natural joins (JoinType is Natural or GHD) with same joinType and mode into one multiway join */
object MergeDelayedJoin extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case j1 @ MultiwayNaturalJoin(children, joinType, mode, _)
          if (joinType == JoinType.GHD || joinType == JoinType.Natural) => {
        val joinInputs = children.flatMap { f =>
          f match {
            case j2 @ MultiwayNaturalJoin(grandsons, _, _, _)
                if j2.joinType == joinType && j2.mode == ExecMode.CoupledWithComputationDelay =>
              grandsons
            case _ => f :: Nil
          }
        }
        MultiwayNaturalJoin(joinInputs, joinType, mode)
      }
    }
}

/** A rule that merges consecutive joins into one multiway join */
object MergeAllJoin extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform { case j1 @ MultiwayNaturalJoin(children, _, mode, _) =>
      val joinInputs = children.flatMap { f =>
        f match {
          case j2 @ MultiwayNaturalJoin(grandsons, _, _, _) =>
            grandsons
          case _ => f :: Nil
        }
      }
      MultiwayNaturalJoin(joinInputs, JoinType.Natural, mode)

    }
}

/** A rule that extracts Primary-Foreign Key Join from the rest of the Join */
object ExtractPKFKJoin extends Rule[LogicalPlan] {

  //  construct a join graph where edge (R1, R2) exists if R1 contains foreign key of R2
  def constructPKFKJoinGraph(plans: Seq[LogicalPlan]) = {
    assert(plans.size >= 2, "#children of join should be at least 2")
    val PKFKEdges = plans
      .combinations(2)
      .flatMap { f =>
        val l = f(0)
        val r = f(1)

        val intersectionAttributeSet = l.outputOld.intersect(r.outputOld).toSet

        if (
          l.primaryKeyOld.nonEmpty && l.primaryKeyOld.toSet.subsetOf(
            intersectionAttributeSet
          )
        ) {
          Some(r, l)
        } else if (
          r.primaryKeyOld.nonEmpty && r.primaryKeyOld.toSet.subsetOf(
            intersectionAttributeSet
          )
        ) {
          Some(l, r)
        } else {
          None
        }
      }
      .toSeq

    val PKFKNodes = PKFKEdges.flatMap(f => f._1 :: f._2 :: Nil).distinct

    (PKFKNodes, PKFKEdges)
  }

  def extractPrimaryKeyForeignKeyJoin(plans: Seq[LogicalPlan]): LogicalPlan = {
    val temp = constructPKFKJoinGraph(plans)
    val PKFKNodes = temp._1
    val PKFKEdges = temp._2
    val nonPKFKNodes = plans.diff(PKFKNodes)

    val rootPKFKNodesSet = mutable.HashSet(PKFKNodes.filter { plan =>
      PKFKEdges.forall(_._2 != plan)
    }: _*)
    val remainingEdgeSet = mutable.HashSet(PKFKEdges: _*)

    while (remainingEdgeSet.nonEmpty) {
      val rootPKFKNodesList = rootPKFKNodesSet.toSeq
      rootPKFKNodesList.foreach { l =>
        remainingEdgeSet.find(edge => edge._1 == l) match {
          case Some((_, r)) =>
            val newPlan = PKFKJoin(l, r, JoinType.PKFK, ExecMode.Coupled)
            val edgesStartInL = remainingEdgeSet.filter(_._1 == l)
            val edgesStartInR = remainingEdgeSet.filter(_._1 == r)
            val edgesEndInR = remainingEdgeSet.filter(_._2 == r)

            val edgesToRemove = edgesStartInL ++ edgesEndInR
            val edgesToAdd = edgesStartInR.map(f => (newPlan, f._2))

            //replace old plan by new plan
            rootPKFKNodesSet.remove(l)
            rootPKFKNodesSet.remove(r)
            rootPKFKNodesSet.add(newPlan)
            edgesToRemove.foreach(remainingEdgeSet.remove)
            edgesToAdd.foreach(remainingEdgeSet.add)
          case None =>
        }
      }
    }

    if (nonPKFKNodes.isEmpty && rootPKFKNodesSet.size == 1) {
      rootPKFKNodesSet.head
    } else {
      MultiwayNaturalJoin(
        nonPKFKNodes ++ rootPKFKNodesSet,
        JoinType.FKFK,
        ExecMode.Coupled
      )
    }

  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      case MultiwayNaturalJoin(children, JoinType.Natural, mode, _) =>
        extractPrimaryKeyForeignKeyJoin(children)
    }
}

/** A rule that reorder multi-way join into binary join such that the join is consecutive, i.e., two consecutive join
  * must shares some attributes.
  */
object ConsecutiveJoinReorder extends Rule[LogicalPlan] {

  def findConsecutiveJoin(join: MultiwayNaturalJoin): MultiwayNaturalJoin = {

    //we assume there is no cartesian product and numbers of children > 2
    assert(join.children.size > 2)

    val leaf1 = join.children.head
    val leaf2 = join.children
      .drop(1)
      .filter(_.outputOld.intersect(leaf1.outputOld).nonEmpty)
      .head
    val leafJoin = MultiwayNaturalJoin(
      children = Seq(leaf1, leaf2),
      joinType = join.joinType,
      mode = join.mode
    )

    var remainingChildren =
      join.children.filter(f => f != leaf1 && f != leaf2)
    var rootJoin = leafJoin
    while (remainingChildren.nonEmpty) {
      val nextLeaf = remainingChildren
        .filter(_.outputOld.intersect(rootJoin.outputOld).nonEmpty)
        .head
      rootJoin = MultiwayNaturalJoin(
        children = Seq(rootJoin, nextLeaf),
        joinType = join.joinType,
        mode = join.mode
      )
      remainingChildren = remainingChildren.filter(f => f != nextLeaf)
    }

    rootJoin
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case j: MultiwayNaturalJoin if j.children.size > 2 =>
        findConsecutiveJoin(j)
    }
}

/** A rule that reorder FK-FKJoins by GHD
  *
  * Something that need to take special care of
  * 1. we should order GHD join tree by groupingList or projectionList if upper node
  *    is an aggregation or projection.
  * 2. we should perform GHD join reorder only on Foreign Key tables, otherwise, the
  *    GHD may not be optimal.
  */
object GHDBasedJoinReorder extends Rule[LogicalPlan] {

  def constructGHDJoinTree(
      plans: Seq[LogicalPlan],
      rootAttributes: Seq[String]
  ) = {
    val rootAttributeSet = rootAttributes.toSet
    val coreLeavePlan = plans.map { p =>
      p match {
//        case PKFKJoin(l, r, JoinType.PKFK, mode) => (l, r)
        case e @ _ => (e, e)
      }
    }
    val corePlan = coreLeavePlan.map(_._1)
    val leaveForCore = coreLeavePlan.toMap

    //DEBUG
//    println(s"plans:${plans}")
//    println(s"corePlan:${corePlan}")
//    println(s"leaveForCore:${leaveForCore}")

    val schema2Plan =
      corePlan
        .map(f =>
          (
            CatalogTable("Temp", f.outputOld.map(g => CatalogColumn(g)))
              .asInstanceOf[AbstractCatalogTable],
            f
          )
        )
        .toMap
    val decomposer = GHDDecomposer.defaultDecomposer

    //DEBUG
//    println(rootAttributeSet)

    val optimalGHD =
      decomposer
        .decomposeTree(schema2Plan.keys.toSeq)
        .filter { tree =>
          tree.nodes.exists { case (_, schemas) =>
            rootAttributeSet.subsetOf(schemas.flatMap(_.attributeNames).toSet)
          }
        }
        .head
    val rootNode = optimalGHD.nodes.find { case (_, schemas) =>
      rootAttributeSet.subsetOf(schemas.flatMap(_.attributeNames).toSet)
    }.get

    val rootNodeID = rootNode._1
    val edgeVisited = mutable.ArrayBuffer[(Int, Int)]()
    val nodeVisited = mutable.ArrayBuffer[Int](rootNodeID)

    val ghdEdges = optimalGHD.edges
    val remainingEdges = mutable.Set(ghdEdges: _*)

    //DEBUG
//    println(optimalGHD)
//    println(remainingEdges)
//    println(nodeVisited)

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

    val rootId = reverseEdgeVisited.nonEmpty match {
      case true  => reverseEdgeVisited.head._1
      case false => optimalGHD.nodes.head._1
    }

    val firstGHDNodeChi =
      optimalGHD
        .idToGHDNode(rootId)
        .map(schema2Plan)
        .map { core =>
          val leaf = leaveForCore(core)
          if (leaf == core) {
            core
          } else {
            PKFKJoin(core, leaf, JoinType.PKFK, ExecMode.Coupled)
          }
        }
    val firstGHDNodeLambda = firstGHDNodeChi.flatMap(_.outputOld).distinct
    val firstGHDNode =
      GHDNode(firstGHDNodeChi, firstGHDNodeLambda, ExecMode.Coupled)

    //DEBUG
//    println(s"optimalGHD:${optimalGHD}")
//    println(s"reverseEdgeVisited:${reverseEdgeVisited}")
//    println(s"rootId:${rootId}")

    val visitedNode = ArrayBuffer[Int]()
    visitedNode += rootId

    if (reverseEdgeVisited.nonEmpty) {
      reverseEdgeVisited.foldLeft(firstGHDNode.asInstanceOf[LogicalPlan]) {
        case (subTree, (x, y)) =>
          val nextNodeToVisit = visitedNode.contains(x) match {
            case true  => y
            case false => x
          }

          visitedNode += nextNodeToVisit
          val chi =
            optimalGHD.idToGHDNode(nextNodeToVisit).map(schema2Plan).map {
              core =>
                val leaf = leaveForCore(core)
                if (leaf == core) {
                  core
                } else {
                  PKFKJoin(core, leaf, JoinType.PKFK, ExecMode.Coupled)
                }
            }
          val lambda = chi.flatMap(_.outputOld).distinct
          val ghdnode = GHDNode(chi, lambda, ExecMode.Coupled)
          MultiwayNaturalJoin(
            subTree :: ghdnode :: Nil,
            JoinType.GHDFKFK,
            ExecMode.Coupled
          )
      }
    } else {
      //DEBUG
//      println(firstGHDNode)
      firstGHDNode
    }

  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case p @ Project(
            MultiwayNaturalJoin(children, JoinType.FKFK, mode1, _),
            projectionList,
            mode2,
            _
          ) =>
        p.copy(child = constructGHDJoinTree(children, projectionList))
      case a @ Aggregate(
            MultiwayNaturalJoin(children, JoinType.FKFK, mode1, _),
            groupingList,
            _,
            _,
            mode2,
            _,
            _
          ) =>
        a.copy(child = constructGHDJoinTree(children, groupingList))
      case MultiwayNaturalJoin(children, JoinType.FKFK, mode, _) =>
        constructGHDJoinTree(children, Seq())
      case p @ Project(
            MultiwayNaturalJoin(children, JoinType.PKFK, mode1, _),
            projectionList,
            mode2,
            _
          ) =>
        p
      case a @ Aggregate(
            MultiwayNaturalJoin(children, JoinType.PKFK, mode1, _),
            groupingList,
            _,
            _,
            mode2,
            _,
            _
          ) =>
        a
    }
}

/** a rule that expands GHDNode into LogicalPlan for constructing a plan from GHDTree */
object ExpandGHDNode extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform { case GHDNode(chi, lambda, mode) =>
      if (chi.size > 1) {
        MultiwayNaturalJoin(chi, JoinType.GHD, mode)
      } else {
        chi.head
      }
    }
}
