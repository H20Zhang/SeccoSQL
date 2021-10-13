package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.optimization.support.AnalyzeOutputSupport
import org.apache.spark.secco.execution.SharedParameter
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.statsEstimation.StatsPlanVisitor
import org.apache.spark.secco.trees.RuleExecutor

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/*---------------------------------------------------------------------------------------------------------------------
 *  This files defines a set of rules for separating communication from computation.
 *
 *  0. MarkDelay: mark the logical operators whose computation should be delayed.
 *  1. DecoupleOperators: separate communication from computation for a single logical operator.
 *  2. PackLocalComputationIntoLocalStage: pack separated local computation logical operator into LocalStage operator.
 *  3. UnpackLocalStage: unpack local computation from LocalStage operator.
 *  4. MergeLocalStage: merge consecutive LocalStage operator.
 *  5. SelectivelyPushCommunicationThroughComputation: selectively push communication past computation (i.e., delay
 *  computation).
 *---------------------------------------------------------------------------------------------------------------------
 */

/** A rule that merges consecutive natural joins (JoinType is Natural or GHD) with same joinType and mode into one multiway join */
object MergeDelayedJoin extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case j1 @ MultiwayJoin(children, joinType, mode, _)
          if (joinType == JoinType.GHD || joinType == JoinType.Natural) => {
        val joinInputs = children.flatMap { f =>
          f match {
            case j2 @ MultiwayJoin(grandsons, _, _, _)
                if j2.joinType == joinType && j2.mode == ExecMode.CoupledWithComputationDelay =>
              grandsons
            case _ => f :: Nil
          }
        }
        MultiwayJoin(joinInputs, joinType, mode)
      }
    }
}

/** A rule that merges consecutive joins into one multiway join */
object MergeAllJoin extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform { case j1 @ MultiwayJoin(children, _, mode, _) =>
      val joinInputs = children.flatMap { f =>
        f match {
          case j2 @ MultiwayJoin(grandsons, _, _, _) =>
            grandsons
          case _ => f :: Nil
        }
      }
      MultiwayJoin(joinInputs, JoinType.Natural, mode)

    }
}

/** A rule that reorder multi-way join into binary join such that the join is consecutive, i.e., two consecutive join
  * must shares some attributes.
  */
object ConsecutiveJoinReorder extends Rule[LogicalPlan] {

  def findConsecutiveJoin(join: MultiwayJoin): MultiwayJoin = {

    //we assume there is no cartesian product and numbers of children > 2
    assert(join.children.size > 2)

    val leaf1 = join.children.head
    val leaf2 = join.children
      .drop(1)
      .filter(_.outputOld.intersect(leaf1.outputOld).nonEmpty)
      .head
    val leafJoin = MultiwayJoin(
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
      rootJoin = MultiwayJoin(
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
      case j: MultiwayJoin if j.children.size > 2 =>
        findConsecutiveJoin(j)
    }
}

object MarkDelay extends Rule[LogicalPlan] with AnalyzeOutputSupport {

  def dlSession = SeccoSession.currentSession

  private def newPlanWithMode(plan: LogicalPlan, newMode: ExecMode) =
    plan match {
      case p: Project          => p.copy(mode = newMode)
      case a: Aggregate        => a.copy(mode = newMode)
      case d: Except           => d.copy(mode = newMode)
      case pk: PKFKJoin        => pk.copy(mode = newMode)
      case u: Union            => u.copy(mode = newMode)
      case j: MultiwayJoin     => j.copy(mode = newMode)
      case c: CartesianProduct => c.copy(mode = newMode)
      case f: Filter           => f.copy(mode = newMode)
      case _ =>
        throw new Exception(
          s"not supported plan:${plan.nodeName} for mutating ExecMode"
        )
    }

  /* There exists six different delay strategy: noDelay, allDelay, joinDelay, heuristicDelay, greedyDelay, DPDelay*/

  /** Not delaying any computation */
  def noDelay(rootPlan: LogicalPlan): LogicalPlan =
    rootPlan transform { case j: MultiwayJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

  /** Delaying all computation */
  def allDelay(rootPlan: LogicalPlan): LogicalPlan =
    rootPlan transform {
      case plan: LogicalPlan if plan.fastEquals(rootPlan) => plan
      case plan: LogicalPlan =>
        Try(newPlanWithMode(plan, ExecMode.CoupledWithComputationDelay)) match {
          case Failure(exception) => plan
          case Success(newPlan)   => newPlan
        }
    }

  /** Delaying computation of join */
  def joinDelay(rootPlan: LogicalPlan): LogicalPlan =
    rootPlan transform {
      case plan: LogicalPlan if plan.fastEquals(rootPlan) => plan
      case pkfk: PKFKJoin =>
        MultiwayJoin(
          Seq(pkfk.left, pkfk.right),
          JoinType.GHD,
          ExecMode.CoupledWithComputationDelay
        )
      case j: MultiwayJoin =>
        newPlanWithMode(j, ExecMode.CoupledWithComputationDelay)
    }

  /** Find optimal plan for delaying the computation using heuristic approach */
  def heuristicDelay(rootPlan: LogicalPlan): LogicalPlan =
    rootPlan transform {
      case plan: LogicalPlan if plan.fastEquals(rootPlan) => plan
      case plan: LogicalPlan if !isMaterializable(plan) =>
        newPlanWithMode(plan, ExecMode.CoupledWithComputationDelay)
    }

  /** Find optimal plan for delaying the computation by heuristic approach by judging the difference between input size and output size */
  def heuristicSizeDelay(rootPlan: LogicalPlan): LogicalPlan =
    rootPlan transform {
      case plan: LogicalPlan if plan.fastEquals(rootPlan) => plan
      case j @ MultiwayJoin(children, _, _, _) =>
        val inputSize = children
          .map(f => StatsPlanVisitor.visit(f).rowCount)
          .sum
        val outputSize = StatsPlanVisitor.visit(j).rowCount

        if (inputSize >= outputSize) {
          j
        } else {
          newPlanWithMode(j, ExecMode.CoupledWithComputationDelay)
        }
      case plan: LogicalPlan if !isMaterializable(plan) =>
        newPlanWithMode(plan, ExecMode.CoupledWithComputationDelay)
    }

  /** Find optimal plan for delaying the computation using greedy approach */
  def greedyDelay(rootPlan: LogicalPlan): LogicalPlan = {

    //set the optimizer to enable only decoupled related batches
    val decoupleOptimizer =
      dlSession.sessionState.optimizer
        .setAllBatchDisabled()
        .setBatchesEnabled(
          "Decouple Operators",
          "Pack Local Computations",
          "PartitionPushDown"
        )

    //collect the nodes with mode=ExecMode.Coupled in bottom-up order and mark states of all node as no-delay at first
    val collectedNodes = mutable.ArrayBuffer[LogicalPlan]()
    rootPlan.foreachUp(child =>
      if (child.mode == ExecMode.Coupled && !child.fastEquals(rootPlan)) {
        collectedNodes += child
      }
    )

    //initialize delay decision, optimal plan, and optimal cost
    var nodesWithDecision = collectedNodes.map((_, true)).zipWithIndex
    var optimalPlan = rootPlan
    var optimalCost = decoupleOptimizer.execute(rootPlan).allCommunicationCost()

    val startTime = System.currentTimeMillis()

    //find the optimal plan and cost greedily
    Range(0, nodesWithDecision.size).foreach { i =>
      val newNodesWithDecision = nodesWithDecision.map { case (decision, idx) =>
        if (idx == i) ((decision._1, !decision._2), idx)
        else (decision, idx)
      }
      val decisions = newNodesWithDecision
        .map(_._1)
        .map { f =>
          if (f._2) {
            (f._1, ExecMode.Coupled)
          } else {
            (f._1, ExecMode.CoupledWithComputationDelay)
          }
        }
        .toMap
      val newPlan = rootPlan transform {
        case plan: LogicalPlan
            if decisions.contains(plan) && plan.mode != decisions(plan) =>
          newPlanWithMode(plan, decisions(plan))
      }
      val decoupledPlan = decoupleOptimizer.execute(newPlan)
      val newCost = decoupledPlan.allCommunicationCost()

      logTrace(
        s"newPlan-${i}\nnewCost:${newCost}\ndecoupledPlan:${decoupledPlan}\nnewPlan:${newPlan}"
      )

      if (newCost < optimalCost) {
        nodesWithDecision = newNodesWithDecision
        optimalPlan = newPlan
        optimalCost = newCost
      }
    }

    val endTime = System.currentTimeMillis()
    logTrace(s"Greedy finish in ${endTime - startTime}ms")

    //reset optimizer with its default batches
    decoupleOptimizer.setAllBatchEnabled()

    optimalPlan
  }

  /** Find optimal plan for delaying the computation using dynamic programming approach */
  def DynamicProgrammingDelay(rootPlan: LogicalPlan): LogicalPlan = {
    //set the optimizer to enable only decoupled related batches
    val decoupleOptimizer =
      dlSession.sessionState.optimizer
        .setAllBatchDisabled()
        .setBatchesEnabled(
          "Decouple Operators",
          "Pack Local Computations",
          "PartitionPushDown"
        )

    val memorizationTable =
      mutable.HashMap[LogicalPlan, (LogicalPlan, Double)]()

    def optimalPlanAndCost(root: LogicalPlan): (LogicalPlan, Double) = {

      //check memorization table to see if result has been calculated before
      if (memorizationTable.contains(root)) {
        logTrace(
          s"reusing result for root:${root} with cost:${memorizationTable(root)._2}"
        )
        return memorizationTable(root)
      }

//      logTrace(s"computing plan and cost for root:${root}")

      //collect all nodes
      val allNodes = root.collect { case f: LogicalPlan => f }
      val relationNodes = root.collectLeaves()
      val intermediateNodes = allNodes.diff(relationNodes)

      //generate subtree that contains root
      var subtree = Seq[LogicalPlan]()
      val subtrees = mutable.ArrayBuffer[Seq[LogicalPlan]]()

      root.foreach { f =>
        subtree = subtree :+ f
        subtrees += subtree
      }

      val optimalRootWithChildren = subtrees
        .map { case subtree =>
          val subtreeWithoutRelation = subtree.diff(relationNodes)

          //find root of the childrenTree
          val remainingNodes = intermediateNodes.diff(subtreeWithoutRelation)
          val childrenRoots = remainingNodes.filter { childRoot =>
            remainingNodes
              .diff(Seq(childRoot))
              .forall(child => child.find(f => f == childRoot).isEmpty)
          }

          //calculate communication cost of the root
          val setDelay =
            subtreeWithoutRelation.filter(node => node != root).toSet
          val newRoot = root transform {
            case p: LogicalPlan if setDelay(p) =>
              newPlanWithMode(p, ExecMode.CoupledWithComputationDelay)
          }

          val localStage = decoupleOptimizer.execute(newRoot)
          val selfCost = localStage.communicationCost()
          logTrace(s"localStage:${localStage} with cost:${selfCost}")

          //find the optimal communication cost of the children's root
          val childrenOptimalPlanAndCost = childrenRoots.map(childRoot =>
            (childRoot, optimalPlanAndCost(childRoot))
          )

          //sum the communication cost
          val cost =
            selfCost + childrenOptimalPlanAndCost.map(f => f._2._2).sum

          (newRoot, childrenOptimalPlanAndCost, cost)
        }
        .minBy(_._3)

      //replace old childRoot with new optimal childRoot
      val optimalCost = optimalRootWithChildren._3
      val newRoot = optimalRootWithChildren._1
      val oldChild2NewChild =
        optimalRootWithChildren._2.map(f => (f._1, f._2._1)).toMap
      val optimalPlan = newRoot transform {
        case p: LogicalPlan if oldChild2NewChild.contains(p) =>
          oldChild2NewChild(p)
      }

      //store result into memorization table
      memorizationTable(root) = ((optimalPlan, optimalCost))

      (optimalPlan, optimalCost)
    }

    val startTime = System.currentTimeMillis()

    val (optimalPlan, optimalCost) = optimalPlanAndCost(rootPlan)

    val endTime = System.currentTimeMillis()
    logTrace(s"DP finish in ${endTime - startTime}ms")
    logInfo(s"DP optimal cost:${optimalCost}\noptimal plan:${optimalPlan}")

    //reset optimizer with its default batches
    decoupleOptimizer.setAllBatchEnabled()

    optimalPlan
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform { case RootNode(child, _) =>
//        pprint.pprintln(
//          s"[debug]: dlSession.sessionState.conf.DELAY_STRATEGY:${dlSession.sessionState.conf.delayStrategy}"
//        )

      dlSession.sessionState.conf.delayStrategy match {
        case "Heuristic"     => heuristicDelay(child)
        case "HeuristicSize" => heuristicSizeDelay(child)
        case "Greedy"        => greedyDelay(child)
        case "DP"            => DynamicProgrammingDelay(child)
        case "NoDelay"       => noDelay(child)
        case "AllDelay"      => allDelay(child)
        case "JoinDelay"     => joinDelay(child)
        case x: String =>
          throw new Exception(s"Not supported delay strategy:${x}")
      }
    }
}

/** A rule that decouples communication and computation for operators */
object DecoupleOperators extends Rule[LogicalPlan] with AnalyzeOutputSupport {

  private def handleProject(p: Project): LogicalPlan = {
    val conf = SeccoSession.currentSession.sessionState.conf

    //no delay
    if (p.mode == ExecMode.Coupled) {

      if (conf.enableEarlyAggregationOptimization) { //perform early aggregation
        val localProject =
          Project(p.child, p.projectionListOld, ExecMode.Computation)
        val restriction =
          mutable.HashMap(
            localProject.outputOld
              .diff(p.projectionListOld)
              .map(f => (f, 1)): _*
          )

        val sharedRestriction = SharedParameter(restriction)
        val partition = Partition(localProject, sharedRestriction)
        Project(partition, p.projectionListOld, ExecMode.Computation)
      } else { //not perform early aggregation
        val restriction =
          mutable.HashMap(
            p.child.outputOld.diff(p.projectionListOld).map(f => (f, 1)): _*
          )
        val sharedRestriction = SharedParameter(restriction)

        val partition = Partition(p.child, sharedRestriction)
        Project(partition, p.projectionListOld, ExecMode.Computation)
      }

    } else { //delay

      val restriction =
        mutable.HashMap(
          p.child.outputOld.diff(p.projectionListOld).map(f => (f, 1)): _*
        )
      val sharedRestriction = SharedParameter(restriction)

      val partition = Partition(p.child, sharedRestriction)
      Project(partition, p.projectionListOld, ExecMode.DelayComputation)
    }
  }

  private def handleAggregate(a: Aggregate) = {

    val conf = SeccoSession.currentSession.sessionState.conf

    //no delay
    if (a.mode == ExecMode.Coupled) {
      if (conf.enableEarlyAggregationOptimization) { //perform early aggregation optimization
        val localAggregation =
          Aggregate(
            a.child,
            a.groupingListOld,
            a.semiringListOld,
            Seq(),
            ExecMode.Computation
          )

        val restriction =
          mutable.HashMap[String, Int](
            localAggregation.outputOld
              .diff(a.groupingListOld)
              .map(f => (f, 1)): _*
          )
        val partition =
          Partition(localAggregation, SharedParameter(restriction))
        Aggregate(
          partition,
          a.groupingListOld,
          (a.semiringListOld._1, localAggregation.producedOutputOld.head),
          a.producedOutputOld,
          ExecMode.Computation
        )
      } else { //early aggregation optimization not enabled
        val restriction =
          mutable.HashMap[String, Int](
            a.child.outputOld.diff(a.groupingListOld).map(f => (f, 1)): _*
          )

        val partition = Partition(a.child, SharedParameter(restriction))
        Aggregate(
          partition,
          a.groupingListOld,
          a.semiringListOld,
          a.producedOutputOld,
          ExecMode.Computation
        )
      }
    } else { //delay

      val restriction =
        mutable.HashMap[String, Int](
          a.child.outputOld.diff(a.groupingListOld).map(f => (f, 1)): _*
        )

      val partition = Partition(a.child, SharedParameter(restriction))
      Aggregate(
        partition,
        a.groupingListOld,
        a.semiringListOld,
        a.producedOutputOld,
        ExecMode.DelayComputation
      )
    }
  }

  private def handleDiff(d: Except) = {
    val l = d.left
    val r = d.right
    val restriction = mutable.HashMap[String, Int]()
    val sharedRestriction = SharedParameter(restriction)

    val lPartition = Partition(l, sharedRestriction)
    val rPartition = Partition(r, sharedRestriction)

    d.mode match {
      case ExecMode.Coupled =>
        Except(lPartition, rPartition, ExecMode.Computation)
      case ExecMode.CoupledWithComputationDelay =>
        Except(lPartition, rPartition, ExecMode.DelayComputation)
    }
  }

  private def handlePKFKJoin(pk: PKFKJoin) = {
    val l = pk.left
    val r = pk.right
    val restriction = mutable.HashMap[String, Int]()
    val sharedRestriction = SharedParameter(restriction)
    val lPartition = Partition(l, sharedRestriction)
    val rPartition = Partition(r, sharedRestriction)

    pk.mode match {
      case ExecMode.Coupled =>
        PKFKJoin(lPartition, rPartition, pk.joinType, ExecMode.Computation)
      case ExecMode.CoupledWithComputationDelay =>
        PKFKJoin(lPartition, rPartition, pk.joinType, ExecMode.DelayComputation)
    }

  }

  private def handleUnion(u: Union) = {
    val children = u.children
    val restriction = mutable.HashMap[String, Int]()
    val sharedRestriction = SharedParameter(restriction)
    val childrenPartitions =
      children.map(child => Partition(child, sharedRestriction))

    u.mode match {
      case ExecMode.Coupled => Union(childrenPartitions, ExecMode.Computation)
      case ExecMode.CoupledWithComputationDelay =>
        Union(childrenPartitions, ExecMode.DelayComputation)
    }
  }

  private def handleCartesianProduct(c: CartesianProduct) = {
    val children = c.children
    val restriction = mutable.HashMap[String, Int]()
    val sharedRestriction = SharedParameter(restriction)
    val childrenPartitions =
      children.map(child => Partition(child, sharedRestriction))

    c.mode match {
      case ExecMode.Coupled =>
        CartesianProduct(childrenPartitions, ExecMode.Computation)
      case ExecMode.CoupledWithComputationDelay =>
        CartesianProduct(childrenPartitions, ExecMode.DelayComputation)
    }
  }

  private def handleJoin(j: MultiwayJoin) = {
    val children = j.children
    val restriction = mutable.HashMap[String, Int]()
    val sharedRestriction = SharedParameter(restriction)
    val childrenPartitions =
      children.map(child => Partition(child, sharedRestriction))

    j.mode match {
      case ExecMode.Coupled =>
        MultiwayJoin(
          childrenPartitions,
          j.joinType,
          ExecMode.Computation
        )
      case ExecMode.CoupledWithComputationDelay =>
        MultiwayJoin(
          childrenPartitions,
          j.joinType,
          ExecMode.DelayComputation
        )
    }
  }

  private def handleFilter(f: Filter) = {
    f.mode match {
      case ExecMode.Coupled => f.copy(mode = ExecMode.Computation)
      case ExecMode.CoupledWithComputationDelay =>
        f.copy(mode = ExecMode.DelayComputation)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case p: Project
          if p.mode == ExecMode.Coupled || p.mode == ExecMode.CoupledWithComputationDelay =>
        handleProject(p)
      case a: Aggregate
          if a.mode == ExecMode.Coupled || a.mode == ExecMode.CoupledWithComputationDelay =>
        handleAggregate(a)
      case d: Except
          if d.mode == ExecMode.Coupled || d.mode == ExecMode.CoupledWithComputationDelay =>
        handleDiff(d)
      case pk: PKFKJoin
          if pk.mode == ExecMode.Coupled || pk.mode == ExecMode.CoupledWithComputationDelay =>
        handlePKFKJoin(pk)
      case u: Union
          if u.mode == ExecMode.Coupled || u.mode == ExecMode.CoupledWithComputationDelay =>
        handleUnion(u)
      case j: MultiwayJoin
          if j.mode == ExecMode.Coupled || j.mode == ExecMode.CoupledWithComputationDelay =>
        handleJoin(j)
      case c: CartesianProduct
          if c.mode == ExecMode.Coupled || c.mode == ExecMode.CoupledWithComputationDelay =>
        handleCartesianProduct(c)
      case f: Filter
          if f.mode == ExecMode.Coupled || f.mode == ExecMode.CoupledWithComputationDelay =>
        handleFilter(f)
    }
}

/** A rule that packs local operator into LocalStage */
object PackLocalComputationIntoLocalStage extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      case p: Project
          if p.mode == ExecMode.Computation || p.mode == ExecMode.DelayComputation =>
        LocalStage.box(p.children, p)
      case j: Filter
          if j.mode == ExecMode.Computation || j.mode == ExecMode.DelayComputation =>
        LocalStage.box(j.children, j)
      case a: Aggregate
          if a.mode == ExecMode.Computation || a.mode == ExecMode.DelayComputation =>
        LocalStage.box(a.children, a)
      case pk: PKFKJoin
          if pk.mode == ExecMode.Computation || pk.mode == ExecMode.DelayComputation =>
        LocalStage.box(pk.children, pk)
      case d: Except
          if d.mode == ExecMode.Computation || d.mode == ExecMode.DelayComputation =>
        LocalStage.box(d.children, d)
      case u: Union
          if u.mode == ExecMode.Computation || u.mode == ExecMode.DelayComputation =>
        LocalStage.box(u.children, u)
      case j: MultiwayJoin
          if j.mode == ExecMode.Computation || j.mode == ExecMode.DelayComputation =>
        LocalStage.box(j.children, j)
      case c: CartesianProduct
          if c.mode == ExecMode.Computation || c.mode == ExecMode.DelayComputation =>
        LocalStage.box(c.children, c)
    }
}

/** A rule that unpacks LocalStage into local computation operators */
object UnPackLocalStage extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform { case l: LocalStage =>
      l.unboxedPlan()
    }
}

/** A rule that merges multiple LOP into single one */
object MergeLocalStage extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case l: LocalStage if l.children.exists(_.isInstanceOf[LocalStage]) =>
        l.mergeConsecutiveLocalStage()
    }
}

/** A rule that selectively push communication operator (Partition) past LocalStage based on mode (Computation versus DelayComputation) */
object SelectivelyPushCommunicationThroughComputation
    extends Rule[LogicalPlan] {

  // we should check if with the new restrictions, there is chance to produce non-all 1 shares.
  private def isPushDownValid(p: Partition): Boolean = {

    val restrictionAttributes = p.sharedRestriction.res.keys.toSeq

    val childrenRestrictionAttributes = {
      val localStage = p.child.asInstanceOf[LocalStage]
      val partitions = localStage.children.map(_.asInstanceOf[Partition])

      if (partitions.nonEmpty) {
        partitions.head.restriction.keys.toSeq
      } else {
        Seq()
      }
    }

    (restrictionAttributes ++ childrenRestrictionAttributes).distinct.toSet != p.outputOld
  }

  private def pushDownOnePartition(p: Partition): LogicalPlan = {

    val localStage = p.child.asInstanceOf[LocalStage]
    val partitions = localStage.children.map(_.asInstanceOf[Partition])

    if (partitions.nonEmpty) {
      val headOfPartitions = partitions.head
      assert(
        partitions.forall(
          _.sharedRestriction == headOfPartitions.sharedRestriction
        )
      )
      p.sharedRestriction.res ++= headOfPartitions.sharedRestriction.res
    }

    // create and update restriction of newPartitions
    val newPartitions = partitions.map { gChild =>
      val newGChild =
        gChild.copy(
          sharedRestriction = p.sharedRestriction
        )
      newGChild
    }

    localStage.withNewChildren(newPartitions)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      case p @ Partition(child, _, _) if child.mode == ExecMode.Computation => p
      case p @ Partition(child, _, _)
          if child.mode == ExecMode.DelayComputation
            && child.children.forall(
              _.isInstanceOf[Partition]
            ) && isPushDownValid(p) =>
        pushDownOnePartition(p)
    }
}

////TODO: More testings are still needed, small bugs may exist somewhere.
///** A rule that selectively push communication operator (Partition) past LocalStage */
//object SelectivelyPushCommunicationThroughComputation
//    extends Rule[LogicalPlan]
//    with AnalyzeOutputSupport {
//
//  //TODO: we should check if with the restrictions, there is
//  //      chance to produce good shares.
//  private def isRestrictionValid(partition: Partition): Boolean = {
//    true
//  }
//
//  private def pushDownOnePartition(p: Partition): LogicalPlan = {
//
//    val lOp = p.child.asInstanceOf[LocalStage]
//    val gChildren = lOp.children.map(_.asInstanceOf[Partition])
//
//    if (gChildren.nonEmpty) {
//      val gHead = gChildren.head
//      assert(gChildren.forall(_.sharedRestriction == gHead.sharedRestriction))
//      p.sharedRestriction.res ++= gHead.sharedRestriction.res
//    }
//
//    // create and update restriction of newGChildren
//    val newGChildren = gChildren.map { gChild =>
//      val newGChild =
//        gChild.copy(
//          sharedRestriction = p.sharedRestriction
//        )
//      newGChild
//    }
//
//    lOp.withNewChildren(newGChildren)
//  }
//
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transformUp {
//      case l @ LocalStage(_, _) if l.children.forall(_.isInstanceOf[Partition])=>
//        val newl = l
//          .mapChildren { child =>
//            val partition = child.asInstanceOf[Partition]
//            if (partition.child.mode == ExecMode.Computation) {
//              partition
//            } else {
//              pushDownOnePartition(child.asInstanceOf[Partition])
//            }
//          }
//          .asInstanceOf[LocalStage]
//
//        val LOpChilren = newl.children.filter(_.isInstanceOf[LocalStage])
//
//        if (LOpChilren.nonEmpty && newl != l) {
//          val outputPlan = LOpChilren.foldLeft(newl)((lop, childLOp) =>
//            lop.merge(childLOp.asInstanceOf[LocalStage])
//          )
//          outputPlan
//        } else {
//          newl
//        }
//    }
//}

///** A operator that unpack LOP, if its root plan is scan */
//object UnPackScanOp extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case l @ LOp(_, _, _) if l.rootPlan.isInstanceOf[Scan] =>
//        l.rootPlan
//    }
//}

///** A rule that decouples communication and computation for unary operators */
//object DecoupleUnaryOperator
//  extends Rule[LogicalPlan]
//    with AnalyzeOutputSupport {
//
//  private def handleProject(p: Project): LogicalPlan = {
//    if (isMaterializable(p)) {
//      val localProject =
//        Project(p.child, p.projectionListOld, ExecMode.Computation)
//      val restriction =
//        mutable.HashMap(
//          localProject.outputOld.diff(p.projectionListOld).map(f => (f, 1)): _*
//        )
//
//      val sharedRestriction = SharedParameter(restriction)
//      val partition = Partition(localProject, sharedRestriction)
//      Project(partition, p.projectionListOld, ExecMode.Computation)
//    } else {
//
//      val restriction =
//        mutable.HashMap(
//          p.child.outputOld.diff(p.projectionListOld).map(f => (f, 1)): _*
//        )
//      val sharedRestriction = SharedParameter(restriction)
//
//      val partition = Partition(p.child, sharedRestriction)
//      Project(partition, p.projectionListOld, ExecMode.Computation)
//    }
//  }
//
//  private def handleAggregate(a: Aggregate) = {
//
//    if (isMaterializable(a) || a.groupingListOld.isEmpty) {
//      val localAggregation =
//        Aggregate(
//          a.child,
//          a.groupingListOld,
//          a.semiringListOld,
//          Seq(),
//          ExecMode.Computation
//        )
//
//      val restriction =
//        mutable.HashMap[String, Int](
//          localAggregation.outputOld
//            .diff(a.groupingListOld)
//            .map(f => (f, 1)): _*
//        )
//      val partition = Partition(localAggregation, SharedParameter(restriction))
//      Aggregate(
//        partition,
//        a.groupingListOld,
//        (a.semiringListOld._1, localAggregation.producedOutput.head),
//        a.producedOutput,
//        ExecMode.Computation
//      )
//    } else {
//
//      val restriction =
//        mutable.HashMap[String, Int](
//          a.child.outputOld.diff(a.groupingListOld).map(f => (f, 1)): _*
//        )
//
//      val partition = Partition(a.child, SharedParameter(restriction))
//      Aggregate(
//        partition,
//        a.groupingListOld,
//        a.semiringListOld,
//        a.producedOutput,
//        ExecMode.Computation
//      )
//    }
//  }
//
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case p @ Project(child, projectionList, ExecMode.Coupled, _) =>
//        handleProject(p)
//      case a @ Aggregate(
//      child,
//      groupingList,
//      semiringList,
//      _,
//      ExecMode.Coupled,
//      _,
//      _
//      ) =>
//        handleAggregate(a)
//    }
//}
//
///** A rule that decouples communication and computation for binary operators */
//object DecoupleBinaryOperator
//  extends Rule[LogicalPlan]
//    with AnalyzeOutputSupport {
//
//  private def handleDiff(d: Diff) = {
//    val l = d.left
//    val r = d.right
//    val restriction = mutable.HashMap[String, Int]()
//    val sharedRestriction = SharedParameter(restriction)
//
//    val lgOp = Partition(l, sharedRestriction)
//    val rgOp = Partition(r, sharedRestriction)
//
//    Diff(lgOp, rgOp, ExecMode.Computation)
//  }
//
//  private def handlePKFKJoin(pk: PKFKJoin) = {
//    val l = pk.left
//    val r = pk.right
//    val restriction = mutable.HashMap[String, Int]()
//    val sharedRestriction = SharedParameter(restriction)
//    val lgOp = Partition(l, sharedRestriction)
//    val rgOp = Partition(r, sharedRestriction)
//
//    PKFKJoin(lgOp, rgOp, pk.joinType, ExecMode.Computation)
//  }
//
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case d @ Diff(l, r, ExecMode.Coupled) => handleDiff(d)
//      case pk @ PKFKJoin(l, r, joinType, ExecMode.Coupled, _) =>
//        handlePKFKJoin(pk)
//    }
//}
//
///** A rule that decouples communication and computation for multiway operators */
//object DecoupleMultiOperator
//  extends Rule[LogicalPlan]
//    with AnalyzeOutputSupport {
//
//  private def handleUnion(u: Union) = {
//    val children = u.children
//    val restriction = mutable.HashMap[String, Int]()
//    val sharedRestriction = SharedParameter(restriction)
//    val childrenGOp =
//      children.map(child => Partition(child, sharedRestriction))
//
//    Union(childrenGOp, ExecMode.Computation)
//  }
//
//  private def handleCartesianProduct(c: CartesianProduct) = {
//    val children = c.children
//    val restriction = mutable.HashMap[String, Int]()
//    val sharedRestriction = SharedParameter(restriction)
//    val childrenGOp =
//      children.map(child => Partition(child, sharedRestriction))
//
//    CartesianProduct(childrenGOp, ExecMode.Computation)
//  }
//
//  private def handleJoin(j: Join) = {
//    val children = j.children
//    val restriction = mutable.HashMap[String, Int]()
//    val sharedRestriction = SharedParameter(restriction)
//    val childrenGOp =
//      children.map(child => Partition(child, sharedRestriction))
//
//    Join(childrenGOp, j.joinType, ExecMode.Computation)
//  }
//
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case u @ Union(children, ExecMode.Coupled)             => handleUnion(u)
//      case j @ Join(children, joinType, ExecMode.Coupled, _) => handleJoin(j)
//      case c @ CartesianProduct(children, ExecMode.Coupled) =>
//        handleCartesianProduct(c)
//    }
//}
