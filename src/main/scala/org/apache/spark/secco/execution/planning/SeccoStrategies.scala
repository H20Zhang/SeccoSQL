package org.apache.spark.secco.execution.planning

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.TempViewManager
import org.apache.spark.secco.execution.plan.atomic.SubqueryExec
import org.apache.spark.secco.execution.plan.communication.{
  PartitionExchangeExec,
  PullPairExchangeExec,
  ShareConstraintContext,
  ShareValues,
  ShareValuesContext
}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.execution.plan.computation._
import org.apache.spark.secco.execution.plan.computation.newIter.SeccoIterator
import org.apache.spark.secco.execution.plan.io.{
  ExternalRDDScanExec,
  LocalRowScanExec,
  PartitionedRDDScanExec
}
import org.apache.spark.secco.execution.storage.InternalPartition
import org.apache.spark.secco.expression.{
  Alias,
  Attribute,
  EqualTo,
  PredicateHelper
}
import org.apache.spark.secco.expression.aggregate.AggregateFunction
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.optimization.ExecMode.ExecMode
//import org.apache.spark.secco.execution.plan.io.{DiskScanExec, InMemoryScanExec}
import org.apache.spark.secco.execution.{
  SeccoPlan,
  LeafExecNode,
  SharedContext,
  Strategy
}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//TODO: DEBUG

/** Converts a logical plan into zero or more SeccoPlan.
  */
abstract class SeccoStrategy extends GenericStrategy[SeccoPlan] {

  /** Plan the logical plan later. */
  override protected def planLater(plan: LogicalPlan): SeccoPlan =
    PlanLater(plan)

  /** Plan the logical plan later in given `localStage`. */
  override protected def localPlanLater(
      plan: LogicalPlan,
      localStage: LocalStageExec
  ): LocalProcessingExec =
    LocalPlanLater(plan, localStage)
}

/** An place holder operator for logical plan to be planned. */
case class PlanLater(plan: LogicalPlan) extends LeafExecNode {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalPartition] = {
    throw new UnsupportedOperationException()
  }

}

/** An place holder operator for local computation logical plan to be planned. */
case class LocalPlanLater(plan: LogicalPlan, localStage: LocalStageExec)
    extends LocalProcessingExec {

  override def output: Seq[Attribute] = plan.output

  override def iterator(): SeccoIterator = {
    throw new UnsupportedOperationException()
  }

  override def children: Seq[SeccoPlan] = Seq()
}

abstract class SeccoStrategies extends QueryPlanner[SeccoPlan] {
  self: SeccoPlanner =>

  //TODO: refactor planner rules.

  /** Planning the subquery. */
  object PlanningSubquery extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SeccoPlan] = plan match {
      case s: SubqueryAlias =>
        Seq(SubqueryExec(planLater(s.child), s.alias, s.output))
      case _ => Seq()
    }
  }

  /** Planning the scanning. */
  object PlanningRowScan extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SeccoPlan] =
      plan match {
        case r: RDDRows   => Seq(ExternalRDDScanExec(r.rdd, r.output))
        case l: LocalRows => Seq(LocalRowScanExec(l.seq, l.output))
        case p: PartitionedRDDRows =>
          Seq(PartitionedRDDScanExec(p.partitions, p.output))
        case _ => Seq()
      }
  }

  /** Planning the local computations. */
  object PlanningLocalComputation extends Strategy with PredicateHelper {

    private var _localStageExec: Option[LocalStageExec] = None

    def setLocalStageExec(localStageExec: LocalStageExec): Unit = {
      _localStageExec = Some(localStageExec)
    }

    def localStageExec: LocalStageExec = {
      _localStageExec.getOrElse(
        throw new Exception(
          "LocalStageExec of Strategy PlanningLocalComputation is not set."
        )
      )
    }

    def resetLocalStageExec(): Unit = { _localStageExec = None }

    def checkValidExecutionMode(mode: ExecMode) =
      mode == ExecMode.Computation || mode == ExecMode.DelayComputation

    /** Choose the optimal binary join implementation. */
    def binaryJoinSelection(b: BinaryJoin): LocalProcessingExec = {
      assert(
        b.condition.nonEmpty,
        "Join condition of Binary Join must be non-empty, otherwise CartesianProduct should be used."
      )

      val (leftKeys, rightKeys) = b.condition
        .map { cond =>
          val conds = splitConjunctivePredicates(cond)
          conds.collect { case EqualTo(a: Attribute, b: Attribute) =>
            (a, b)
          }.unzip
        }
        .getOrElse((Seq(), Seq()))

      val buildHashTableExec = LocalBuildIndexExec(
        localPlanLater(b.right, localStageExec),
        HashMapIndex,
        rightKeys
      )

      // TODO: optimize for theta-join.
      LocalHashJoinExec(
        localPlanLater(b.left, localStageExec),
        buildHashTableExec,
        leftKeys,
        b.joinType,
        b.condition.get
      )

    }

    /** Choose the optimal multiway join implementation. */
    def multiwayJoinSelection(m: MultiwayJoin): LocalProcessingExec = {
      val buildTrieExec = m.children.map(child =>
        LocalBuildIndexExec(
          localPlanLater(child, localStageExec),
          TrieIndex,
          m.FindRelativeAttributeOrder(child.output)
        )
      )

      LocalLeapFrogJoinExec(buildTrieExec, m.conditions, m.attributeOrder)
    }

    override def apply(plan: LogicalPlan): Seq[SeccoPlan] =
      plan match {
        case p @ PlaceHolder(pos, output) if checkValidExecutionMode(p.mode) =>
          Seq(LocalInputExec(localStageExec, pos, output))
        case s @ Filter(child, condition, mode)
            if checkValidExecutionMode(mode) =>
          Seq(
            LocalSelectExec(
              localPlanLater(child, localStageExec),
              condition
            )
          )
        case p @ Project(child, projectionList, mode)
            if checkValidExecutionMode(mode) =>
          Seq(
            LocalProjectExec(
              localPlanLater(child, localStageExec),
              projectionList
            )
          )
        case d @ Distinct(child, mode) if checkValidExecutionMode(mode) =>
          Seq(LocalDistinctExec(localPlanLater(child, localStageExec)))
        case s @ Sort(child, sortOrder, mode)
            if checkValidExecutionMode(mode) =>
          Seq(
            LocalSortExec(
              localPlanLater(child, localStageExec),
              sortOrder.map(_._1.toAttribute),
              sortOrder.map(_._2)
            )
          )
        case a @ Aggregate(
              child,
              aggregateExpressions,
              groupingExpressions,
              mode
            ) if checkValidExecutionMode(mode) =>
          assert(
            aggregateExpressions.forall(_.isInstanceOf[Alias]),
            "The top expr operator in aggregateExpressions must be `AS`."
          )

          val aggregateFunctions = aggregateExpressions
            .map(_.asInstanceOf[Alias])
            .map(_.child.asInstanceOf[AggregateFunction])

          Seq(
            LocalAggregateExec(
              localPlanLater(child, localStageExec),
              a.output,
              groupingExpressions,
              aggregateFunctions
            )
          )
        case c @ CartesianProduct(left, right, mode)
            if checkValidExecutionMode(mode) =>
          Seq(
            LocalCartesianProductExec(
              localPlanLater(left, localStageExec),
              localPlanLater(right, localStageExec)
            )
          )
        case b: BinaryJoin if checkValidExecutionMode(b.mode) =>
          Seq(binaryJoinSelection(b))

        case m: MultiwayJoin if checkValidExecutionMode(m.mode) =>
          Seq(
            multiwayJoinSelection(m)
          )
        case u @ Union(children, mode) if checkValidExecutionMode(mode) =>
          // Convert multiway union to binary LocalUnionExec.
          assert(
            children.size >= 2,
            "Numbers of relation to be unioned must be >= 2."
          )
          val left = children(0)
          val right = children(1)
          val unionExec = LocalUnionExec(
            localPlanLater(left, localStageExec),
            localPlanLater(right, localStageExec)
          )
          val remainingChildren = children.drop(2)
          Seq(remainingChildren.foldLeft(unionExec) { case (unionExec, right) =>
            LocalUnionExec(unionExec, localPlanLater(right, localStageExec))
          })
        case _ => Seq()
      }

  }

  //TODO: DEBUG
  /** Planning the pair operators, and construct the local stages for local computation operators to be executed. */
  object PlanningPairAndLocalCompute extends Strategy {

    /** Generate local plans that performs local computation. */
    private def genLocalExec(
        rootPlan: LogicalPlan,
        localStageExec: LocalStageExec
    ): LocalProcessingExec = {

      // Init the planner for planning local computation.
      val planner = new SeccoPlanner()

      // Set the Local Computation Stages for the Local Computation to be planned.
      planner.PlanningLocalComputation.setLocalStageExec(localStageExec)

      val localExec =
        planner
          .plan(rootPlan)
          .next()
          .asInstanceOf[LocalProcessingExec]

      // Reset the Local Computation Stage to avoid affecting later planning.
      planner.PlanningLocalComputation.resetLocalStageExec()

      localExec
    }

    /** Generate pair exchange plan that pairs up partitions from partitions of inputs. */
    private def genPairExchangeExec(
        partitions: Seq[Partition]
    ): PullPairExchangeExec = {

      assert(
        partitions.nonEmpty,
        "numbers of partitions participated in PiarExchangeExc should > 1."
      )
      val shareConstraintContext = partitions.head.shareConstraintContext
      val naiveRawShares = AttributeMap(
        partitions.flatMap(f => f.output).map(f => (f, 1))
      )

      val shareValues = ShareValues(
        naiveRawShares,
        shareConstraintContext.shareConstraint.equivalenceAttrs
      )

      val sharesValueContext = ShareValuesContext(shareValues)

      PullPairExchangeExec(
        partitions.map { partition =>
          PartitionExchangeExec(planLater(partition.child), sharesValueContext)
        },
        shareConstraintContext,
        sharesValueContext
      )
    }

    private def isInputPlan(plan: LogicalPlan): Boolean = plan match {
      case _: LocalRows          => true
      case _: RDDRows            => true
      case _: PartitionedRDDRows => true
      case _: SubqueryAlias      => true
      case _                     => false
    }

    //TODO: implement the planning for iterative queries.
    override def apply(plan: LogicalPlan): Seq[SeccoPlan] =
      plan match {
        // Handle input relation cases.
        case l @ PairThenCompute(Seq(child), localPlan)
            if (isInputPlan(child)) =>
          val localStageExec =
            LocalStageExec(
              planLater(child),
              null
            )

          localStageExec.localExec = genLocalExec(localPlan, localStageExec)

          Seq(localStageExec)
        // Handle other cases
        case l @ PairThenCompute(children, localPlan)
            if children.forall(_.isInstanceOf[Partition]) =>
          val pairExchangeExec = genPairExchangeExec(
            children.map(_.asInstanceOf[Partition])
          )

          val localStageExec =
            LocalStageExec(
              pairExchangeExec,
              null
            )

          localStageExec.localExec = genLocalExec(localPlan, localStageExec)

          Seq(localStageExec)
        //        case l @ PairThenCompute(children, subTreeNodes)
        //            if children.forall(child =>
        //              child.isInstanceOf[Partition] || child.isInstanceOf[Cache]
        //            ) =>
        //          val sharedAttributeOrder = SharedParameter(
        //            mutable.ArrayBuffer[String]()
        //          )
        //          val sharedPreparationTasks = genSharedPreparationTasks(children.size)
        //
        //          val localExec = genLocalExec(l.localPlan, sharedAttributeOrder)
        //          val iterativePairExchangeExec =
        //            genIterativePairExchangeExec(
        //              children,
        //              sharedAttributeOrder,
        //              sharedPreparationTasks
        //            )
        //          val lOpExec =
        //            LocalStageExec(
        //              iterativePairExchangeExec,
        //              sharedAttributeOrder,
        //              sharedPreparationTasks,
        //              localExec
        //            )
        //
        //          Seq(lOpExec)
        case _ => Seq()
      }

//    def genIterativePairExchangeExec(
//        children: Seq[LogicalPlan],
//        sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]],
//        sharedPreparationTasks: Array[
//          SharedParameter[mutable.ArrayBuffer[PreparationTask]]
//        ],
//        sharedShare: SharedParameter[mutable.HashMap[String, Int]] =
//          SharedParameter(
//            mutable.HashMap()
//          )
//    ): IterativePairExchangeExec = {
//      val cacheOp = {
//        children.filter(_.isInstanceOf[Cache])
//      }
//      val uncachedPartitionOps = children
//        .filter(_.isInstanceOf[Partition])
//        .map(_.asInstanceOf[Partition])
//      val cachedPartitionOps = cacheOp.map(
//        _.asInstanceOf[Cache].child.asInstanceOf[Partition]
//      )
//
//      val cachedPartitionPlusUncachedPartition =
//        cacheOp ++ uncachedPartitionOps
//      val orderRearrange: Seq[Int] =
//        children.map(cachedPartitionPlusUncachedPartition.indexOf)
//
//      val cachedPos = cacheOp.map(children.indexOf)
//      val unCachedPos = uncachedPartitionOps.map(children.indexOf)
//
//      val sharedPreparationTasksForCached =
//        cachedPos.map(sharedPreparationTasks).toArray
//      val sharedPreparationTasksForUncached =
//        unCachedPos.map(sharedPreparationTasks).toArray
//
//      val cachedPairedExec =
//        genPairExchangeExec(
//          cachedPartitionOps,
//          sharedAttributeOrder,
//          sharedPreparationTasksForCached,
//          sharedShare
//        )
//
//      val constraint =
//        (cachedPartitionOps ++ uncachedPartitionOps).head.restriction
//      val pairExchangeExec = IterativePairExchangeExec(
//        cachedPairedExec +: uncachedPartitionOps.zipWithIndex.map {
//          case (partition, i) =>
//            LocalPreparationExec(
//              PartitionExchangeExec(planLater(partition.child), sharedShare),
//              sharedAttributeOrder,
//              sharedPreparationTasksForUncached(i)
//            )
//        },
//        orderRearrange,
//        constraint,
//        sharedShare
//      )
//      pairExchangeExec
//    }
  }

  //  object AtomicStrategy extends Strategy {
  //    override def apply(plan: LogicalPlan): Seq[SeccoPlan] =
  //      plan match {
  //        case c @ Cache(child, _) if !child.isInstanceOf[Partition] =>
  //          Seq(CacheExec(planLater(child)))
  //        case a @ Assign(child, tableIdentifier, _) =>
  //          Seq(AssignExec(planLater(child), tableIdentifier))
  //        case u @ Update(child, tableIdentifier, deltaTableName, key, _) =>
  //          Seq(
  //            UpdateExec(planLater(child), tableIdentifier, deltaTableName, key)
  //          )
  //        case i @ Iterative(child, returnTableIdentifier, numRun, _) =>
  //          Seq(IterativeExec(planLater(child), returnTableIdentifier, numRun))
  //        case t @ Transform(child, f, output, _) =>
  //          Seq(TransformExec(planLater(child), f, output))
  //        case _ => Seq()
  //      }
  //  }

}
