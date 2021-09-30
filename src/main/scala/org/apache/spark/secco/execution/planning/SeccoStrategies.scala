package org.apache.spark.secco.execution.planning

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.CachedDataManager
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.execution.plan.atomic._
import org.apache.spark.secco.execution.plan.computation._
import org.apache.spark.secco.execution.plan.computation.iter.SeccoIterator
import org.apache.spark.secco.execution.plan.communication.{
  IterativePairExchangeExec,
  PartitionExchangeExec,
  PullPairExchangeExec
}
import org.apache.spark.secco.execution.plan.io.{DiskScanExec, InMemoryScanExec}
import org.apache.spark.secco.execution.{
  SeccoPlan,
  InternalBlock,
  LeafExecNode,
  SharedParameter,
  Strategy
}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//TODO: DEBUG

/** Converts a logical plan into zero or more SeccoPlan.
  */
abstract class SeccoStrategy extends GenericStrategy[SeccoPlan] {

  override protected def planLater(plan: LogicalPlan): SeccoPlan =
    PlanLater(plan)

  override protected def localPlanLater(
      plan: LogicalPlan
  ): LocalProcessingExec =
    LocalPlanLater(plan)

}

case class PlanLater(plan: LogicalPlan) extends LeafExecNode {

  override def outputOld: Seq[String] = plan.outputOld

  protected override def doExecute(): RDD[InternalBlock] = {
    throw new UnsupportedOperationException()
  }
}

case class LocalPlanLater(plan: LogicalPlan) extends LocalProcessingExec {

  /** The output iterator */
  override def iterator(): SeccoIterator = {
    throw new UnsupportedOperationException()
  }

  /** The output attributes */
  override def outputOld: Seq[String] = plan.outputOld

  override def children: Seq[SeccoPlan] = Seq()

  /** shared parameter--- attribute order */
  override def sharedAttributeOrder: SharedParameter[ArrayBuffer[String]] = null
}

abstract class SeccoStrategies extends QueryPlanner[SeccoPlan] {
  self: SeccoPlanner =>

  object IOStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SeccoPlan] =
      plan match {
        case s: Relation =>
          val dataManager =
            SeccoSession.currentSession.sessionState.cachedDataManager
          dataManager(s.tableIdentifier) match {
            case Some(x) =>
              x match {
                case r: RDD[InternalBlock] =>
                  Seq(InMemoryScanExec(s.tableIdentifier, s.outputOld))
                case d: String =>
                  Seq(DiskScanExec(s.tableIdentifier, s.outputOld, d))
              }
            case None =>
              throw new Exception(s"No such table:${s.tableIdentifier}")
          }
        case r @ Rename(child, attrRenameMap, _)
            if child.isInstanceOf[Relation] =>
          val s = child.asInstanceOf[Relation]
          val dataManager =
            SeccoSession.currentSession.sessionState.cachedDataManager
          dataManager(s.tableIdentifier) match {
            case Some(x) =>
              x match {
                case r: RDD[InternalBlock] =>
                  Seq(
                    InMemoryScanExec(
                      s.tableIdentifier,
                      s.outputOld.map(attr =>
                        attrRenameMap.getOrElse(attr, attr)
                      )
                    )
                  )
                case d: String =>
                  Seq(
                    DiskScanExec(
                      s.tableIdentifier,
                      s.outputOld.map(attr =>
                        attrRenameMap.getOrElse(attr, attr)
                      ),
                      d
                    )
                  )
              }
            case None =>
              throw new Exception(s"No such table:${s.tableIdentifier}")
          }

        case _ => Seq()
      }
  }

  //TODO: DEBUG
  object LOpStrategy extends Strategy {

    def genLocalExec(
        rootPlan: LogicalPlan,
        sharedAttributeOrder1: SharedParameter[mutable.ArrayBuffer[String]]
    ): LocalProcessingExec = {

      //gen localExec
      val planner = new SeccoPlanner()
      var localExec =
        planner
          .plan(rootPlan)
          .next()
          .asInstanceOf[LocalProcessingExec]

      //assign shared attribute order
      localExec = localExec
        .transform {
          case c: LocalCartesianProductExec =>
            c.copy(sharedAttributeOrder = sharedAttributeOrder1)
          case s: LocalSelectExec =>
            s.copy(sharedAttributeOrder = sharedAttributeOrder1)
          case j: LocalJoinExec =>
            j.copy(sharedAttributeOrder = sharedAttributeOrder1)
          case p: LocalProjectExec =>
            p.copy(sharedAttributeOrder = sharedAttributeOrder1)
          case a: LocalSemiringAggregateExec =>
            a.copy(sharedAttributeOrder = sharedAttributeOrder1)
          case placeHolderLocalExec: LocalPlaceHolderExec =>
            placeHolderLocalExec
              .copy(
                sharedAttributeOrder = sharedAttributeOrder1
              )
        }
        .asInstanceOf[LocalProcessingExec]

      //TODO: determine the base and index in FKFKJoin, and perform custom preparation

      localExec
    }

    def genPairExchangeExec(
        partitions: Seq[Partition],
        sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]],
        sharedPreparationTasks: Array[
          SharedParameter[mutable.ArrayBuffer[PreparationTask]]
        ],
        sharedShare: SharedParameter[mutable.HashMap[String, Int]] =
          SharedParameter(
            mutable.HashMap()
          ),
        isTrieNeeded: Boolean = true
    ): PullPairExchangeExec = {

      val constraint = partitions.head.restriction

      if (isTrieNeeded) {

        PullPairExchangeExec(
          partitions.zipWithIndex.map { case (partition, i) =>
            LocalPreparationExec(
              PartitionExchangeExec(planLater(partition.child), sharedShare),
              sharedAttributeOrder,
              sharedPreparationTasks(i)
            )
          },
          constraint,
          sharedShare
        )
      } else {
        PullPairExchangeExec(
          partitions.map { partition =>
            PartitionExchangeExec(planLater(partition.child), sharedShare)
          },
          constraint,
          sharedShare
        )
      }

    }

    def genIterativePairExchangeExec(
        children: Seq[LogicalPlan],
        sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]],
        sharedPreparationTasks: Array[
          SharedParameter[mutable.ArrayBuffer[PreparationTask]]
        ],
        sharedShare: SharedParameter[mutable.HashMap[String, Int]] =
          SharedParameter(
            mutable.HashMap()
          )
    ): IterativePairExchangeExec = {
      val cacheOp = {
        children.filter(_.isInstanceOf[Cache])
      }
      val uncachedPartitionOps = children
        .filter(_.isInstanceOf[Partition])
        .map(_.asInstanceOf[Partition])
      val cachedPartitionOps = cacheOp.map(
        _.asInstanceOf[Cache].child.asInstanceOf[Partition]
      )

      val cachedPartitionPlusUncachedPartition =
        cacheOp ++ uncachedPartitionOps
      val orderRearrange: Seq[Int] =
        children.map(cachedPartitionPlusUncachedPartition.indexOf)

      val cachedPos = cacheOp.map(children.indexOf)
      val unCachedPos = uncachedPartitionOps.map(children.indexOf)

      val sharedPreparationTasksForCached =
        cachedPos.map(sharedPreparationTasks).toArray
      val sharedPreparationTasksForUncached =
        unCachedPos.map(sharedPreparationTasks).toArray

      val cachedPairedExec =
        genPairExchangeExec(
          cachedPartitionOps,
          sharedAttributeOrder,
          sharedPreparationTasksForCached,
          sharedShare
        )

      val constraint =
        (cachedPartitionOps ++ uncachedPartitionOps).head.restriction
      val pairExchangeExec = IterativePairExchangeExec(
        cachedPairedExec +: uncachedPartitionOps.zipWithIndex.map {
          case (partition, i) =>
            LocalPreparationExec(
              PartitionExchangeExec(planLater(partition.child), sharedShare),
              sharedAttributeOrder,
              sharedPreparationTasksForUncached(i)
            )
        },
        orderRearrange,
        constraint,
        sharedShare
      )
      pairExchangeExec
    }

    private def genSharedPreparationTasks(size: Int) = {
      val sharedPreparationTasks =
        new Array[SharedParameter[mutable.ArrayBuffer[PreparationTask]]](size)
      var i = 0
      val sharedPreparationTaskSize = sharedPreparationTasks.size
      while (i < sharedPreparationTaskSize) {
        sharedPreparationTasks(i) = SharedParameter(
          mutable.ArrayBuffer[PreparationTask]()
        )
        i += 1
      }
      sharedPreparationTasks
    }

    override def apply(plan: LogicalPlan): Seq[SeccoPlan] =
      plan match {
        case l @ LocalStage(children, _)
            if (children.size == 1 && (children.forall(
              _.isInstanceOf[Relation]
            ) || children.forall(_.isInstanceOf[Transform]))) =>
          val sharedAttributeOrder = SharedParameter(
            mutable.ArrayBuffer[String]()
          )
          val sharedPreparationTasks = genSharedPreparationTasks(children.size)

          val localExec = genLocalExec(l.localPlan, sharedAttributeOrder)
          val lOpExec =
            LocalStageExec(
              LocalPreparationExec(
                planLater(l.children.head),
                sharedAttributeOrder,
                sharedPreparationTasks(0)
              ),
              sharedAttributeOrder,
              sharedPreparationTasks,
              localExec
            )

          Seq(lOpExec)
        case l @ LocalStage(children, _)
            if children.forall(_.isInstanceOf[Partition]) =>
          val sharedAttributeOrder = SharedParameter(
            mutable.ArrayBuffer[String]()
          )
          val sharedPreparationTasks = genSharedPreparationTasks(children.size)

          val localExec = genLocalExec(l.localPlan, sharedAttributeOrder)
          val pairExchangeExec =
            genPairExchangeExec(
              children.map(_.asInstanceOf[Partition]),
              sharedAttributeOrder,
              sharedPreparationTasks
            )
          val lOpExec =
            LocalStageExec(
              pairExchangeExec,
              sharedAttributeOrder,
              sharedPreparationTasks,
              localExec
            )

          Seq(lOpExec)
        case l @ LocalStage(children, subTreeNodes)
            if children.forall(child =>
              child.isInstanceOf[Partition] || child.isInstanceOf[Cache]
            ) =>
          val sharedAttributeOrder = SharedParameter(
            mutable.ArrayBuffer[String]()
          )
          val sharedPreparationTasks = genSharedPreparationTasks(children.size)

          val localExec = genLocalExec(l.localPlan, sharedAttributeOrder)
          val iterativePairExchangeExec =
            genIterativePairExchangeExec(
              children,
              sharedAttributeOrder,
              sharedPreparationTasks
            )
          val lOpExec =
            LocalStageExec(
              iterativePairExchangeExec,
              sharedAttributeOrder,
              sharedPreparationTasks,
              localExec
            )

          Seq(lOpExec)
        case _ => Seq()
      }
  }

  object LocalExecStrategy extends Strategy {

    private val dummyAttributeOrder
        : SharedParameter[mutable.ArrayBuffer[String]] =
      SharedParameter(mutable.ArrayBuffer[String]())

    override def apply(plan: LogicalPlan): Seq[SeccoPlan] =
      plan match {
        case s @ Filter(child, selectionExprs, _, _)
            if s.mode == ExecMode.Computation || s.mode == ExecMode.DelayComputation =>
          Seq(
            LocalSelectExec(
              localPlanLater(child),
              selectionExprs,
              dummyAttributeOrder
            )
          )
        case c @ CartesianProduct(children, _)
            if c.mode == ExecMode.Computation || c.mode == ExecMode.DelayComputation =>
          Seq(
            LocalCartesianProductExec(
              children.map(localPlanLater),
              dummyAttributeOrder
            )
          )
        case j @ MultiwayNaturalJoin(children, joinType, _, _)
            if j.mode == ExecMode.Computation || j.mode == ExecMode.DelayComputation =>
          Seq(
            LocalJoinExec(
              children.map(localPlanLater),
              joinType,
              dummyAttributeOrder
            )
          )
        case pkfk @ PKFKJoin(left, right, joinType, _, _)
            if pkfk.mode == ExecMode.Computation || pkfk.mode == ExecMode.DelayComputation =>
          Seq(
            LocalJoinExec(
              pkfk.children.map(localPlanLater),
              joinType,
              dummyAttributeOrder
            )
          )
        case p @ Project(
              child,
              projectionList,
              _,
              _
            )
            if p.mode == ExecMode.Computation || p.mode == ExecMode.DelayComputation =>
          Seq(
            LocalProjectExec(
              localPlanLater(child),
              projectionList,
              dummyAttributeOrder
            )
          )
        case a @ Aggregate(
              child,
              groupingList,
              semiringList,
              _,
              _,
              _,
              _
            )
            if a.mode == ExecMode.Computation || a.mode == ExecMode.DelayComputation =>
          Seq(
            LocalSemiringAggregateExec(
              localPlanLater(child),
              groupingList,
              a.producedOutputOld.head,
              semiringList,
              dummyAttributeOrder
            )
          )
        case placeHolder @ PlaceHolder(pos, output) =>
          Seq(LocalPlaceHolderExec(pos, output, dummyAttributeOrder))
        case _ => Seq()
      }

  }

  object AtomicStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SeccoPlan] =
      plan match {
        case c @ Cache(child, _) if !child.isInstanceOf[Partition] =>
          Seq(CacheExec(planLater(child)))
        case a @ Assign(child, tableIdentifier, _) =>
          Seq(AssignExec(planLater(child), tableIdentifier))
        case u @ Update(child, tableIdentifier, deltaTableName, key, _) =>
          Seq(
            UpdateExec(planLater(child), tableIdentifier, deltaTableName, key)
          )
        case i @ Iterative(child, returnTableIdentifier, numRun, _) =>
          Seq(IterativeExec(planLater(child), returnTableIdentifier, numRun))
        case t @ Transform(child, f, output, _) =>
          Seq(TransformExec(planLater(child), f, output))
        case _ => Seq()
      }
  }

}
