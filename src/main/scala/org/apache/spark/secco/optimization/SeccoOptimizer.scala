package org.apache.spark.secco.optimization

import javax.swing.plaf.basic.BasicTableHeaderUI
import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.optimization.rules._
import org.apache.spark.secco.trees.RuleExecutor

/** The optimizer of the Secco, which accepts a series of [[org.apache.spark.secco.optimization.Rule]]
  * and apply the rules to transform the plan
  *
  * @param conf current session's configuration
  */
class SeccoOptimizer(
    conf: SeccoConfiguration = SeccoSession.currentSession.sessionState.conf
) extends RuleExecutor[LogicalPlan] {

  protected def fixedPoint = FixedPoint(conf.maxIteration)

  def containsBatch(batchName: String): Boolean =
    validBatches.filter(_._1.name == batchName).nonEmpty

  /** disable batches with `batchNames`
    * @param batchNames name of batches to disable
    * @return self
    */
  def setBatchesDisabled(batchNames: String*): SeccoOptimizer = {
    val containBatchName = batchNames.toSet

    assert(
      batchNames.forall(containsBatch),
      s"batches with name:${batchNames} is not contained in optimizer"
    )

    validBatches = validBatches.map { f =>
      if (containBatchName(f._1.name)) {
        (f._1, false)
      } else {
        f
      }
    }

    this
  }

  /** enable batches with `batchNames`
    * @param batchNames name of batches to enable
    * @return self
    */
  def setBatchesEnabled(batchNames: String*): SeccoOptimizer = {
    val containBatchName = batchNames.toSet

    assert(
      batchNames.forall(containsBatch),
      s"batches with name:${batchNames} is not contained in optimizer"
    )

    validBatches = validBatches.map { f =>
      if (containBatchName(f._1.name)) {
        (f._1, true)
      } else {
        f
      }
    }

    this
  }

  /** Disable all batches.
    * @return self
    */
  def setAllBatchDisabled(): SeccoOptimizer = {
    validBatches = validBatches.map(f => (f._1, false))
    this
  }

  /** Enable all batches.
    * @return self
    */
  def setAllBatchEnabled(): SeccoOptimizer = {
    validBatches = validBatches.map(f => (f._1, true))
    this
  }

  /** The batches of rules that are enabled
    * @return self
    */
  private var validBatches = defaultBatches.map(f => (f, true)).toMap

  /** The default batches of rules */
  private def defaultBatches = {
    val basicRules = Seq(
      Batch("Clean up", Once, PushRenameToLeaf),
      Batch(
        "Remove Redundant Operators",
        fixedPoint,
        MergeAllJoin,
        MergeUnion,
        MergeProjection,
        MergeSelection,
        RemoveRedundantSelection
      ),
      Batch(
        "Push Down Predicates",
        fixedPoint,
        PushSelectionThroughJoin,
        PushSelectionThroughJoin,
        PushSelectionThroughUnion,
        PushProjectionThroughUnion
      )
    )

    val advanceRules = Seq(
      Batch("ExtractPKFKJoin", fixedPoint, OptimizePKFKJoin),
      Batch(
        "GHD-Based Join Reorder",
        fixedPoint,
        OptimizeMultiwayJoin,
        ExpandGHDNode
      ),
      Batch(
        "Aggregation Push-Down",
        Once,
        PushDownAggregation
      ),
      Batch(
        "Projection Push-Down",
        Once,
        PushProjectionThroughJoin
      ),
      Batch(
        "Projection Cleaning",
        fixedPoint,
        MergeProjection,
        RemoveRedundantProjection
      ),
      Batch("Optimize GHD Node", fixedPoint, ConsecutiveJoinReorder)
    )

    val decouplingRules = Seq(
      Batch("Mark Delay", Once, MarkDelay),
      Batch("Merge Join", fixedPoint, MergeDelayedJoin),
      Batch(
        "Decouple Operators",
        Once,
        DecoupleOperators
      ),
      Batch(
        "Pack Local Computations",
        fixedPoint,
        PackLocalComputationIntoLocalStage,
        MergeLocalStage
      ),
      Batch(
        "PartitionPushDown",
        fixedPoint,
        SelectivelyPushCommunicationThroughComputation,
        MergeLocalStage
      )
    )

    val iterativeProcessingRules = Seq(
      Batch(
        "AddCache",
        Once,
        AddCache,
        RemoveRedundantCache
      )
    )

    val cleanRules = Seq(Batch("Cleanning", Once, CleanRoot))

    if (conf.enableOnlyDecoupleOptimization) {
      decouplingRules ++ iterativeProcessingRules ++ cleanRules
    } else {
      basicRules ++ advanceRules ++ decouplingRules ++ iterativeProcessingRules ++ cleanRules
    }
  }

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  override protected def batches: Seq[Batch] = {
    defaultBatches.filter(batch => validBatches(batch))
  }

  /** Currently, active batches. */
  def activeBatches: Seq[Batch] = batches
}
