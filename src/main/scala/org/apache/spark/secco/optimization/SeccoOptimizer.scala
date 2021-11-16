package org.apache.spark.secco.optimization

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

    //TODO: order the optimization rules.

    val operatorOptimizationRules = Seq(
      Batch(
        "Operator Optimization",
        fixedPoint,
        // Operator push down
        PushDownSelection,
        PushSelectionIntoJoin,
        PushSelectionThroughJoin,
        PushDownProjection,
        // Operator Combine
        MergeUnion,
        MergeProjection,
        MergeSelection,
        MergeLimit,
        // Remove unnecessary operator
        RemoveRedundantSelection,
        RemoveRedundantProjection,
        // Optimize expression via constant folding
        ConstantPropagation,
        ConstantFolding,
        ReorderAssociativeOperator,
        BooleanSimplification,
        SimplifyBinaryComparison,
        RemoveDispensableExpressions
      )
    )

    val joinOptimizationRules = Seq(
      Batch(
        "Optimize PrimaryKey-ForeignKey Join",
        Once,
        MarkJoinPredicateProperty,
        MarkJoinIntegrityConstraintProperty,
        OptimizePKFKJoin
      ),
      Batch(
        "Optimize ForeignKey-ForeignKey Join",
        Once,
        MarkJoinCyclicityProperty,
        MarkJoinPredicateProperty,
        MarkJoinIntegrityConstraintProperty,
        // Combine consecutive cyclic FKFK-Join into multiway join.
        ReplaceBinaryJoinWithMultiwayJoin,
        // Optimize multiway join by GHD
        OptimizeMultiwayJoin,
        // Optimize the join tree produced by GHD
        OptimizeJoinTree
      )
    )

    val decoupleOptimizationRules = Seq(
      Batch(
        "Mark Delay and Decouple Communication from Computation",
        Once,
        MarkDelay,
        DecoupleOperators
      ),
      Batch(
        "Pack Local Computations into Local Stage",
        fixedPoint,
        PackLocalComputationIntoLocalStage,
        MergeLocalStage
      ),
      Batch(
        "Selective PartitionPushDown",
        fixedPoint,
        SelectivelyPushCommunicationThroughComputation,
        MergeLocalStage
      )
    )

    val iterativeComputationOptimizationRules = Seq(
      Batch(
        "AddCache",
        Once,
        AddCache,
        RemoveRedundantCache
      )
    )

    val cleanRules = Seq(Batch("Cleaning", Once, CleanRoot))

    if (conf.enableOnlyDecoupleOptimization) {
      decoupleOptimizationRules ++ iterativeComputationOptimizationRules ++ cleanRules
    } else {
      operatorOptimizationRules ++ joinOptimizationRules ++ decoupleOptimizationRules ++ iterativeComputationOptimizationRules ++ cleanRules
    }
  }

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  override protected def batches: Seq[Batch] = {
    defaultBatches.filter(batch => validBatches(batch))
  }

  /** Current active batches. */
  def activeBatches: Seq[Batch] = batches
}
