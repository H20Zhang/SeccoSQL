package unit.optimization

import org.apache.spark.secco.optimization.SeccoOptimizer
import util.{SeccoFunSuite, UnitTestTag}

import scala.util.Try

class SeccoOptimizerSuite extends SeccoFunSuite {

  test("check_disable_and_enable_rules", UnitTestTag) {

    val optimizer = seccoSession.sessionState.optimizer
    val allBatchName = optimizer.activeBatches.map(_.name)

    println(allBatchName)

    // Disable all batches.
    optimizer.setAllBatchDisabled()
    assert(optimizer.activeBatches.isEmpty)

    // Enable two batch.
    optimizer.setBatchesEnabled(
      "Operator Optimization",
      "Optimize PrimaryKey-ForeignKey Join"
    )

    // Check te current enabled batches.
    assert(
      optimizer.activeBatches
        .map(_.name) == Seq(
        "Operator Optimization",
        "Optimize PrimaryKey-ForeignKey Join"
      )
    )

    assert(Try(optimizer.setBatchesEnabled("NotExistBatch")).isFailure)

    // Disable batch.
    optimizer.setBatchesDisabled("Operator Optimization")

    assert(
      optimizer.activeBatches
        .map(_.name) == Seq(
        "Optimize PrimaryKey-ForeignKey Join"
      )
    )

    assert(Try(optimizer.setBatchesDisabled("NotExistBatch")).isFailure)

    // Enable all batches.
    optimizer.setAllBatchEnabled()
    assert(optimizer.activeBatches.map(_.name) == allBatchName)

  }

//  test(
//    "enable_only_decouple_optimization",
//    UnitTestTag
//  ) {
//    val conf = seccoSession.sessionState.conf
//    conf.setEnableOnlyDecoupleOptimization(true)
//
//    val ruleBeforeDecouple = Seq(
//      "Clean up",
//      "Remove Redundant Operators",
//      "Push Down Predicates",
//      "GHD-Based Join Reorder",
//      "Aggregation Push-Down",
//      "Projection Push-Down",
//      "Projection Cleaning",
//      "Optimize GHD Node"
//    )
//
//    val optimizer = dlSession.sessionState.optimizer
//
//    assert(
//      optimizer.activeBatches.map(_.name).intersect(ruleBeforeDecouple).isEmpty
//    )
//
//  }

}
