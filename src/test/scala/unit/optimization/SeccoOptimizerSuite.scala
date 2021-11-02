//package unit.optimization
//
//import org.apache.spark.secco.optimization.SeccoOptimizer
//import org.apache.spark.secco.optimization.plan.LocalStage
//import org.apache.spark.secco.optimization.rules.MarkDelay.dlSession
//import util.{SeccoFunSuite, UnitTestTag}
//
//import scala.util.Try
//
//class SeccoOptimizerSuite extends SeccoFunSuite {
//
//  test("check_initialization", UnitTestTag) {
//    //initialize optimizer
//    val optimizer = new SeccoOptimizer()
//  }
//
//  test("check_optimizer_execute", UnitTestTag) {
//    import org.apache.spark.secco.analysis.deprecated.RelationAlgebraWithAnalysis._
//
//    val optimizer = new SeccoOptimizer()
//    val expr = select("A<B", join("R1(A, B, C)", "R2(A, B)", "R3(B, C)"))
//    val optimizedExpr = optimizer.execute(expr)
//
//    assert(optimizedExpr.isInstanceOf[LocalStage])
//  }
//
//  test("check_disable_and_enable_rules", UnitTestTag) {
//
//    val optimizer = dlSession.sessionState.optimizer
//    val allBatchName = optimizer.activeBatches.map(_.name)
//
//    //disable all batch
//    optimizer.setAllBatchDisabled()
//    assert(optimizer.activeBatches.isEmpty)
//
//    //enable batch
//    optimizer.setBatchesEnabled(
//      "Push Down Predicates",
//      "PartitionPushDown"
//    )
//
//    assert(
//      optimizer.activeBatches
//        .map(_.name) == Seq(
//        "Push Down Predicates",
//        "PartitionPushDown"
//      )
//    )
//
//    assert(Try(optimizer.setBatchesEnabled("NotExistBatch")).isFailure)
//
//    //disable batch
//    optimizer.setBatchesDisabled("Push Down Predicates")
//
//    assert(
//      optimizer.activeBatches
//        .map(_.name) == Seq(
//        "PartitionPushDown"
//      )
//    )
//
//    assert(Try(optimizer.setBatchesDisabled("NotExistBatch")).isFailure)
//
//    //enable all batch
//    optimizer.setAllBatchEnabled()
//    println(optimizer.activeBatches.map(_.name) == allBatchName)
//
//  }
//
//  test(
//    "enable_only_decouple_optimization",
//    UnitTestTag
//  ) {
//    val conf = dlSession.sessionState.conf
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
//
//}
