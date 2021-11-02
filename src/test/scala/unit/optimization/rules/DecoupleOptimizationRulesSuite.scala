//package unit.optimization.rules
//
//import org.apache.spark.secco.Dataset
//import org.apache.spark.secco.execution.SharedParameter
//import org.apache.spark.secco.optimization.ExecMode
//import org.apache.spark.secco.optimization.plan.{
//  MultiwayJoin,
//  LocalStage,
//  Partition,
//  RootNode
//}
//import org.apache.spark.secco.optimization.rules._
//import util.{SeccoFunSuite, UnitTestTag}
//
//import scala.collection.mutable
//
///** This class contains testing related to rules and operators about decoupling communication
//  * and computation
//  */
//class DecoupleOptimizationRulesSuite extends SeccoFunSuite {
//
//  override def setupDB(): Unit = {
//    super.setupDB()
//
//    Dataset.empty("R1", "a" :: "b" :: "c" :: Nil)
//    Dataset.empty("R2", "a" :: "d" :: "e" :: Nil)
//    Dataset.empty("R3", "a" :: "f" :: "g" :: Nil)
//  }
//
//  def ds1 = dlSession.table("R1")
//  def ds2 = dlSession.table("R2")
//  def ds3 = dlSession.table("R3")
//
//  /** Test decoupled operators */
//  test("operators", UnitTestTag) {
//
//    val plan =
//      RootNode(ds1.join(ds2).project("a, b, d").join(ds3).logical)
//
//    println(plan)
//
//    val R1 = ds1.logical
//    val R2 = ds2.logical
//    val R3 = ds3.logical
//
//    //partition
//    val restriction1 = SharedParameter(mutable.HashMap[String, Int]())
//    val p1 = Partition(R1, restriction1)
//    val p2 = Partition(R2, restriction1)
//
//    //localStage
//    val l1 = LocalStage.box(
//      Seq(p1, p2),
//      MultiwayJoin(Seq(p1, p2), mode = ExecMode.Computation)
//    )
//    println(l1)
//
//    val restriction2 = SharedParameter(mutable.HashMap[String, Int]())
//    val p3 = Partition(R3, restriction2)
//    val l2 = LocalStage.box(
//      Seq(p3, l1),
//      MultiwayJoin(Seq(p3, l1), mode = ExecMode.Computation)
//    )
//    println(l2)
//
//    val l3 = l2.mergeConsecutiveLocalStage()
//    println(l3)
//
//  }
//
//  /** Test if the rules related to decoupling the operators works well */
//  test("rules", UnitTestTag) {
//
//    val plan =
//      RootNode(ds1.join(ds2).project("a, b, d").join(ds3).logical)
//
//    println(plan)
//
//    val markedDelayPlan = MarkDelay.apply(plan)
//    println(markedDelayPlan)
//
//    val decoupledPlan = DecoupleOperators.apply(markedDelayPlan)
//    println(decoupledPlan)
//
//    val packedLocalStagePlan =
//      PackLocalComputationIntoLocalStage.apply(decoupledPlan)
//    println(packedLocalStagePlan)
//
//    val computationDelayedPlan =
//      MergeLocalStage(
//        SelectivelyPushCommunicationThroughComputation(
//          MergeLocalStage(
//            SelectivelyPushCommunicationThroughComputation(packedLocalStagePlan)
//          )
//        )
//      )
//    println(computationDelayedPlan)
//  }
//}
