//package unit.optimization.rules
//
//import org.apache.spark.secco.Dataset
//import org.apache.spark.secco.optimization.plan._
//import org.apache.spark.secco.optimization.rules.{
//  ConsecutiveJoinReorder,
//  ExpandGHDNode,
//  OptimizePKFKJoin,
//  OptimizeMultiwayJoin
//}
//import util.{SeccoFunSuite, UnitTestTag}
//
///** This class contains testing of join optimization rules */
//class JoinOptimizationRulesSuite extends SeccoFunSuite {
//
//  val optimizer = dlSession.sessionState.optimizer
//
//  override def setupDB(): Unit = {
//
//    super.setupDB()
//
//    Dataset.empty("R1", Seq("A", "B"))
//    Dataset.empty("R2", Seq("B", "C"))
//    Dataset.empty("R3", Seq("A", "C"))
//    Dataset.empty("R4", Seq("A", "D"), _primaryKey = Some(Seq("A")))
//    Dataset.empty("R5", Seq("B", "E"), _primaryKey = Some(Seq("B")))
//    Dataset.empty("R6", Seq("C", "F"), _primaryKey = Some(Seq("C")))
//    Dataset.empty("R7", Seq("B", "G"))
//    Dataset.empty("R8", Seq("C", "G"))
//  }
//
//  def R1 = dlSession.table("R1")
//  def R2 = dlSession.table("R2")
//  def R3 = dlSession.table("R3")
//  def R4 = dlSession.table("R4")
//  def R5 = dlSession.table("R5")
//  def R6 = dlSession.table("R6")
//  def R7 = dlSession.table("R7")
//  def R8 = dlSession.table("R8")
//
//  /** Test extractPKFKJoin */
//  test("ExtractPKFKJoin", UnitTestTag) {
//
//    val expr = R1.join(R2, R3, R4, R5, R6).logical
//
//    val optimizedExpr = OptimizePKFKJoin(expr)
//
//    println(optimizedExpr)
//  }
//
//  /** Test GHD-Based join reordering */
//  test("GHD_Reorder", UnitTestTag) {
//
//    val expr =
//      R1.join(R2, R3, R4, R5, R6, R7, R8).logical
//
//    val optimizedExpr =
//      ExpandGHDNode(OptimizeMultiwayJoin(OptimizePKFKJoin(expr)))
//
//    println(optimizedExpr)
//  }
//
//  test("consecutive_join_reorder", UnitTestTag) {
//    val expr =
//      R1
//        .join(R2, R3, R4, R5, R6, R7, R8)
//        .logical
//        .transform { case j: MultiwayJoin =>
//          j.copy(joinType = JoinType.GHD)
//        }
//
//    val optimizedExpr = ConsecutiveJoinReorder(expr)
//
//    println(optimizedExpr)
//  }
//
//  /** Test projection and aggregation push-down along the GHD Tree */
//  test("push_down", UnitTestTag) {
//
//    val expr1 =
//      R1
//        .join(R2, R3, R4, R5, R6, R7, R8)
//        .project("A")
//        .logical
//
//    println(expr1)
//    val optimizedExpr1 = optimizer.execute(expr1)
//    println(optimizedExpr1)
//
//    val expr2 = R1
//      .join(R2, R3, R4, R5, R6, R7, R8)
//      .aggregate("min(E)")
//      .logical
//
//    println(expr2)
//    val optimizedExpr2 = optimizer.execute(expr2)
//    println(optimizedExpr2)
//
//    val expr3 = R1
//      .join(R2, R3, R4, R5, R6, R7, R8)
//      .aggregate("sum(E) by A,B")
//      .logical
//
//    println(expr3)
//    val optimizedExpr3 = optimizer.execute(expr3)
//    println(optimizedExpr3)
//  }
//
//}
