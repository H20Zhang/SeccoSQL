//package unit.execution.plan.communication.utils
//
//import org.apache.spark.secco.execution.plan.communication.utils.EnumShareComputer
//import util.{SeccoFunSuite, UnitTestTag}
//
//class ShareComputerSuite extends SeccoFunSuite {
//
//  test("basic", UnitTestTag) {
//
//    val conf = dlSession.sessionState.conf
//
//    val schemas = Seq(
//      Seq("b", "c"),
//      Seq("a", "e"),
//      Seq("a", "b"),
//      Seq("b", "e"),
//      Seq("c", "d"),
//      Seq("d", "e")
//    )
//    val constraint = Map("a" -> 1, "c" -> 1, "d" -> 1)
//    val tasks = 196
//    val statisticMap = schemas
//      .map(f => (f, 22190596L))
//      .toMap
//
//    val shareComputer =
//      new EnumShareComputer(schemas, constraint, tasks, statisticMap)
//
//    val shareResults =
//      shareComputer.optimalShareWithBudget()
//
//    pprint.pprintln(shareResults)
//
//  }
//}
