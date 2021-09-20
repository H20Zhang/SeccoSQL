package unit.execution.plan.computation.optimize

import org.apache.spark.secco.execution.SharedParameter
import org.apache.spark.secco.execution.plan.computation.optimize.AttributeOrderComputer
import org.apache.spark.secco.execution.plan.computation.{
  LocalJoinExec,
  LocalPlaceHolderExec,
  LocalProcessingExec
}
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper
import org.apache.spark.secco.optimization.plan.JoinType
import org.apache.spark.secco.optimization.statsEstimation.Statistics
import util.{SeccoFunSuite, UnitTestTag}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//TODO: more testing needed
class AttributeOrderComputerSuite extends SeccoFunSuite {

  def genDummyPlaceHolderStatisticKeeper(
      placeHolders: Seq[LocalPlaceHolderExec]
  ): Seq[StatisticKeeper] = {
    placeHolders.map { placeHolderExec =>
      val recordMap = mutable.HashMap[Set[String], Long]()
      val attributes = placeHolderExec.outputOld

      for (i <- 1 to attributes.size) {
        placeHolderExec.outputOld.combinations(i).foreach { subAttributes =>
          recordMap(subAttributes.toSet) =
            math.pow(10, subAttributes.size).toLong
        }
      }
      val statsKeeper = StatisticKeeper(placeHolderExec)

      statsKeeper.setStatistic(
        Statistics(rowCount = 0, fullCardinality = recordMap)
      )

      statsKeeper
    }
  }

  //TODO: clean this up
  test("genDummyPlaceHolderStatistics", UnitTestTag) {

    val sharedParameter = SharedParameter(ArrayBuffer[String]())
    val placeHolderExec1 = {
      LocalPlaceHolderExec(1, Seq("A", "B"), sharedParameter)
    }
    val placeHolderExec2 = {
      LocalPlaceHolderExec(2, Seq("B", "C"), sharedParameter)
    }

    val placeHolders = Seq(placeHolderExec1, placeHolderExec2)
    val placeHolderStatistics = genDummyPlaceHolderStatisticKeeper(placeHolders)

    println(s"placeHolders:")
    pprint.pprintln(placeHolders)

    println(s"placeHolderStatistics:")
    pprint.pprintln(placeHolderStatistics)
  }

  test("simple", UnitTestTag) {
    val sharedParameter = SharedParameter(ArrayBuffer[String]())
    val placeHolderExec0 =
      LocalPlaceHolderExec(0, Seq("A", "B"), sharedParameter)
    val placeHolderExec1 =
      LocalPlaceHolderExec(1, Seq("B", "C"), sharedParameter)
    val placeHolderExec2 =
      LocalPlaceHolderExec(2, Seq("C", "D"), sharedParameter)

    val placeHolders = Seq(placeHolderExec0, placeHolderExec1, placeHolderExec2)
    val placeHolderStatistics = genDummyPlaceHolderStatisticKeeper(placeHolders)

    var exec: LocalProcessingExec = null

    //pure GHDJoin
//    exec = LocalJoinExec(
//      placeHolderExec0 :: placeHolderExec1 :: Nil,
//      JoinType.GHD,
//      sharedParameter
//    )

    //pure FKFKJoin
//    exec = LocalJoinExec(
//      placeHolderExec0 :: placeHolderExec1 :: Nil,
//      JoinType.GHDFKFK,
//      sharedParameter
//    )

//    val temp1 = LocalJoinExec(
//      placeHolderExec1 :: placeHolderExec0 :: Nil,
//      JoinType.GHDFKFK,
//      sharedParameter
//    )
//
//    exec = LocalJoinExec(
//      placeHolderExec2 :: temp1 :: Nil,
//      JoinType.GHDFKFK,
//      sharedParameter
//    )

    //pure PKFKJoin

    val temp1 = LocalJoinExec(
      placeHolderExec1 :: placeHolderExec0 :: Nil,
      JoinType.GHDFKFK,
      sharedParameter
    )

    exec = LocalJoinExec(
      placeHolderExec2 :: temp1 :: Nil,
      JoinType.PKFK,
      sharedParameter
    )

    //join with project
//    val temp1 = LocalProjectExec(
//      LocalJoinExec(
//        placeHolderExec1 :: placeHolderExec0 :: Nil,
//        JoinType.GHDFKFK,
//        sharedParameter
//      ),
//      Seq("A", "C"),
//      true,
//      sharedParameter
//    )
//
//    exec = LocalJoinExec(
//      placeHolderExec2 :: temp1 :: Nil,
//      JoinType.GHDFKFK,
//      sharedParameter
//    )

    //join with aggregate
//    val temp1 = LocalAggregateExec(
//      LocalJoinExec(
//        placeHolderExec1 :: placeHolderExec0 :: Nil,
//        JoinType.GHDFKFK,
//        sharedParameter
//      ),
//      Seq("A", "C"),
//      Seq("A", "C", "w"),
//      ("count", "*"),
//      sharedParameter
//    )
//
//    exec = LocalJoinExec(
//      placeHolderExec2 :: temp1 :: Nil,
//      JoinType.GHDFKFK,
//      sharedParameter
//    )

    //join with project and aggregate
//    val temp1 = LocalProjectExec(
//      LocalJoinExec(
//        placeHolderExec1 :: placeHolderExec0 :: Nil,
//        JoinType.GHDFKFK,
//        sharedParameter
//      ),
//      Seq("A", "C"),
//      true,
//      sharedParameter
//    )
//
//    val temp2 = LocalJoinExec(
//      placeHolderExec2 :: temp1 :: Nil,
//      JoinType.GHDFKFK,
//      sharedParameter
//    )
//
//    exec = LocalAggregateExec(
//      temp2,
//      Seq("A"),
//      "w",
//      ("sum", "D"),
//      sharedParameter
//    )

    //TODO: queries that involve PKFKJoin

    val attributeOrderComputer =
      new AttributeOrderComputer(
        exec,
        sharedParameter.res,
        placeHolderStatistics
      )
    val attributeOrder = attributeOrderComputer.genAttributeOrder()
    val postProcessedRootPlan =
      attributeOrderComputer.genProcessedLocalPlan()

    println(s"attributeOrder:")
    pprint.pprintln(attributeOrder)

    println(s"rootPlanWIthBaseAndIndexDetermined:")
    println(postProcessedRootPlan)
  }

}
