//package unit.execution.plan.communication
//
//import org.apache.spark.secco.execution.plan.communication.utils.PairPartitioner
//import org.apache.spark.secco.execution.plan.communication.{
//  IterativePairExchangeExec,
//  PartitionExchangeExec,
//  PullPairExchangeExec
//}
//import org.apache.spark.secco.execution.{
//  MultiTableIndexedBlock,
//  RowIndexedBlock,
//  SharedParameter
//}
//import org.apache.spark.secco.util.misc.Timer
//import util.{SeccoFunSuite, TestDataGenerator, UnitTestTag}
//
//import scala.collection.mutable
//
//class IterativePairExchangeExecSuite extends SeccoFunSuite {
//
//  test("IterativePullPairExchangeExec", UnitTestTag) {
//
//    // Prepare
//    val cardinality = 1000
//    val upperBound = 100
//    val scanR1 = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
//      "R1",
//      "A" :: "B" :: Nil,
//      cardinality,
//      upperBound
//    )
//    val scanR2 = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
//      "R2",
//      "B" :: "C" :: Nil,
//      cardinality,
//      upperBound
//    )
//    val scanR3 = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
//      "R3",
//      "C" :: "D" :: Nil,
//      cardinality,
//      upperBound
//    )
//    val sharedShare = SharedParameter(mutable.HashMap[String, Int]())
//    val partitionR1 = PartitionExchangeExec(scanR1, sharedShare)
//    val partitionR2 = PartitionExchangeExec(scanR2, sharedShare)
//    val partitionR3 = PartitionExchangeExec(scanR3, sharedShare)
//    val children = partitionR3 :: partitionR2 :: partitionR1 :: Nil
//    val pullPairExchangeExec = PullPairExchangeExec(
//      partitionR1 :: partitionR2 :: Nil,
//      Map(),
//      sharedShare
//    )
//
//    val orderRearrnage = Seq(2, 1, 0)
//    val iterativePullPairExchangeExec = IterativePairExchangeExec(
//      pullPairExchangeExec :: partitionR3 :: Nil,
//      orderRearrnage,
//      Map(),
//      sharedShare
//    )
//
//    // Testing
//    Range(0, 3).foreach { idx =>
//      pprint.pprintln(s"idx:${idx}")
//      val timer = new Timer
//      timer.start()
//      iterativePullPairExchangeExec.execute().foreachPartition { it =>
//        val block = it.next()
//        block match {
//          case MultiTableIndexedBlock(output, shareVector, blockContents) =>
//            //DEBUG
//            // println(s"output:${output}, shareVector:${shareVector.toSeq}")
//
//            blockContents
//              .map(_.asInstanceOf[RowIndexedBlock])
//              .zipWithIndex
//              .foreach {
//                case (indexedRowBlock, index) =>
//                  //DEBUG
//                  //                println(
//                  //                  s"localOutput:${rowHCubeBlock.output},localShareVector:${rowHCubeBlock.shareVector.toSeq},blockContents:${rowHCubeBlock.blockContent.content
//                  //                    .map(_.mkString("(", ",", ")"))
//                  //                    .toSeq}"
//                  //                )
//                  val partitioners =
//                    children
//                      .map(f =>
//                        new PairPartitioner(
//                          f.outputOld.map(sharedShare.res).toArray
//                        )
//                      )
//                  indexedRowBlock.blockContent.content.forall(row =>
//                    partitioners(index).getPartition(row) == partitioners(index)
//                      .getPartition(
//                        indexedRowBlock.index
//                      )
//                  )
//              }
//        }
//
//        it.hasNext
//      }
//      timer.`end`()
//      pprint.pprintln(s"executed in ${timer}ms")
//    }
//
//  }
//
//}
