package unit.execution.plan.communication

import org.apache.spark.secco.execution.plan.communication.utils.PairPartitioner
import org.apache.spark.secco.execution.plan.communication.{
  PartitionExchangeExec,
  PullPairExchangeExec,
  PushPairExchangeExec
}
import org.apache.spark.secco.execution.{
  MultiTableIndexedBlockOld,
  RowIndexedBlockOld,
  SharedParameter
}
import util.{SeccoFunSuite, TestDataGenerator, UnitTestTag}

import scala.collection.mutable
import scala.util.Random

class PairExchangeSuite extends SeccoFunSuite {

  test("PairPartitioner", UnitTestTag) {

    val artys = Range(0, 8)

    artys.foreach { artiy =>
      Range(0, 10).foreach { _ =>
        val shareSpaceVector =
          Range(0, artiy).map(f => Math.abs(Random.nextInt(20) + 1)).toArray
        val partitioner = new PairPartitioner(shareSpaceVector)
        Range(0, 10).foreach { _ =>
          val tuple =
            Range(0, shareSpaceVector.size)
              .map(f => Random.nextInt())
              .toArray
          val serverId = partitioner.getPartition(tuple)
          val coordinate = partitioner.getCoordinate(serverId)
          val _serverId = partitioner.getServerId(coordinate)
          val _coordinate = partitioner.getCoordinate(_serverId)
          assert(
            coordinate.toSeq == _coordinate.toSeq,
            s"coordinate:${coordinate} != _coordinate:${_coordinate}"
          )
        }
      }
    }

  }

  test("PartitionExchangeExec", UnitTestTag) {
    val arities = Range(1, 8)
    val numRun = 1
    val cardinality = 100
    val upperBound = 100

    Range(0, numRun).foreach { run =>
      arities.foreach { artiy =>
        val attrs = Range(0, artiy).map(_.toString)
        val scanExec =
          TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
            "R1",
            attrs,
            cardinality,
            upperBound
          )
        println(s"scanExec.execute().count():${scanExec.execute().count()}")
        val sharedShare =
          SharedParameter(
            mutable.HashMap(
              attrs.map(attr => (attr, Random.nextInt(5) + 1)): _*
            )
          )

        val partitionExec = PartitionExchangeExec(scanExec, sharedShare)
        println(
          s"sharedShare:${sharedShare}"
        )
        val partitioner = partitionExec.taskPartitioner
        partitionExec
          .execute()
          .map { f =>
            f match {
              case r @ RowIndexedBlockOld(output, shareVector, blockContent) =>
                //DEBUG
                //              println(
                //                s"output:${output}, shareVector:${shareVector.toSeq}, blockContent:${blockContent.content.toSeq}"
                //              )
                blockContent.content.forall(row =>
                  partitioner.getPartition(row) == partitioner
                    .getPartition(shareVector)
                )

              case _ => throw new Exception("fail")
            }
            f
          }
          .count()
      }
    }

  }

//  test("PushHCubeExchangeExec", UnitTestTag) {
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
//    val sharedShare = SharedParameter(mutable.HashMap[String, Int]())
//    val output = "A" :: "B" :: "C" :: Nil
//    val children = scanR1 :: scanR2 :: Nil
//    val pushHCubeExchangeExec =
//      PushHCubeExchangeExec(children, Map(), sharedShare)
//
//    val partitioners =
//      children.map(f =>
//        new HCubePartitioner(f.outputOld.map(sharedShare.res).toArray)
//      )
//
//    pushHCubeExchangeExec.execute().foreach { block =>
//      block match {
//        case MultiTableHCubeBlock(output, shareVector, blockContents) =>
////          //DEBUG
////          println(s"output:${output}, shareVector:${shareVector.toSeq}")
//
//          blockContents
//            .map(_.asInstanceOf[RowHCubeBlock])
//            .zipWithIndex
//            .foreach {
//              case (rowHCubeBlock, index) =>
//                //DEBUG
////                println(
////                  s"localOutput:${rowHCubeBlock.output},localShareVector:${rowHCubeBlock.shareVector.toSeq},blockContents:${rowHCubeBlock.blockContent.content
////                    .map(_.mkString("(", ",", ")"))
////                    .toSeq}"
////                )
//                rowHCubeBlock.blockContent.content.forall(row =>
//                  partitioners(index).getPartition(row) == partitioners(index)
//                    .getPartition(
//                      rowHCubeBlock.shareVector
//                    )
//                )
//            }
//      }
//    }
//
//  }

  test("PullPairExchangeExec", UnitTestTag) {

    // Prepare
    val cardinality = 3
    val upperBound = 100
    val scanR1 = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
      "R1",
      "A" :: "B" :: Nil,
      cardinality,
      upperBound
    )
    val scanR2 = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
      "R2",
      "C" :: "D" :: Nil,
      cardinality,
      upperBound
    )
    val sharedShare = SharedParameter(mutable.HashMap[String, Int]())
    val output = "A" :: "B" :: "C" :: "D" :: Nil
    val partitionR1 = PartitionExchangeExec(scanR1, sharedShare)
    val partitionR2 = PartitionExchangeExec(scanR2, sharedShare)
    val children = partitionR1 :: partitionR2 :: Nil
    val pushPairExchangeExec =
      PullPairExchangeExec(children, Map(), sharedShare)

    // Testing
    pushPairExchangeExec.execute().foreachPartition { it =>
      val block = it.next()
      block match {
        case MultiTableIndexedBlockOld(output, shareVector, blockContents) =>
          //DEBUG
          // println(s"output:${output}, shareVector:${shareVector.toSeq}")

          blockContents
            .map(_.asInstanceOf[RowIndexedBlockOld])
            .zipWithIndex
            .foreach { case (indexedRowBlock, index) =>
              //DEBUG
              println(
                s"localOutput:${indexedRowBlock.output},localShareVector:${indexedRowBlock.index.toSeq},blockContents:${indexedRowBlock.blockContent.content
                  .map(_.mkString("(", ",", ")"))
                  .toSeq}"
              )
              val partitioners =
                children
                  .map(f =>
                    new PairPartitioner(
                      f.outputOld.map(sharedShare.res).toArray
                    )
                  )
              indexedRowBlock.blockContent.content.forall(row =>
                partitioners(index).getPartition(row) == partitioners(index)
                  .getPartition(
                    indexedRowBlock.index
                  )
              )
            }
      }

      it.hasNext
    }

  }
}
