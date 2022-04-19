package unit.execution.plan.communication

import org.apache.spark.secco.execution.plan.communication.{
  PartitionExchangeExec,
  PullPairExchangeExec,
  ShareConstraint,
  ShareConstraintContext,
  ShareValues,
  ShareValuesContext
}
import org.apache.spark.secco.execution.storage.PairedPartition
import org.apache.spark.secco.expression.AttributeReference
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.optimization.util.EquiAttributes
import org.apache.spark.secco.types.DataTypes
import util.SeccoFunSuite

class PullPairExchangeExecSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {

    val R1Rows = Seq(Seq(0, 1, 1), Seq(2, 3, 3), Seq(3, 4, 4))
    val R2Rows = Seq(Seq(0, 1, 1), Seq(2, 3, 3), Seq(3, 4, 4))

    createTestRelation(
      R1Rows,
      "R1",
      "a",
      "b",
      "c"
    )()

    createTestRelation(
      R2Rows,
      "R2",
      "d",
      "e",
      "f"
    )()
  }

  test("simple") {
    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")

    val scanR1Exec = R1.queryExecution.executionPlan
    val scanR2Exec = R2.queryExecution.executionPlan

    val attrA = scanR1Exec.output(0)
    val attrB = scanR1Exec.output(1)
    val attrC = scanR1Exec.output(2)
    val attrD = scanR2Exec.output(0)
    val attrE = scanR2Exec.output(1)
    val attrF = scanR2Exec.output(2)

    val equiAttrs =
      EquiAttributes.fromEquiAttributes(
        Seq(Seq(attrA), Seq(attrB, attrE), Seq(attrC), Seq(attrD), Seq(attrF))
      )
    val rawShares =
      AttributeMap(
        Seq(
          (attrA, 1),
          (attrB, 1),
          (attrC, 1),
          (attrD, 1),
          (attrE, 1),
          (attrF, 1)
        )
      )
    val shareValues = ShareValues(rawShares, equiAttrs)
    val shareValuesContext = ShareValuesContext(shareValues)

    val shareConstraint =
      ShareConstraint(AttributeMap(Seq()), shareValues.equivalenceAttrs)
    val shareConstraintContext = ShareConstraintContext(shareConstraint)

    val partR1Exec = PartitionExchangeExec(scanR1Exec, shareValuesContext)
    val partR2Exec = PartitionExchangeExec(scanR2Exec, shareValuesContext)

//    partR1Exec.execute().foreach { partition =>
//      println(partition)
//    }

//    partR2Exec.execute().foreach { partition =>
//      println(partition)
//    }

//    val R1DataTypes = partR1Exec.output.map(_.dataType)
//    val R2DataTypes = partR2Exec.output.map(_.dataType)

    val pairExec = PullPairExchangeExec(
      Seq(partR1Exec, partR2Exec),
      shareConstraintContext,
      shareValuesContext
    )

    pairExec.execute().foreach { partition =>
      println(partition)
    }
  }

//    test("automated") {
//
//      // Prepare
//      val cardinality = 3
//      val upperBound = 100
//      val scanR1 = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
//        "R1",
//        "A" :: "B" :: Nil,
//        cardinality,
//        upperBound
//      )
//      val scanR2 = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
//        "R2",
//        "C" :: "D" :: Nil,
//        cardinality,
//        upperBound
//      )
//      val sharedShare = SharedParameter(mutable.HashMap[String, Int]())
//      val output = "A" :: "B" :: "C" :: "D" :: Nil
//      val partitionR1 = PartitionExchangeExec(scanR1, sharedShare)
//      val partitionR2 = PartitionExchangeExec(scanR2, sharedShare)
//      val children = partitionR1 :: partitionR2 :: Nil
//      val pushPairExchangeExec =
//        PullPairExchangeExec(children, Map(), sharedShare)
//
//      // Testing
//      pushPairExchangeExec.execute().foreachPartition { it =>
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
//                  println(
//                    s"localOutput:${indexedRowBlock.output},localShareVector:${indexedRowBlock.index.toSeq},blockContents:${indexedRowBlock.blockContent.content
//                      .map(_.mkString("(", ",", ")"))
//                      .toSeq}"
//                  )
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
//
//    }
}
