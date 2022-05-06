package unit.execution.plan.communication

import org.apache.spark.secco.debug
import org.apache.spark.secco.execution.plan.communication.{
  PartitionExchangeExec,
  ShareValues,
  ShareValuesContext
}
import org.apache.spark.secco.execution.storage.block.UnsafeInternalRowBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.EqualTo
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.optimization.util.EquiAttributes
import org.apache.spark.secco.types.{DataTypes, StructType}
import util.SeccoFunSuite

class PartitionExchangeExecSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    createTestRelation(
      Seq(Seq(0, 1, 1), Seq(2, 3, 3), Seq(3, 4, 4)),
      "R1",
      "a",
      "b",
      "c"
    )()
  }

  test("simple") {

    val R1 = seccoSession.table("R1")
    val inputExec = R1.queryExecution.executionPlan

    val attrA = inputExec.output(0)
    val attrB = inputExec.output(1)
    val attrC = inputExec.output(2)
    val attrD = createTestAttribute("d")

    val equiAttrs =
      EquiAttributes.fromEquiAttributes(
        Seq(Seq(attrD, attrA), Seq(attrB, attrC))
      )
    val rawShares =
      AttributeMap(
        Seq((attrD, 2), (attrA, 2), (attrB, 3), (attrC, 3))
      )
    val shareValues = ShareValues(rawShares, equiAttrs)
    val shareValuesContext = ShareValuesContext(shareValues)

    val partitionExec = PartitionExchangeExec(inputExec, shareValuesContext)

    partitionExec.execute().foreach { partition =>
      println(partition)
    }
  }

//    test("automated") {
//      val arities = Range(1, 8)
//      val numRun = 1
//      val cardinality = 100
//      val upperBound = 100
//
//      Range(0, numRun).foreach { run =>
//        arities.foreach { artiy =>
//          val attrs = Range(0, artiy).map(_.toString)
//          val scanExec =
//            TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
//              "R1",
//              attrs,
//              cardinality,
//              upperBound
//            )
//          println(s"scanExec.execute().count():${scanExec.execute().count()}")
//          val sharedShare =
//            SharedParameter(
//              mutable.HashMap(
//                attrs.map(attr => (attr, Random.nextInt(5) + 1)): _*
//              )
//            )
//
//          val partitionExec = PartitionExchangeExec(scanExec, sharedShare)
//          println(
//            s"sharedShare:${sharedShare}"
//          )
//          val partitioner = partitionExec.taskPartitioner
//          partitionExec
//            .execute()
//            .map { f =>
//              f match {
//                case r @ RowIndexedBlock(output, shareVector, blockContent) =>
//                  //DEBUG
//                  //              println(
//                  //                s"output:${output}, shareVector:${shareVector.toSeq}, blockContent:${blockContent.content.toSeq}"
//                  //              )
//                  blockContent.content.forall(row =>
//                    partitioner.getPartition(row) == partitioner
//                      .getPartition(shareVector)
//                  )
//
//                case _ => throw new Exception("fail")
//              }
//              f
//            }
//            .count()
//        }
//      }
//
//    }

}
