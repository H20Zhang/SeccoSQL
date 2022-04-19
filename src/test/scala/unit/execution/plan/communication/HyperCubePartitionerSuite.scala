package unit.execution.plan.communication

import org.apache.spark.secco.analysis.UnresolvedAttribute
import org.apache.spark.secco.execution.plan.communication.{
  HyperCubePartitioner,
  ShareValues
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.expression.{AttributeReference, EqualTo}
import org.apache.spark.secco.optimization.util.EquiAttributes
import org.apache.spark.secco.types.DataTypes
import util.SeccoFunSuite

import scala.util.Random

class HyperCubePartitionerSuite extends SeccoFunSuite {

  test("simple") {
    val attrA = createTestAttribute("A")
    val attrB = createTestAttribute("B")
    val attrC = createTestAttribute("C")
    val attrD = createTestAttribute("D")
    val attrE = createTestAttribute("E")
    val attrF = createTestAttribute("F")

    val allAttributes = Array(attrA, attrB, attrC, attrD, attrE)

    val equiAttrs =
      EquiAttributes.fromEquiAttributes(
        Seq(Seq(attrA), Seq(attrB, attrE), Seq(attrC), Seq(attrD), Seq(attrF))
      )
    val rawShares =
      AttributeMap(
        Seq(
          (attrA, 1),
          (attrB, 3),
          (attrC, 2),
          (attrD, 1),
          (attrE, 3),
          (attrF, 2)
        )
      )

    val shareValues = ShareValues(rawShares, equiAttrs)

    val selectedAttrs = Array(attrD, attrE, attrF)
    val partitioner =
      new HyperCubePartitioner(selectedAttrs, shareValues)

    println(
      s"allAttributes:${allAttributes.toSeq}, selectedAttrs:${selectedAttrs.toSeq}"
    )
    println(s"rawShares:${rawShares}")

    val row1 = InternalRow(3, 2, 1)
    val serverId1 = partitioner.getPartition(row1)
    val coordiante1 = partitioner.getIndex(serverId1)

    coordiante1.index(2) = 0
    val serverId2 = partitioner.getServerId(coordiante1)

    println(row1, partitioner.numPartitions, serverId1, serverId2, coordiante1)
  }

//  test("automated") {
//
//    val artys = Range(0, 8)
//
//    artys.foreach { artiy =>
//      val attributes =
//        Range(0, artiy).map(f => Math.abs(Random.nextInt(20) + 1)).map { id =>
//          AttributeReference(id.toString, DataTypes.IntegerType)()
//        }
//
//      val shareValues =
//
//
//      val partitioner = new PairPartitioner(shareSpaceVector)
//      Range(0, 10).foreach { _ =>
//        val tuple =
//          Range(0, shareSpaceVector.size)
//            .map(f => Random.nextInt())
//            .toArray
//        val serverId = partitioner.getPartition(tuple)
//        val coordinate = partitioner.getCoordinate(serverId)
//        val _serverId = partitioner.getServerId(coordinate)
//        val _coordinate = partitioner.getCoordinate(_serverId)
//        assert(
//          coordinate.toSeq == _coordinate.toSeq,
//          s"coordinate:${coordinate} != _coordinate:${_coordinate}"
//        )
//      }
//
//    }
//
//  }

}
