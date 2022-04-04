package unit.execution.plan.communication

import org.apache.spark.secco.analysis.UnresolvedAttribute
import org.apache.spark.secco.execution.plan.communication.PairPartitioner
import org.apache.spark.secco.expression.AttributeReference
import org.apache.spark.secco.types.DataTypes
import util.SeccoFunSuite

import scala.util.Random

class PairPartitionerSuite extends SeccoFunSuite {

//  test("PairPartitioner") {
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
