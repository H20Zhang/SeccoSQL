package org.apache.spark.secco.execution.plan.communication

import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.{SeccoPlan, _}
import org.apache.spark.secco.execution.plan.communication.utils.{
  EnumShareComputer,
  PairPartitioner,
  ShareResults
}
import org.apache.spark.secco.execution.plan.support.PairExchangeSupport
import org.apache.spark.rdd.RDD

import scala.collection.mutable

case class PushPairExchangeExec(
    children: Seq[SeccoPlan],
    constraint: Map[String, Int],
    sharedShare: SharedParameter[mutable.HashMap[String, Int]]
) extends SeccoPlan
    with PairExchangeSupport
    with Serializable {

  override def outputOld: Seq[String] = children.flatMap(_.outputOld).distinct

  val shareSpaceVector = outputOld.map(share)
  val partitioner = new PairPartitioner(shareSpaceVector.toArray)

  def nonNegativeMod[T](x: T, mod: Int): Int = {
    val rawMod = x.hashCode() % mod
    rawMod + (if (rawMod < 0) mod else 0) toInt
  }

  /** generate special RDD where each line is (serverID, (relationID, row)) */
  def genCoordinateRDD(
      child: SeccoPlan,
      rdd: RDD[InternalBlock],
      relationId: Int
  ): RDD[(Int, (Int, InternalRow))] = {

    //init
    val localOutput = child.outputOld
    val arity = localOutput.size
    val attrIdsLocalPosToGlobalPos = localOutput.map { idx =>
      outputOld.indexOf(idx)
    }
    //here we need to make sure that the relationschema has an id
    val totalAttrsSize = shareSpaceVector.size
    val globalPosToAttrIdsLocalPosMap =
      attrIdsLocalPosToGlobalPos.zipWithIndex.toMap

    rdd.flatMap { block =>
      block.asInstanceOf[RowBlock].blockContent.content.flatMap { tuple =>
        //find the hash values for each attribute of the tuple
        val hashValues = new Array[Int](arity)
        var i = 0
        while (i < arity) {
          hashValues(i) = nonNegativeMod(
            tuple(i),
            shareSpaceVector(attrIdsLocalPosToGlobalPos(i))
          )

          i += 1
        }

        //fill the hash values for all attributes
        val locationForEachAttrs = Range(0, totalAttrsSize).map { idx =>
          if (globalPosToAttrIdsLocalPosMap.contains(idx)) {
            Array(hashValues(globalPosToAttrIdsLocalPosMap(idx)))
          } else {
            Range(0, shareSpaceVector(idx)).toArray
          }
        }.toArray

        //gen the location to be sent for the tuple
        var locations = locationForEachAttrs(0).map(Array(_))

        i = 1
        while (i < totalAttrsSize) {
          locations = locationForEachAttrs(i).flatMap { value =>
            locations.map(location => location :+ value)
          }
          i += 1
        }

        val locationForTuple =
          locations.map(location => partitioner.getPartition(location))

        locationForTuple.map(locationId => (locationId, (relationId, tuple)))
      }
    }
  }

  override protected def doPrepare(): Unit = {
    super.doPrepare()

    val cardinalities = children.map { child =>
      (
        child.outputOld,
        child.statisticKeeper.rowCountOnlyStatistic().rowCount.toLong
      )
    }.toMap
    val schemas = children.map(_.outputOld)
    val shareComputer = new EnumShareComputer(
      schemas,
      constraint,
      SeccoConfiguration.newDefaultConf().numPartition,
      cardinalities
    )
    val shareResults = shareComputer.optimalShareWithBudget()
    _shareResults = Some(shareResults)
    shareResults.share.foreach { case (key, value) => share(key) = value }
  }

  /**
    * Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalBlock] = {

    // increment the counter for benchmark
    val counterManager = dlSession.sessionState.counterManager
    counterManager
      .getOrCreateCounter("benchmark", s"communicationCostInTuples")
      .increment(_shareResults.get.communicationCostInTuples)
    counterManager
      .getOrCreateCounter("benchmark", s"communicationCostInBytes")
      .increment(_shareResults.get.communicationCostInBytes)

    // do execute
    val inputs = children.map(_.execute())
    val rdds = inputs.zipWithIndex.map {
      case (input, relationID) =>
        genCoordinateRDD(children(relationID), input, relationID)
    }

    val relationId2Output =
      children.map(_.outputOld).zipWithIndex.map(_.swap).toMap
    val relationId2LocalIndex =
      children.map(_.outputOld.map(outputOld.indexOf(_)))

    //generate RDD[InternalBlock]
    sparkContext
      .union(rdds)
      .groupByKey(SeccoConfiguration.newDefaultConf().numPartition)
      .map {
        case (key, tuples) =>
          val shareVector = partitioner.getCoordinate(key)
          val receivedRowBlocks = tuples
            .groupBy(_._1)
            .map {
              case (relationId, values) =>
                val content = values.map(_._2).toArray
                val rowBlock = RowBlockContent(content)
                val localShareVector =
                  relationId2LocalIndex(relationId).map(shareVector).toArray
                val localOutput = relationId2Output(relationId)
                val indexedRowBlock =
                  RowIndexedBlock(
                    localOutput,
                    localShareVector,
                    rowBlock
                  )
                indexedRowBlock
            }
            .toArray

          MultiTableIndexedBlock(outputOld, shareVector, receivedRowBlocks)
      }

  }
}
