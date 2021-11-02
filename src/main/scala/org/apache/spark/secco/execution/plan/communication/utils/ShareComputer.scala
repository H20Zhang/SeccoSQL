package org.apache.spark.secco.execution.plan.communication.utils

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.benchmark.BenchmarkResult
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.util.misc.LogAble

import scala.collection.mutable.ArrayBuffer

/** The case class that encapsulate the results of [[EnumShareComputer]] */
case class ShareResults(
    share: Map[Attribute, Int],
    communicationCostInBytes: Long,
    loadInBytes: Double,
    communicationCostInTuples: Long,
    loadInTuples: Double
)

/** The optimizer for computing the optimal share in terms of load.
  * @param schemas schema of the input relations.
  * @param constraint constraint for attributes' share
  * @param tasks minimum number of tasks.
  * @param cardinalities cardinality of the input relations.
  */
class EnumShareComputer(
    schemas: Seq[Seq[Attribute]],
    constraint: Map[Attribute, Int],
    tasks: Int,
    cardinalities: Map[Seq[Attribute], Long]
) extends LogAble {

  private var numTask = tasks
  private val attributes = schemas.flatMap(identity).distinct
  private val averageBytesPerTuple =
    (schemas.map(_.size).sum.toDouble / schemas.size) * 8

  /** Generate all possible share configuration with `attributes`, `constraint` and `numTask`. */
  private def genAllShare(): Array[Array[Int]] = {
    //    get all shares
    val shareEnumerator = new ShareEnumerator(attributes, constraint, numTask)
    val allShare = shareEnumerator.genAllShares()
    allShare.toArray
  }

  /** Compute the optimal share with memory budget (Pair Memory Budget). */
  def optimalShareWithBudget(): ShareResults = {

    var shareResults = optimalShare()
    var share = shareResults.share
    var loadInBytes = shareResults.loadInBytes

    val maximalLoad =
      SeccoSession.currentSession.sessionState.conf.pairMemoryBudget

    // we try to increase num to task to decrease the load until numTask is 10x of original tasks numbers
    while (loadInBytes > maximalLoad && numTask < 100 * tasks) {
      numTask = (numTask * 2)
      shareResults = optimalShare()
      share = shareResults.share
      loadInBytes = shareResults.loadInBytes
    }

    logInfo(
      s"shareResults:${shareResults}, loadInBytes:${loadInBytes}, maximalLoad:${maximalLoad}, cardinalities:${cardinalities}"
    )

    shareResults
  }

  /** Compute the optimal share without memory budget */
  def optimalShare(): ShareResults = {

    //    get all shares
    val allShare = genAllShare()

//    println(s"allShare.size:${allShare.size}")

    //    find optimal share --- init
    val attribute2ToPos = attributes.zipWithIndex.toMap
    var minShare: Array[Int] = Array()
    var minCommunicationInTuples: Long = Long.MaxValue
    var minLoadInTuples: Double = Double.MaxValue
    var shareSum: Int = Int.MaxValue

    val excludedAttributesOfRelationAndCardinality = cardinalities
      .map(f => (attributes.filter(A => !f._1.contains(A)), f._2))
      .map(f => (f._1.map(attribute2ToPos), f._2))

    //    find optimal share --- examine communication cost incurred by every share
    allShare.foreach { share =>
      val communicationCost = excludedAttributesOfRelationAndCardinality.map {
        case (excludedAttrs, cardinality) =>
          var multiplyFactor = 1L

          excludedAttrs.foreach { case idx =>
            multiplyFactor = multiplyFactor * share(idx)
          }

          multiplyFactor * cardinality
      }.sum

      val load = (communicationCost.toDouble / share.product)

      if (load == minLoadInTuples && share.sum < shareSum) {
        minLoadInTuples = load
        minCommunicationInTuples = communicationCost
        minShare = share
        shareSum = share.sum
      } else if (load < minLoadInTuples) {
        minLoadInTuples = load
        minCommunicationInTuples = communicationCost
        minShare = share
        shareSum = share.sum
      }

    }

    val share = attribute2ToPos.mapValues(idx => minShare(idx))
    val minLoadInBytes = minLoadInTuples * averageBytesPerTuple
    val minCommunicationInBytes =
      (minCommunicationInTuples * averageBytesPerTuple).toLong

    ShareResults(
      share,
      minCommunicationInBytes,
      minLoadInBytes,
      minCommunicationInTuples,
      minLoadInTuples
    )
  }

}

class ShareEnumerator(
    attributes: Seq[Attribute],
    constraint: Map[Attribute, Int],
    tasks: Int
) {

  val length = attributes.size
  val filterFunc = {
    val constraintArray = constraint
      .map { case (key, value) =>
        (attributes.indexOf(key), value)
      }
      .filter(_._1 != -1)
      .toArray

    (shareVector: Array[Int]) => {
      constraintArray.forall { case (keyPos, value) =>
        shareVector(keyPos) == value
      }
    }
  }

  def genAllShares(): ArrayBuffer[Array[Int]] = {
    _genAllShare(1, length).filter(filterFunc)
  }

  private def _genAllShare(
      prevProd: Int,
      remainLength: Int
  ): ArrayBuffer[Array[Int]] = {

    val largest_possible = tasks / prevProd

    if (remainLength == 1) {
      val mutableArray = new ArrayBuffer[Array[Int]]()
      for (i <- 1 to largest_possible) {
        mutableArray += Array(i)
      }

      return mutableArray
    } else {
      val mutableArray = new ArrayBuffer[Array[Int]]()
      for (i <- 1 to largest_possible) {
        val subs = _genAllShare(prevProd * i, remainLength - 1)
        for (j <- subs) {
          val tempArray = new Array[Int](remainLength)
          j.copyToArray(tempArray)
          tempArray(remainLength - 1) = i
          mutableArray += tempArray
        }
      }
      return mutableArray
    }
  }
}

//class NonLinearShareComputer(schemas: Seq[Schema],
//                             cardinalities: Seq[Double],
//                             memoryBudget: Double) {
//
//  val attrIds = schemas.flatMap(_.attrIDs).distinct
//  val attrIdsWithIdx = attrIds.zipWithIndex.map(f => (f._1, f._2 + 1))
//
//  def optimalShare(): Map[AttributeID, Int] = {
//    val script = genOctaveScript()
//    val rawShare = performOptimization(script)
//    val share = roundOctaveResult(rawShare)
//    share
//  }
//
//  def commCost(share: Map[AttributeID, Int]): Double = {
//    val cost = schemas
//      .zip(cardinalities)
//      .map {
//        case (schema, cardinality) =>
//          val notIncludedAttrId = attrIds.filter { attrId =>
//            !schema.attrIDs.contains(attrId)
//          }
//          val ratio = notIncludedAttrId.map { attrId =>
//            share(attrId)
//          }.product
//
//          cardinality * ratio
//      }
//      .sum
//
//    if ((cost / share.values.product) < memoryBudget) {
//      cost
//    } else {
//
//      Double.MaxValue
//    }
//  }
//
//  def genOctaveScript(): String = {
//
//    val factor = 1000
//    val objScript = schemas
//      .zip(cardinalities)
//      .map {
//        case (schema, cardinality) =>
//          val notIncludedAttrWithIdx = attrIdsWithIdx.filter {
//            case (attrId, idx) => !schema.attrIDs.contains(attrId)
//          }
//          val ratioScript = notIncludedAttrWithIdx
//            .map { case (attrId, idx) => s"x(${idx})*" }
//            .reduce(_ + _)
//            .dropRight(1)
//          val relationCostScript = s"${cardinality / factor}*${ratioScript}"
//          relationCostScript
//      }
//      .map(relationCostScript => s"${relationCostScript}+")
//      .reduce(_ + _)
//      .dropRight(1)
//
//    val memoryConstraintScript = s"${memoryBudget} - (" + schemas
//      .map { schema =>
//        val ratio = schema.attrIDs
//          .map(attrIds.indexOf)
//          .map(idx => s"x(${idx + 1})*")
//          .reduce(_ + _)
//          .dropRight(1)
//        val cardinality = cardinalities(schemas.indexOf(schema))
//        s"${cardinality}/(${ratio})+"
//      }
//      .reduce(_ + _)
//      .dropRight(1) + ");"
//
//    val lowerBoundScript = attrIdsWithIdx
//      .map { case (_, idx) => s"1;" }
//      .reduce(_ + _)
//      .dropRight(1)
//
//    val upperBoundScript = attrIdsWithIdx
//      .map {
//        case (attrId, idx) =>
//          if (schemas
//            .filter(schema => schema.attrIDs.contains(attrId))
//            .size == 1) {
//            s"1;"
//          } else {
//            s"1000;"
//          }
//      }
//      .reduce(_ + _)
//      .dropRight(1)
//
//    val initialPointScript = attrIdsWithIdx
//      .map {
//        case (_, idx) =>
//          s"${Math.pow(Conf.defaultConf().NUM_CORE, 1.0 / attrIds.size)};"
//      }
//      .reduce(_ + _)
//      .dropRight(1)
//
//    val machineNumConstraintScript = attrIdsWithIdx
//      .map { case (_, idx) => s"x(${idx})*" }
//      .reduce(_ + _)
//      .dropRight(1) + s" - ${Conf.defaultConf().NUM_CORE};"
//
//    val octaveScript =
//      s"""
//         |#!octave -qf
//         |1;
//         |
//         |function r = h (x)
//         |  r = [${memoryConstraintScript} ${machineNumConstraintScript}];
//         |endfunction
//         |
//         |function obj = phi (x)
//         |  obj = $objScript;
//         |endfunction
//         |
//         |x0 = [${initialPointScript}];
//         |lower = [${lowerBoundScript}];
//         |upper = [${upperBoundScript}];
//         |
//         |[x, obj, info, iter, nf, lambda] = sqp (x0, @phi, [], @h,lower,upper,500);
//         |
//         |disp(x')
//         |""".stripMargin
//
//    octaveScript
//  }
//
//  def performOptimization(script: String): Map[AttributeID, Double] = {
//
//    import java.io._
//
//    val tempFile = new File("./tempFile.m")
//    val pw = new PrintWriter(tempFile)
//    pw.write(script)
//    pw.close
//
//    import sys.process._
//    val result = "octave -qf ./tempFile.m" !!
//
//    tempFile.delete()
//
//    var rawShareVector = result.split("\\s").filter(_.nonEmpty).map(_.toDouble)
//
//    //fail safe, there is some bug in octave
//    if (rawShareVector.isEmpty) {
//      rawShareVector = attrIds.map(id => Double.MaxValue / 2).toArray
//    }
//
//    val rawShare = attrIds.zip(rawShareVector).toMap
//
//    rawShare
//  }
//
//  def roundOctaveResult(
//                         shareMap: Map[AttributeID, Double]
//                       ): Map[AttributeID, Int] = {
//
//    val shareSeq = shareMap.toSeq
//    val shareSize = shareMap.values.size
//    var roundUpOrDowns = ArrayBuffer(Array(true), Array(false))
//    Range(1, shareSize).foreach { idx =>
//      roundUpOrDowns = roundUpOrDowns.flatMap { roundUpOrDown =>
//        Array(true, false).map(f => roundUpOrDown :+ f)
//      }
//    }
//
//    var roundedShareMaps = roundUpOrDowns.map { f =>
//      f.zipWithIndex.map {
//        case (roundUpDecision, idx) =>
//          var (attrId, shareForAttrId) = shareSeq(idx)
//          if (roundUpDecision) {
//            shareForAttrId = math.floor(shareForAttrId) + 1
//          } else {
//            shareForAttrId = math.floor(shareForAttrId)
//          }
//
//          (attrId, shareForAttrId.toInt)
//      }.toMap
//    }
//
//    roundedShareMaps = roundedShareMaps.filter { share =>
//      share.values.product > Conf.defaultConf().NUM_CORE
//    }
//
//    val optimalRoundedShareMap = roundedShareMaps
//      .map(shareMap => (shareMap, commCost(shareMap)))
//      .sortBy(_._2)
//      .head
//      ._1
//
//    optimalRoundedShareMap
//  }
//
//}
