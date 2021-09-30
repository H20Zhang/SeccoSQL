package org.apache.spark.secco.optimization.statsEstimation.histogram

import org.apache.spark.secco.execution.OldInternalDataType
import org.apache.spark.secco.optimization.plan.MultiwayNaturalJoin
import org.apache.spark.secco.optimization.statsEstimation.{
  ColumnStat,
  Estimation,
  Histogram,
  Statistics
}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object HistogramJoinEstimation extends Estimation[MultiwayNaturalJoin] {
  override def estimate(join: MultiwayNaturalJoin): Option[Statistics] = {

    if (join.children.size == 2) {
      estimateBinaryJoin(join.children(0), join.children(1))
    } else if (join.children.size > 2) {
      //we assume there is no cartesian product
      //we construct connected binary join tree for estimation
      val leaf1 = join.children(0)
      val leaf2 = join.children
        .filter(_.outputOld.intersect(leaf1.outputOld).nonEmpty)
        .head
      val leafJoin = MultiwayNaturalJoin(
        children = Seq(leaf1, leaf2),
        mode = ExecMode.Coupled
      )

      var remainingChildren =
        join.children.filter(f => f != leaf1 || f != leaf2)
      var rootJoin = leafJoin
      while (remainingChildren.nonEmpty) {
        val nextLeaf = remainingChildren
          .filter(_.outputOld.intersect(rootJoin.outputOld).nonEmpty)
          .head
        rootJoin = MultiwayNaturalJoin(
          children = Seq(rootJoin, nextLeaf),
          mode = ExecMode.Coupled
        )
        remainingChildren = remainingChildren.filter(f => f != nextLeaf)
      }

      Some(rootJoin.stats)
    } else {
      throw new Exception("numbers of children for join should always >= 2")
    }

  }

  //we only consider natural join.
  private def estimateBinaryJoin(
      leftChild: LogicalPlan,
      rightChild: LogicalPlan
  ): Option[Statistics] = {

    val leftStats = leftChild.stats
    val rightStats = rightChild.stats

    val joinKey = leftChild.outputOld.intersect(rightChild.outputOld)
    val outputOld = (leftChild.outputOld ++ rightChild.outputOld).distinct

    // 1. compute new join cardinality and updated statistic
    val joinKeyWithStats = leftChild.stats.attributeStats.keySet
      .intersect(rightChild.stats.attributeStats.keySet)
      .intersect(joinKey.toSet)
    val (outputRows, keyStatsAfterJoin) =
      computeCardinalityAndStats(joinKeyWithStats.toSeq, leftStats, rightStats)

    // 2. Update statistics based on the output of join
    val inputAttrStats = mutable.HashMap(
      (leftStats.attributeStats.toSeq ++ rightStats.attributeStats.toSeq): _*
    )
    val attributesWithStat = outputOld.filter(a =>
      inputAttrStats.get(a).map(_.hasCountStats).getOrElse(false)
    )

    val outputStats: Seq[(String, ColumnStat)] = if (outputRows == 0) {
      // The output is empty, we don't need to keep column stats.
      Nil
    } else if (outputRows == leftStats.rowCount * rightStats.rowCount) {
      // Cartesian product, just propagate the original column stats
      inputAttrStats.toSeq
    } else {
      updateOutputStats(
        outputRows,
        attributesWithStat,
        inputAttrStats,
        keyStatsAfterJoin,
        leftStats,
        rightStats
      )
    }

    val outputAttrStats = mutable.HashMap(outputStats: _*)
    Some(
      Statistics(
        rowCount = outputRows,
        attributeStats = outputAttrStats
      )
    )

  }

  // scalastyle:off
  /** The number of rows of A inner join B on A.k1 = B.k1 is estimated by this basic formula:
    * T(A IJ B) = T(A) * T(B) / max(V(A.k1), V(B.k1)),
    * where V is the number of distinct values (ndv) of that column. The underlying assumption for
    * this formula is: each value of the smaller domain is included in the larger domain.
    *
    * Generally, inner join with multiple join keys can be estimated based on the above formula:
    * T(A IJ B) = T(A) * T(B) / (max(V(A.k1), V(B.k1)) * max(V(A.k2), V(B.k2)) * ... * max(V(A.kn), V(B.kn)))
    * However, the denominator can become very large and excessively reduce the result, so we use a
    * conservative strategy to take only the largest max(V(A.ki), V(B.ki)) as the denominator.
    *
    * That is, join estimation is based on the most selective join keys. We follow this strategy
    * when different types of column statistics are available. E.g., if card1 is the cardinality
    * estimated by ndv of join key A.k1 and B.k1, card2 is the cardinality estimated by histograms
    * of join key A.k2 and B.k2, then the result cardinality would be min(card1, card2).
    *
    * @param keys join keys
    *
    * @return join cardinality, and column stats for join keys after the join
    */
  // scalastyle:on
  private def computeCardinalityAndStats(
      keys: Seq[String],
      leftStats: Statistics,
      rightStats: Statistics
  ): (BigInt, mutable.HashMap[String, ColumnStat]) = {
    // If there's no column stats available for join keys, estimate as cartesian product.
    var joinCard: BigInt = leftStats.rowCount * rightStats.rowCount
    val keyStatsAfterJoin = new mutable.HashMap[String, ColumnStat]()
    var i = 0
    while (i < keys.length && joinCard != 0) {
      val key = keys(i)
      // Check if the two sides are disjoint
      val leftKeyStat = leftStats.attributeStats(key)
      val rightKeyStat = rightStats.attributeStats(key)

      if (
        !(leftKeyStat.max.get < rightKeyStat.min.get || rightKeyStat.max.get < leftKeyStat.min.get)
      ) {

        val newMin = if (leftKeyStat.min.get < rightKeyStat.min.get) {
          rightKeyStat.min.get
        } else {
          leftKeyStat.min.get
        }

        val newMax = if (leftKeyStat.max.get < rightKeyStat.max.get) {
          leftKeyStat.max.get
        } else {
          rightKeyStat.max.get
        }

        val (card, joinStat) =
          (leftKeyStat.histogram, rightKeyStat.histogram) match {
            case (Some(l: Histogram), Some(r: Histogram)) =>
              computeByHistogram(
                key,
                l,
                r,
                newMin,
                newMax,
                leftStats,
                rightStats
              )
            case _ =>
              computeByNdv(key, newMin, newMax, leftStats, rightStats)
          }

        keyStatsAfterJoin += (
          // Histograms are propagated as unchanged. During future estimation, they should be
          // truncated by the updated max/min. In this way, only pointers of the histograms are
          // propagated and thus reduce memory consumption.
          key -> joinStat.copy(histogram = leftKeyStat.histogram)
        )
        // Return cardinality estimated from the most selective join keys.
        if (card < joinCard) joinCard = card
      } else {
        // One of the join key pairs is disjoint, thus the two sides of join is disjoint.
        joinCard = 0
      }
      i += 1
    }
    (joinCard, keyStatsAfterJoin)
  }

  /** Returns join cardinality and the column stat for this pair of join keys. */
  private def computeByNdv(
      key: String,
      min: OldInternalDataType,
      max: OldInternalDataType,
      leftStats: Statistics,
      rightStats: Statistics
  ): (BigInt, ColumnStat) = {
    val leftKeyStat = leftStats.attributeStats(key)
    val rightKeyStat = rightStats.attributeStats(key)
    val maxNdv =
      leftKeyStat.distinctCount.get.max(rightKeyStat.distinctCount.get)
    // Compute cardinality by the basic formula.
    val card = BigDecimal(
      leftStats.rowCount * rightStats.rowCount
    ) / BigDecimal(maxNdv)

    // Get the intersected column stat.
    val newNdv = Some(
      leftKeyStat.distinctCount.get.min(rightKeyStat.distinctCount.get)
    )
    val newMaxLen =
      if (leftKeyStat.maxLen.isDefined && rightKeyStat.maxLen.isDefined) {
        Some(math.min(leftKeyStat.maxLen.get, rightKeyStat.maxLen.get))
      } else {
        None
      }
    val newAvgLen =
      if (leftKeyStat.avgLen.isDefined && rightKeyStat.avgLen.isDefined) {
        Some((leftKeyStat.avgLen.get + rightKeyStat.avgLen.get) / 2)
      } else {
        None
      }
    val newStats =
      ColumnStat(newNdv, Some(min), Some(max), Some(0), newAvgLen, newMaxLen)

    (Estimation.ceil(card), newStats)
  }

  /** Compute join cardinality using equi-height histograms. */
  private def computeByHistogram(
      key: String,
      leftHistogram: Histogram,
      rightHistogram: Histogram,
      newMin: OldInternalDataType,
      newMax: OldInternalDataType,
      leftStats: Statistics,
      rightStats: Statistics
  ): (BigInt, ColumnStat) = {

    val overlappedRanges = Estimation.getOverlappedRanges(
      leftHistogram = leftHistogram,
      rightHistogram = rightHistogram,
      // Only numeric values have equi-height histograms.
      lowerBound = newMin,
      upperBound = newMax
    )

//    pprint.pprintln(overlappedRanges)

    var card: BigDecimal = 0
    var totalNdv: Double = 0
    for (i <- overlappedRanges.indices) {
      val range = overlappedRanges(i)
      if (i == 0 || range.hi != overlappedRanges(i - 1).hi) {
        // If range.hi == overlappedRanges(i - 1).hi, that means the current range has only one
        // value, and this value is already counted in the previous range. So there is no need to
        // count it in this range.
        totalNdv += math.min(range.leftNdv, range.rightNdv)
      }

      // Apply the formula in this overlapped range.
      card += range.leftNumRows * range.rightNumRows / math.max(
        range.leftNdv,
        range.rightNdv
      )
    }

    val leftKeyStat = leftStats.attributeStats(key)
    val rightKeyStat = rightStats.attributeStats(key)
    val newMaxLen =
      if (leftKeyStat.maxLen.isDefined && rightKeyStat.maxLen.isDefined) {
        Some(math.min(leftKeyStat.maxLen.get, rightKeyStat.maxLen.get))
      } else {
        None
      }
    val newAvgLen =
      if (leftKeyStat.avgLen.isDefined && rightKeyStat.avgLen.isDefined) {
        Some((leftKeyStat.avgLen.get + rightKeyStat.avgLen.get) / 2)
      } else {
        None
      }
    val newStats = ColumnStat(
      Some(Estimation.ceil(totalNdv)),
      Some(newMin),
      Some(newMax),
      Some(0),
      newAvgLen,
      newMaxLen
    )
    (Estimation.ceil(card), newStats)
  }

  /** Propagate or update column stats for output attributes.
    */
  private def updateOutputStats(
      outputRows: BigInt,
      outputOld: Seq[String],
      oldAttrStats: mutable.HashMap[String, ColumnStat],
      keyStatsAfterJoin: mutable.HashMap[String, ColumnStat],
      leftStats: Statistics,
      rightStats: Statistics
  ): Seq[(String, ColumnStat)] = {
    val outputAttrStats = new ArrayBuffer[(String, ColumnStat)]()
    val leftRows = leftStats.rowCount
    val rightRows = rightStats.rowCount

    outputOld.foreach { a =>
      // check if this attribute is a join key
      if (keyStatsAfterJoin.contains(a)) {
        outputAttrStats += a -> keyStatsAfterJoin(a)
      } else {
        val oldColStat = oldAttrStats(a)
        val oldNdv = oldColStat.distinctCount
        val newNdv = if (oldNdv.isDefined) {
          Some(
            Estimation.updateNdv(
              oldNumRows = leftRows,
              newNumRows = outputRows,
              oldNdv = oldNdv.get
            )
          )
        } else {
          None
        }
        val newColStat = oldColStat.copy(distinctCount = newNdv)
        // TODO: support nullCount updates for specific outer joins
        outputAttrStats += a -> newColStat
      }
    }

    outputAttrStats
  }

}
