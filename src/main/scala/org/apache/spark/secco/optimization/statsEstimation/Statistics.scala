package org.apache.spark.secco.optimization.statsEstimation

import org.apache.spark.util.Utils
import java.math.{MathContext, RoundingMode}

import org.apache.spark.secco.execution.OldInternalDataType

import scala.collection.mutable

/** Estimates of various statistics.  The default estimation logic simply lazily multiplies the
  * corresponding statistic produced by the children.  To override this behavior, override
  * `statistics` and assign it an overridden version of `Statistics`.
  *
  * '''NOTE''': concrete and/or overridden versions of statistics fields should pay attention to the
  * performance of the implementations.  The reason is that estimations might get triggered in
  * performance-critical processes, such as query plan planning.
  *
  * Note that we are using a BigInt here since it is easy to overflow a 64-bit integer in
  * cardinality estimation (e.g. cartesian joins).
  *
  * @param rowCount Estimated number of rows.
  * @param sizeInBytes Physical size in bytes. For leaf operators this defaults to 1, otherwise it
  *                    defaults to the product of children's `sizeInBytes`.
  * @param attributeStats Statistics for Attributes.
  * @param fullCardinality full cardinalities which records numbers of unique tuples per sub-attributes.
  */
case class Statistics(
    rowCount: BigInt,
    sizeInBytes: Option[BigInt] = None,
    attributeStats: mutable.HashMap[String, ColumnStat] =
      mutable.HashMap[String, ColumnStat](),
    fullCardinality: mutable.HashMap[Set[String], Long] =
      mutable.HashMap[Set[String], Long]()
) {

  override def toString: String = "Statistics(" + simpleString + ")"

  /** Readable string representation for the Statistics. */
  def simpleString: String = {
    Seq(
      s"rowCount=${BigDecimal(rowCount, new MathContext(3, RoundingMode.HALF_UP)).toString()}",
      if (sizeInBytes.isDefined) {
        // Show row count in scientific notation.
        s"sizeInBytes=${Utils.bytesToString(sizeInBytes.get)}"
      } else {
        ""
      }
    ).filter(_.nonEmpty).mkString(", ")
  }

  //find the relative degree of "a" attributes respect to "b" attributes
  def relativeDegree(
      a: Seq[String],
      b: Seq[String]
  ): Long = {
    val cardinalityForA = cardinality(a)
    val cardinalityForB = cardinality(b)
    Math.ceil(cardinalityForA.toDouble / cardinalityForB.toDouble).toLong
  }

  def cardinality(a: Seq[String]): Long = {
    if (a.isEmpty) {
      1
    } else {
      val cardinalityForA = fullCardinality(a.toSet)
      cardinalityForA
    }
  }
}

/** Statistics collected for a column.
  *
  * 1. The JVM data type stored in min/max is the internal data type for the corresponding
  *    Catalyst data type. For example, the internal type of DateType is Int, and that the internal
  *    type of TimestampType is Long.
  * 2. There is no guarantee that the statistics collected are accurate. Approximation algorithms
  *    (sketches) might have been used, and the data collected can also be stale.
  *
  * @param distinctCount number of distinct values
  * @param min minimum value
  * @param max maximum value
  * @param nullCount number of nulls
  * @param avgLen average length of the values. For fixed-length types, this should be a constant.
  * @param maxLen maximum length of the values. For fixed-length types, this should be a constant.
  * @param histogram histogram of the values
  */
case class ColumnStat(
    distinctCount: Option[BigInt] = None,
    min: Option[OldInternalDataType] = None,
    max: Option[OldInternalDataType] = None,
    nullCount: Option[BigInt] = None,
    avgLen: Option[Long] = None,
    maxLen: Option[Long] = None,
    histogram: Option[Histogram] = None
) {

  // Are distinctCount statistics defined?
  val hasCountStats = distinctCount.isDefined

  // Are min and max statistics defined?
  val hasMinMaxStats = min.isDefined && max.isDefined

  // Are avgLen and maxLen statistics defined?
  val hasLenStats = avgLen.isDefined && maxLen.isDefined

}

/** This class is an implementation of equi-height histogram.
  * Equi-height histogram represents the distribution of a column's values by a sequence of bins.
  * Each bin has a value range and contains approximately the same number of rows.
  *
  * @param height number of rows in each bin
  * @param bins equi-height histogram bins
  */
case class Histogram(height: Double, bins: Array[HistogramBin]) {

  // Only for histogram equality test.
  override def equals(other: Any): Boolean =
    other match {
      case otherHgm: Histogram =>
        height == otherHgm.height && bins.sameElements(otherHgm.bins)
      case _ => false
    }

  override def hashCode(): Int = {
    val temp = java.lang.Double.doubleToLongBits(height)
    var result = (temp ^ (temp >>> 32)).toInt
    result =
      31 * result + java.util.Arrays.hashCode(bins.asInstanceOf[Array[AnyRef]])
    result
  }
}

/** A bin in an equi-height histogram. We use double type for lower/higher bound for simplicity.
  *
  * @param lo lower bound of the value range in this bin
  * @param hi higher bound of the value range in this bin
  * @param ndv approximate number of distinct values in this bin
  */
case class HistogramBin(lo: Double, hi: Double, ndv: Long)
