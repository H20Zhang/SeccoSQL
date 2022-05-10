package org.apache.spark.secco.optimization.statsEstimation.histogram

import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.secco.analysis.RelationAlgebraWithAnalysis.dlSession
import org.apache.spark.secco.execution
import org.apache.spark.secco.execution.OldInternalDataType
import org.apache.spark.secco.optimization.plan.{Filter, Relation}
import org.apache.spark.secco.optimization.statsEstimation.{ColumnStat, Estimation, EstimationUtils, NumericValueInterval, Statistics, ValueInterval}
import org.apache.spark.secco.expression.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.secco.optimization.statsEstimation.EstimationUtils.ceil

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.optimization.ExecMode
import org.apache.spark.secco.optimization.statsEstimation.EstimationUtils.getOutputSize
import org.apache.spark.secco.types.{BinaryType, BooleanType, NumericType}
import org.apache.spark.secco.types.StringType
import shapeless.syntax.std.product.productOps
import sourcecode.Text.generate
import spire.math.Polynomial.x

import scala.collection.immutable.Nil.toMap

//import org.apache.spark.sql.catalyst.expressions._
//import org.apache.spark.sql.catalyst.plans.logical._
//import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
//import org.apache.spark.sql.types._
import org.apache.spark.secco.optimization.statsEstimation.{
  ColumnStat,
  Estimation,
  Histogram,
  Statistics
}

object HistogramFilterEstimation extends Estimation[Filter] {
  def main(args: Array[String]): Unit = {
    def R1 = dlSession.table("R1")
    val filter: Filter = Filter(Relation("R1"), Seq(("A", "<", "B")), ExecMode.Coupled)
    val res = estimate(filter)
    println(res)
  }
  /**
    * Returns an option of Statistics for a Filter logical plan node.
    * For a given compound expression condition, this method computes filter selectivity
    * (or the percentage of rows meeting the filter condition), which
    * is used to compute row count, size in bytes, and the updated statistics after a given
    * predicated is applied.
    *
    * @return Option[Statistics] When there is no statistics collected, it returns None.
    */
  override def estimate(plan: Filter): Option[Statistics] = {
    val childStats = plan.child.stats
//    val stats = childStats.attributeStats
//    val attr = AttributeMap(stats.map(x => (x._1, x._2)).toMap)
    val colStatsMap = ColumnStatsMap(childStats.attributeStats)
//    if (childStats.rowCount.isEmpty) return None

    // Estimate selectivity of this filter predicate, and update column stats if needed.
    // For not-supported condition, set filter selectivity to a conservative estimate 100%
    val condition = plan.condition.get
    val filterSelectivity = calculateFilterSelectivity(childStats.attributeStats, condition).getOrElse(1.0)

    val filteredRowCount: BigInt = {
			EstimationUtils.ceil(BigDecimal(childStats.rowCount) * filterSelectivity)
    }
    val newColStats = if (filteredRowCount == 0) {
      // The output is empty, we don't need to keep column stats.
//      AttributeMap[ColumnStat](Nil)
      mutable.HashMap.empty[String, ColumnStat]
    } else {
      colStatsMap.outputColumnStats(rowsBeforeFilter = childStats.rowCount,
        rowsAfterFilter = filteredRowCount)
    }
//    val filteredSizeInBytes: BigInt = getOutputSize(plan.output, filteredRowCount, newColStats)
    val filteredSizeInBytes: BigInt = getOutputSize(plan.output, filteredRowCount, newColStats)
    Some(childStats.copy(sizeInBytes = Option(filteredSizeInBytes), rowCount = filteredRowCount,
      attributeStats = newColStats))
  }

  /**
    * Returns a percentage of rows meeting a condition in Filter node.
    * If it's a single condition, we calculate the percentage directly.
    * If it's a compound condition, it is decomposed into multiple single conditions linked with
    * AND, OR, NOT.
    * For logical AND conditions, we need to update stats after a condition estimation
    * so that the stats will be more accurate for subsequent estimation.  This is needed for
    * range condition such as (c > 40 AND c <= 50)
    * For logical OR and NOT conditions, we do not update stats after a condition estimation.
    *
    * @param condition the compound logical expression
    * @param update    a boolean flag to specify if we need to update ColumnStat of a column
    *                  for subsequent conditions
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if the condition is not supported.
    */
  def calculateFilterSelectivity(colStatsMap: mutable.HashMap[String, ColumnStat], condition: Expression, update: Boolean = true): Option[Double] = {
    condition match {
      case And(cond1, cond2) =>
        val percent1 = calculateFilterSelectivity(colStatsMap, cond1, update).getOrElse(1.0)
        val percent2 = calculateFilterSelectivity(colStatsMap, cond2, update).getOrElse(1.0)
        Some(percent1 * percent2)

      case Or(cond1, cond2) =>
        val percent1 = calculateFilterSelectivity(colStatsMap, cond1, update = false).getOrElse(1.0)
        val percent2 = calculateFilterSelectivity(colStatsMap, cond2, update = false).getOrElse(1.0)
        Some(percent1 + percent2 - (percent1 * percent2))

      // Not-operator pushdown
      case Not(And(cond1, cond2)) =>
        calculateFilterSelectivity(colStatsMap, Or(Not(cond1), Not(cond2)), update = false)

      // Not-operator pushdown
      case Not(Or(cond1, cond2)) =>
        calculateFilterSelectivity(colStatsMap, And(Not(cond1), Not(cond2)), update = false)

      // Collapse two consecutive Not operators which could be generated after Not-operator pushdown
      case Not(Not(cond)) =>
        calculateFilterSelectivity(colStatsMap, cond, update = false)

      // The foldable Not has been processed in the ConstantFolding rule
      // This is a top-down traversal. The Not could be pushed down by the above two cases.
      case Not(l@Literal(null, _)) =>
        calculateSingleCondition(colStatsMap, l, update = false).map(boundProbability(_))

      case Not(cond) =>
        calculateFilterSelectivity(colStatsMap, cond, update = false) match {
          case Some(percent) => Some(1.0 - percent)
          case None => None
        }

      case _ =>
        calculateSingleCondition(colStatsMap, condition, update).map(boundProbability(_))
    }
  }

  def evaluateLiteral(literal: Literal): Option[Double] = {
    literal match {
      case Literal(null, _) => Some(0.0)
      case FalseLiteral => Some(0.0)
      case TrueLiteral => Some(1.0)
      // Ideally, we should not hit the following branch
      case _ => None
    }
  }
  /**
    * Returns a percentage of rows meeting a single condition in Filter node.
    * Currently we only support binary predicates where one side is a column,
    * and the other is a literal.
    *
    * @param condition a single logical expression
    * @param update    a boolean flag to specify if we need to update ColumnStat of a column
    *                  for subsequent conditions
    * @return an optional double value to show the percentage of rows meeting a given condition.
    *         It returns None if the condition is not supported.
    */
  def calculateSingleCondition(colStatsMap: mutable.HashMap[String, ColumnStat], condition: Expression, update: Boolean): Option[Double] = {
    condition match {
      case l: Literal =>
        evaluateLiteral(l)

      // For evaluateBinary method, we assume the literal on the right side of an operator.
      // So we will change the order if not.

      // EqualTo/EqualNullSafe does not care about the order
      case Equality(ar: Attribute, l: Literal) =>
        evaluateEquality(colStatsMap, ar, l, update)
      case Equality(l: Literal, ar: Attribute) =>
        evaluateEquality(colStatsMap, ar, l, update)

      case op@LessThan(ar: Attribute, l: Literal) =>
        evaluateBinary(colStatsMap, op, ar, l, update)
      case op@LessThan(l: Literal, ar: Attribute) =>
        evaluateBinary(colStatsMap, GreaterThan(ar, l), ar, l, update)

      case op@LessThanOrEqual(ar: Attribute, l: Literal) =>
        evaluateBinary(colStatsMap, op, ar, l, update)
      case op@LessThanOrEqual(l: Literal, ar: Attribute) =>
        evaluateBinary(colStatsMap, GreaterThanOrEqual(ar, l), ar, l, update)

      case op@GreaterThan(ar: Attribute, l: Literal) =>
        evaluateBinary(colStatsMap, op, ar, l, update)
      case op@GreaterThan(l: Literal, ar: Attribute) =>
        evaluateBinary(colStatsMap, LessThan(ar, l), ar, l, update)

      case op@GreaterThanOrEqual(ar: Attribute, l: Literal) =>
        evaluateBinary(colStatsMap, op, ar, l, update)
      case op@GreaterThanOrEqual(l: Literal, ar: Attribute) =>
        evaluateBinary(colStatsMap, LessThanOrEqual(ar, l), ar, l, update)

      case In(ar: Attribute, expList)
        if expList.forall(e => e.isInstanceOf[Literal]) =>
        // Expression [In (value, seq[Literal])] will be replaced with optimized version
        // [InSet (value, HashSet[Literal])] in Optimizer, but only for list.size > 10.
        // Here we convert In into InSet anyway, because they share the same processing logic.
        val hSet = expList.map(e => e.eval())
        evaluateInSet(ar, HashSet() ++ hSet, update)
//TODO
//      case InSet(ar: Attribute, set) =>
//        evaluateInSet(ar, set, update)

      // In current stage, we don't have advanced statistics such as sketches or histograms.
      // As a result, some operator can't estimate `nullCount` accurately. E.g. left outer join
      // estimation does not accurately update `nullCount` currently.
      // So for IsNull and IsNotNull predicates, we only estimate them when the child is a leaf
      // node, whose `nullCount` is accurate.
      // This is a limitation due to lack of advanced stats. We should remove it in the future.

      // TODO
//      case IsNull(ar: Attribute) if plan.child.isInstanceOf[LeafNode] =>
//        evaluateNullCheck(ar, isNull = true, update)
//
//      case IsNotNull(ar: Attribute) if plan.child.isInstanceOf[LeafNode] =>
//        evaluateNullCheck(ar, isNull = false, update)

      case op@Equality(attrLeft: Attribute, attrRight: Attribute) =>
        evaluateBinaryForTwoColumns(colStatsMap, op, attrLeft, attrRight, update)

      case op@LessThan(attrLeft: Attribute, attrRight: Attribute) =>
        evaluateBinaryForTwoColumns(colStatsMap, op, attrLeft, attrRight, update)

      case op@LessThanOrEqual(attrLeft: Attribute, attrRight: Attribute) =>
        evaluateBinaryForTwoColumns(colStatsMap, op, attrLeft, attrRight, update)

      case op@GreaterThan(attrLeft: Attribute, attrRight: Attribute) =>
        evaluateBinaryForTwoColumns(colStatsMap, op, attrLeft, attrRight, update)

      case op@GreaterThanOrEqual(attrLeft: Attribute, attrRight: Attribute) =>
        evaluateBinaryForTwoColumns(colStatsMap, op, attrLeft, attrRight, update)

      case _ =>
        // TODO: it's difficult to support string operators without advanced statistics.
        // Hence, these string operators Like(_, _) | Contains(_, _) | StartsWith(_, _)
        // | EndsWith(_, _) are not supported yet
//        logDebug("[CBO] Unsupported filter condition: " + condition)
        None
    }
  }

  /**
    * Returns a percentage of rows meeting an equality (=) expression.
    * This method evaluates the equality predicate for all data types.
    *
    * For EqualNullSafe (<=>), if the literal is not null, result will be the same as EqualTo;
    * if the literal is null, the condition will be changed to IsNull after optimization.
    * So we don't need specific logic for EqualNullSafe here.
    *
    * @param attr    an Attribute (or a column)
    * @param literal a literal value (or constant)
    * @param update  a boolean flag to specify if we need to update ColumnStat of a given column
    *                for subsequent conditions
    * @return an optional double value to show the percentage of rows meeting a given condition
    */
  def evaluateEquality(
                        colStatsMap: mutable.HashMap[String, ColumnStat],
                        attr: Attribute,
                        literal: Literal,
                        update: Boolean): Option[Double] = {

    if (!colStatsMap.isEmpty) {
//      logDebug("[CBO] No statistics for " + attr)
      return None
    }
    val colStat = colStatsMap(attr.name)
    // decide if the value is in [min, max] of the column.
    // We currently don't store min/max for binary/string type.
    // Hence, we assume it is in boundary for binary/string type.
    val statsInterval = ValueInterval(colStat.min, colStat.max, attr.dataType)
    if (statsInterval.contains(literal)) {
      if (update) {
        // We update ColumnStat structure after apply this equality predicate:
        // Set distinctCount to 1, nullCount to 0, and min/max values (if exist) to the literal
        // value.
        val newStats = attr.dataType match {
//          case StringType | BinaryType =>
          case StringType =>
            colStat.copy(distinctCount = Some(1), nullCount = Some(0))
          case _ =>
//            colStat.copy(distinctCount = Some(1), min = Some(literal.value),
            //TODO
            colStat.copy(distinctCount = Some(1), min = Some(0.0),
              max = Some(0.0), nullCount = Some(0))
        }
        colStatsMap.update(attr.name, newStats)
      }
      Some(computeEqualityPossibilityByHistogram(literal, colStat))
    } else { // not in interval
      Some(0.0)
    }
  }

  /**
    * Returns a percentage of rows meeting a binary comparison expression.
    *
    * @param op      a binary comparison operator such as =, <, <=, >, >=
    * @param attr    an Attribute (or a column)
    * @param literal a literal value (or constant)
    * @param update  a boolean flag to specify if we need to update ColumnStat of a given column
    *                for subsequent conditions
    * @return an optional double value to show the percentage of rows meeting a given condition
    *         It returns None if no statistics exists for a given column or wrong value.
    */
  def evaluateBinary(
                      colStatsMap: mutable.HashMap[String, ColumnStat],
                      op: BinaryComparison,
                      attr: Attribute,
                      literal: Literal,
                      update: Boolean): Option[Double] = {
//    val colStat = colStatsMap(attr.name)
    if (!colStatsMap.contains(attr.name)) {
//      logDebug("[CBO] No statistics for " + attr)
      return None
    }

    attr.dataType match {
//      case _: NumericType | DateType | TimestampType | BooleanType =>
      case _: NumericType | BooleanType =>
      evaluateBinaryForNumeric(colStatsMap, op, attr, literal, update)
//      case StringType | BinaryType =>
      case StringType  =>
        // TODO: It is difficult to support other binary comparisons for String/Binary
        // type without min/max and advanced statistics like histogram.
//        logDebug("[CBO] No range comparison statistics for String/Binary type " + attr)
        None
    }
  }


  /**
    * Returns a percentage of rows meeting "IN" operator expression.
    * This method evaluates the equality predicate for all data types.
    *
    * @param attr   an Attribute (or a column)
    * @param hSet   a set of literal values
    * @param update a boolean flag to specify if we need to update ColumnStat of a given column
    *               for subsequent conditions
    * @return an optional double value to show the percentage of rows meeting a given condition
    *         It returns None if no statistics exists for a given column.
    */

  def evaluateInSet(
                     attr: Attribute,
                     hSet: Set[Any],
                     update: Boolean): Option[Double] = {
    null
  }

  /**
    * Returns a percentage of rows meeting "IS NULL" or "IS NOT NULL" condition.
    *
    * @param attr   an Attribute (or a column)
    * @param isNull set to true for "IS NULL" condition.  set to false for "IS NOT NULL" condition
    * @param update a boolean flag to specify if we need to update ColumnStat of a given column
    *               for subsequent conditions
    * @return an optional double value to show the percentage of rows meeting a given condition
    *         It returns None if no statistics collected for a given column.
    */
  def evaluateNullCheck(
                         attr: Attribute,
                         isNull: Boolean,
                         update: Boolean): Option[Double] = {
    null
  }

  /**
    * Returns a percentage of rows meeting a binary comparison expression.
    * This method evaluate expression for Numeric/Date/Timestamp/Boolean columns.
    *
    * @param op a binary comparison operator such as =, <, <=, >, >=
    * @param attr an Attribute (or a column)
    * @param literal a literal value (or constant)
    * @param update a boolean flag to specify if we need to update ColumnStat of a given column
    *               for subsequent conditions
    * @return an optional double value to show the percentage of rows meeting a given condition
    */
  def evaluateBinaryForNumeric(
                                colStatsMap: mutable.HashMap[String, ColumnStat],
                                op: BinaryComparison,
                                attr: Attribute,
                                literal: Literal,
                                update: Boolean): Option[Double] = {

    if (!ColumnStatsMap(colStatsMap).hasMinMaxStats(attr) || !ColumnStatsMap(colStatsMap).hasDistinctCount(attr)) {
//      logDebug("[CBO] No statistics for " + attr)
      return None
    }

    val colStat = colStatsMap(attr.name)
    val statsInterval =
      ValueInterval(colStat.min, colStat.max, attr.dataType).asInstanceOf[NumericValueInterval]
    val max = statsInterval.max
    val min = statsInterval.min
    val ndv = colStat.distinctCount.get.toDouble

    // determine the overlapping degree between predicate interval and column's interval
    val numericLiteral = EstimationUtils.toDouble(literal.value, literal.dataType)
    val (noOverlap: Boolean, completeOverlap: Boolean) = op match {
      case _: LessThan =>
        (numericLiteral <= min, numericLiteral > max)
      case _: LessThanOrEqual =>
        (numericLiteral < min, numericLiteral >= max)
      case _: GreaterThan =>
        (numericLiteral >= max, numericLiteral < min)
      case _: GreaterThanOrEqual =>
        (numericLiteral > max, numericLiteral <= min)
    }

    var percent = 1.0
    if (noOverlap) {
      percent = 0.0
    } else if (completeOverlap) {
      percent = 1.0
    } else {
      // This is the partial overlap case:

      if (colStat.histogram.isEmpty) {
        // Without advanced statistics like histogram, we assume uniform data distribution.
        // We just prorate the adjusted range over the initial range to compute filter selectivity.
        assert(max > min)
        percent = op match {
          case _: LessThan =>
            if (numericLiteral == max) {
              // If the literal value is right on the boundary, we can minus the part of the
              // boundary value (1/ndv).
              1.0 - 1.0 / ndv
            } else {
              (numericLiteral - min) / (max - min)
            }
          case _: LessThanOrEqual =>
            if (numericLiteral == min) {
              // The boundary value is the only satisfying value.
              1.0 / ndv
            } else {
              (numericLiteral - min) / (max - min)
            }
          case _: GreaterThan =>
            if (numericLiteral == min) {
              1.0 - 1.0 / ndv
            } else {
              (max - numericLiteral) / (max - min)
            }
          case _: GreaterThanOrEqual =>
            if (numericLiteral == max) {
              1.0 / ndv
            } else {
              (max - numericLiteral) / (max - min)
            }
        }
      } else {
        percent = computeComparisonPossibilityByHistogram(op, literal, colStat)
      }

      if (update) {
        //        val newValue = Option(literal.value)
        //        val newValue = Option[OldInternalDataType]
        //        val newValue: Option[OldInternalDataType] = Option[OldInternalDataType]
        val newValue = Option((literal.value).asInstanceOf[Double])
        var newMax = colStat.max
        var newMin = colStat.min

        op match {
          case _: GreaterThan | _: GreaterThanOrEqual =>
            newMin = newValue
          case _: LessThan | _: LessThanOrEqual =>
            newMax = newValue
        }

        val newStats = colStat.copy(distinctCount = Some(ceil(ndv * percent)),
          min = newMin, max = newMax, nullCount = Some(0))

        ColumnStatsMap(colStatsMap).update(attr, newStats)
      }
    }

    Some(percent)
  }

  /**
    * Returns a percentage of rows meeting a binary comparison expression containing two columns.
    * In SQL queries, we also see predicate expressions involving two columns
    * such as "column-1 (op) column-2" where column-1 and column-2 belong to same table.
    * Note that, if column-1 and column-2 belong to different tables, then it is a join
    * operator's work, NOT a filter operator's work.
    *
    * @param op        a binary comparison operator, including =, <=>, <, <=, >, >=
    * @param attrLeft  the left Attribute (or a column)
    * @param attrRight the right Attribute (or a column)
    * @param update    a boolean flag to specify if we need to update ColumnStat of the given columns
    *                  for subsequent conditions
    * @return an optional double value to show the percentage of rows meeting a given condition
    */
  def evaluateBinaryForTwoColumns(
                                   colStatsMap: mutable.HashMap[String, ColumnStat],
                                   op: BinaryComparison,
                                   attrLeft: Attribute,
                                   attrRight: Attribute,
                                   update: Boolean): Option[Double] = {

    if (!ColumnStatsMap(colStatsMap).hasCountStats(attrLeft)) {
//      logDebug("[CBO] No statistics for " + attrLeft)
      return None
    }
    if (!ColumnStatsMap(colStatsMap).hasCountStats(attrRight)) {
//      logDebug("[CBO] No statistics for " + attrRight)
      return None
    }

    attrLeft.dataType match {
      case StringType =>
//      case StringType | BinaryType =>
        // TODO: It is difficult to support other binary comparisons for String/Binary
        // type without min/max and advanced statistics like histogram.
//        logDebug("[CBO] No range comparison statistics for String/Binary type " + attrLeft)
        return None
      case _ =>
        if (!ColumnStatsMap(colStatsMap).hasMinMaxStats(attrLeft)) {
//          logDebug("[CBO] No min/max statistics for " + attrLeft)
          return None
        }
        if (!ColumnStatsMap(colStatsMap).hasMinMaxStats(attrRight)) {
//          logDebug("[CBO] No min/max statistics for " + attrRight)
          return None
        }
    }

    val colStatLeft = colStatsMap(attrLeft.name)
    val statsIntervalLeft = ValueInterval(colStatLeft.min, colStatLeft.max, attrLeft.dataType)
      .asInstanceOf[NumericValueInterval]
    val maxLeft = statsIntervalLeft.max
    val minLeft = statsIntervalLeft.min

    val colStatRight = colStatsMap(attrRight.name)
    val statsIntervalRight = ValueInterval(colStatRight.min, colStatRight.max, attrRight.dataType)
      .asInstanceOf[NumericValueInterval]
    val maxRight = statsIntervalRight.max
    val minRight = statsIntervalRight.min

    // determine the overlapping degree between predicate interval and column's interval
    val allNotNull = (colStatLeft.nullCount.get == 0) && (colStatRight.nullCount.get == 0)
    val (noOverlap: Boolean, completeOverlap: Boolean) = op match {
      // Left < Right or Left <= Right
      // - no overlap:
      //      minRight           maxRight     minLeft       maxLeft
      // --------+------------------+------------+-------------+------->
      // - complete overlap: (If null values exists, we set it to partial overlap.)
      //      minLeft            maxLeft      minRight      maxRight
      // --------+------------------+------------+-------------+------->
      case _: LessThan =>
        (minLeft >= maxRight, (maxLeft < minRight) && allNotNull)
      case _: LessThanOrEqual =>
        (minLeft > maxRight, (maxLeft <= minRight) && allNotNull)

      // Left > Right or Left >= Right
      // - no overlap:
      //      minLeft            maxLeft      minRight      maxRight
      // --------+------------------+------------+-------------+------->
      // - complete overlap: (If null values exists, we set it to partial overlap.)
      //      minRight           maxRight     minLeft       maxLeft
      // --------+------------------+------------+-------------+------->
      case _: GreaterThan =>
        (maxLeft <= minRight, (minLeft > maxRight) && allNotNull)
      case _: GreaterThanOrEqual =>
        (maxLeft < minRight, (minLeft >= maxRight) && allNotNull)

      // Left = Right or Left <=> Right
      // - no overlap:
      //      minLeft            maxLeft      minRight      maxRight
      // --------+------------------+------------+-------------+------->
      //      minRight           maxRight     minLeft       maxLeft
      // --------+------------------+------------+-------------+------->
      // - complete overlap:
      //      minLeft            maxLeft
      //      minRight           maxRight
      // --------+------------------+------->
      case _: EqualTo =>
        ((maxLeft < minRight) || (maxRight < minLeft),
          (minLeft == minRight) && (maxLeft == maxRight) && allNotNull
            && (colStatLeft.distinctCount.get == colStatRight.distinctCount.get)
        )
      case _: EqualNullSafe =>
        // For null-safe equality, we use a very restrictive condition to evaluate its overlap.
        // If null values exists, we set it to partial overlap.
        (((maxLeft < minRight) || (maxRight < minLeft)) && allNotNull,
          (minLeft == minRight) && (maxLeft == maxRight) && allNotNull
            && (colStatLeft.distinctCount.get == colStatRight.distinctCount.get)
        )
    }

    var percent = 1.0
    if (noOverlap) {
      percent = 0.0
    } else if (completeOverlap) {
      percent = 1.0
    } else {
      // For partial overlap, we use an empirical value 1/3 as suggested by the book
      // "Database Systems, the complete book".
      percent = 1.0 / 3.0

      if (update) {
        // Need to adjust new min/max after the filter condition is applied

        val ndvLeft = BigDecimal(colStatLeft.distinctCount.get)
        val newNdvLeft = ceil(ndvLeft * percent)
        val ndvRight = BigDecimal(colStatRight.distinctCount.get)
        val newNdvRight = ceil(ndvRight * percent)

        var newMaxLeft = colStatLeft.max
        var newMinLeft = colStatLeft.min
        var newMaxRight = colStatRight.max
        var newMinRight = colStatRight.min

        op match {
          case _: LessThan | _: LessThanOrEqual =>
            // the left side should be less than the right side.
            // If not, we need to adjust it to narrow the range.
            // Left < Right or Left <= Right
            //      minRight     <     minLeft
            // --------+******************+------->
            //              filtered      ^
            //                            |
            //                        newMinRight
            //
            //      maxRight     <     maxLeft
            // --------+******************+------->
            //         ^    filtered
            //         |
            //     newMaxLeft
            if (minLeft > minRight) newMinRight = colStatLeft.min
            if (maxLeft > maxRight) newMaxLeft = colStatRight.max

          case _: GreaterThan | _: GreaterThanOrEqual =>
            // the left side should be greater than the right side.
            // If not, we need to adjust it to narrow the range.
            // Left > Right or Left >= Right
            //      minLeft     <      minRight
            // --------+******************+------->
            //              filtered      ^
            //                            |
            //                        newMinLeft
            //
            //      maxLeft     <      maxRight
            // --------+******************+------->
            //         ^    filtered
            //         |
            //     newMaxRight
            if (minLeft < minRight) newMinLeft = colStatRight.min
            if (maxLeft < maxRight) newMaxRight = colStatLeft.max

          case _: EqualTo | _: EqualNullSafe =>
            // need to set new min to the larger min value, and
            // set the new max to the smaller max value.
            // Left = Right or Left <=> Right
            //      minLeft     <      minRight
            // --------+******************+------->
            //              filtered      ^
            //                            |
            //                        newMinLeft
            //
            //      minRight    <=     minLeft
            // --------+******************+------->
            //              filtered      ^
            //                            |
            //                        newMinRight
            //
            //      maxLeft     <      maxRight
            // --------+******************+------->
            //         ^    filtered
            //         |
            //     newMaxRight
            //
            //      maxRight    <=     maxLeft
            // --------+******************+------->
            //         ^    filtered
            //         |
            //     newMaxLeft
            if (minLeft < minRight) {
              newMinLeft = colStatRight.min
            } else {
              newMinRight = colStatLeft.min
            }
            if (maxLeft < maxRight) {
              newMaxRight = colStatLeft.max
            } else {
              newMaxLeft = colStatRight.max
            }
        }

        val newStatsLeft = colStatLeft.copy(distinctCount = Some(newNdvLeft), min = newMinLeft,
          max = newMaxLeft)
        colStatsMap(attrLeft.name) = newStatsLeft
        val newStatsRight = colStatRight.copy(distinctCount = Some(newNdvRight), min = newMinRight,
          max = newMaxRight)
        colStatsMap(attrRight.name) = newStatsRight
      }
    }

    Some(percent)
  }

  // Bound result in [0, 1]
  private def boundProbability(p: Double): Double = {
    Math.max(0.0, Math.min(1.0, p))
  }


  /**
    * Computes the possibility of an equality predicate using histogram.
    */
  private def computeEqualityPossibilityByHistogram(literal: Literal, colStat: ColumnStat): Double = {
    val datum = EstimationUtils.toDouble(literal.value, literal.dataType)
    val histogram = colStat.histogram.get

    // find bins where column's current min and max locate.  Note that a column's [min, max]
    // range may change due to another condition applied earlier.
    val min = EstimationUtils.toDouble(colStat.min.get, literal.dataType)
    val max = EstimationUtils.toDouble(colStat.max.get, literal.dataType)

    // compute how many bins the column's current valid range [min, max] occupies.
    val numBinsHoldingEntireRange = EstimationUtils.numBinsHoldingRange(
      upperBound = max,
      upperBoundInclusive = true,
      lowerBound = min,
      lowerBoundInclusive = true,
      histogram.bins)

    val numBinsHoldingDatum = EstimationUtils.numBinsHoldingRange(
      upperBound = datum,
      upperBoundInclusive = true,
      lowerBound = datum,
      lowerBoundInclusive = true,
      histogram.bins)

    numBinsHoldingDatum / numBinsHoldingEntireRange
  }

  /**
    * Computes the possibility of a comparison predicate using histogram.
    */
  private def computeComparisonPossibilityByHistogram(
                                                       op: BinaryComparison, literal: Literal, colStat: ColumnStat): Double = {
    val datum = EstimationUtils.toDouble(literal.value, literal.dataType)
    val histogram = colStat.histogram.get

    // find bins where column's current min and max locate.  Note that a column's [min, max]
    // range may change due to another condition applied earlier.
    val min = EstimationUtils.toDouble(colStat.min.get, literal.dataType)
    val max = EstimationUtils.toDouble(colStat.max.get, literal.dataType)

    // compute how many bins the column's current valid range [min, max] occupies.
    val numBinsHoldingEntireRange = EstimationUtils.numBinsHoldingRange(
      max, upperBoundInclusive = true, min, lowerBoundInclusive = true, histogram.bins)

    val numBinsHoldingRange = op match {
      // LessThan and LessThanOrEqual share the same logic, the only difference is whether to
      // include the upperBound in the range.
      case _: LessThan =>
        EstimationUtils.numBinsHoldingRange(
          upperBound = datum,
          upperBoundInclusive = false,
          lowerBound = min,
          lowerBoundInclusive = true,
          histogram.bins)
      case _: LessThanOrEqual =>
        EstimationUtils.numBinsHoldingRange(
          upperBound = datum,
          upperBoundInclusive = true,
          lowerBound = min,
          lowerBoundInclusive = true,
          histogram.bins)

      // GreaterThan and GreaterThanOrEqual share the same logic, the only difference is whether to
      // include the lowerBound in the range.
      case _: GreaterThan =>
        EstimationUtils.numBinsHoldingRange(
          upperBound = max,
          upperBoundInclusive = true,
          lowerBound = datum,
          lowerBoundInclusive = false,
          histogram.bins)
      case _: GreaterThanOrEqual =>
        EstimationUtils.numBinsHoldingRange(
          upperBound = max,
          upperBoundInclusive = true,
          lowerBound = datum,
          lowerBoundInclusive = true,
          histogram.bins)
    }

    numBinsHoldingRange / numBinsHoldingEntireRange
  }
}

/**
	* This class contains the original column stats from child, and maintains the updated column stats.
	* We will update the corresponding ColumnStats for a column after we apply a predicate condition.
	* For example, column c has [min, max] value as [0, 100].  In a range condition such as
	* (c > 40 AND c <= 50), we need to set the column's [min, max] value to [40, 100] after we
	* evaluate the first condition c > 40. We also need to set the column's [min, max] value to
	* [40, 50] after we evaluate the second condition c <= 50.
	*
	* @param originalMap Original column stats from child.
	*/
case class ColumnStatsMap(originalMap: mutable.HashMap[String, ColumnStat]) {
//case class ColumnStatsMap(originalMap: AttributeMap[ColumnStat]) {

	/** This map maintains the latest column stats. */
//	private val updatedMap: mutable.Map[ExprId, (Attribute, ColumnStat)] = mutable.HashMap.empty
	private val updatedMap: mutable.Map[String, ColumnStat] = mutable.HashMap.empty

//	def contains(a: Attribute): Boolean = updatedMap.contains(a.exprId) || originalMap.contains(a)
   	def contains(a: Attribute): Boolean = originalMap.contains(a.name)

	/**
		* Gets an Option of column stat for the given attribute.
		* Prefer the column stat in updatedMap than that in originalMap,
		* because updatedMap has the latest (updated) column stats.
		*/
	def get(a: Attribute): Option[ColumnStat] = {
//		if (updatedMap.contains(a.exprId)) {
    if (updatedMap.contains(a.name)) {
			updatedMap.get(a.name)
		} else {
			originalMap.get(a.name)
		}
	}

	def hasCountStats(a: Attribute): Boolean =
		get(a).exists(_.hasCountStats)

	def hasDistinctCount(a: Attribute): Boolean =
		get(a).exists(_.distinctCount.isDefined)

	def hasMinMaxStats(a: Attribute): Boolean =
		get(a).exists(_.hasMinMaxStats)

	/**
		* Gets column stat for the given attribute. Prefer the column stat in updatedMap than that in
		* originalMap, because updatedMap has the latest (updated) column stats.
		*/
	def apply(a: Attribute): ColumnStat = {
		get(a).get
	}

	/** Updates column stats in updatedMap. */
//	def update(a: Attribute, stats: ColumnStat): Unit = updatedMap.update(a.exprId, a -> stats)
    def update(a: Attribute, stats: ColumnStat): Unit = updatedMap.update(a.name, stats)

	/**
		* Collects updated column stats; scales down column count stats if the
		* number of rows decreases after this Filter operator.
		*/
	def outputColumnStats(rowsBeforeFilter: BigInt, rowsAfterFilter: BigInt)
	: mutable.HashMap[String, ColumnStat] = {
		val newColumnStats = originalMap.map { case (attr, oriColStat) =>
			attr -> oriColStat.updateCountStats(
//				rowsBeforeFilter, rowsAfterFilter, updatedMap.get(attr.exprId).map(_._2))
          rowsBeforeFilter, rowsAfterFilter, updatedMap.get(attr))
		}
//    AttributeMap(newColumnStats.toSeq)
    newColumnStats
  }
}