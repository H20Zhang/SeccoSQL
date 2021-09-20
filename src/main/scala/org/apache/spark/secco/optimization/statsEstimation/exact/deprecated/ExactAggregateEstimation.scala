package org.apache.spark.secco.optimization.statsEstimation.exact.deprecated

import org.apache.spark.secco.optimization.plan.{Aggregate, Join, Relation}
import org.apache.spark.secco.optimization.statsEstimation.exact.{
  ExactStatsPlanVisitor,
  NoExactCardinalityException
}
import org.apache.spark.secco.optimization.statsEstimation.{
  Estimation,
  Statistics
}

object ExactAggregateEstimation extends Estimation[Aggregate] {

  private var _defaultSelectivity = 0.5
  private var _selectivityMap: Option[Map[(Set[String], Set[String]), Double]] =
    None

  def setDefaultSelectivity(selectivity: Double): Unit = {
    assert(
      selectivity >= 0.0 && selectivity <= 1.0,
      s"$selectivity is not in range [0, 1]"
    )
    _defaultSelectivity = selectivity
  }

  def getDefaultSelectivity(): Double = {
    _defaultSelectivity
  }

  def setSelectivityMap(
      selectivityMap: Map[(Set[String], Set[String]), Double]
  ): Unit = {
    assert(selectivityMap.forall {
      case (_, selectivity) => selectivity >= 0 && selectivity <= 1
    })
    _selectivityMap = Some(selectivityMap)
  }

  def get(): Map[(Set[String], Set[String]), Double] = {
    _selectivityMap.get
  }

  override def estimate(x: Aggregate): Option[Statistics] = {

    if (_selectivityMap.isEmpty) {
      val cardinality = ExactStatsPlanVisitor
        .visit(x.child)
        .rowCount
        .toLong * _defaultSelectivity toLong

      Some(
        Statistics(
          BigInt(cardinality)
        )
      )

    } else if (x.child.isInstanceOf[Relation]) {
      val relationNameSet = Set(x.child.asInstanceOf[Relation].tableName)

      val selectivityOpt =
        _selectivityMap.get.get((x.groupingListOld.toSet, relationNameSet))

      if (selectivityOpt.isEmpty) {
        throw new NoExactCardinalityException(x)
      } else {
        val cardinality = (ExactStatsPlanVisitor
          .visit(x.child)
          .rowCount
          .toLong * selectivityOpt.get).toLong
        Some(
          Statistics(
            BigInt(cardinality)
          )
        )
      }
    } else if (x.child.isInstanceOf[Join]) {
      assert(x.child.isInstanceOf[Join])
      val j = x.child.asInstanceOf[Join]

      val mergedPlan = Estimation.mergeJoin(j)
      assert(mergedPlan.isInstanceOf[Join])
      val mergedJoin = mergedPlan.asInstanceOf[Join]

      assert(mergedJoin.children.forall(_.isInstanceOf[Relation]))
      val relationNameSet =
        mergedJoin.children.map(f => f.asInstanceOf[Relation].tableName).toSet

      val selectivityOpt =
        _selectivityMap.get.get((x.groupingListOld.toSet, relationNameSet))

      if (selectivityOpt.isEmpty) {
        throw new NoExactCardinalityException(x)
      } else {
        val cardinality = (ExactStatsPlanVisitor
          .visit(x.child)
          .rowCount
          .toLong * selectivityOpt.get).toLong
        Some(
          Statistics(
            BigInt(cardinality)
          )
        )
      }
    } else {
      throw new NoExactCardinalityException(x)
    }

  }

}
