package org.apache.spark.secco.execution.plan.computation.optimize

import org.apache.spark.secco.PreferenceLevel
import org.apache.spark.secco.PreferenceLevel.PreferenceLevel
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper

case class LeapFrogOrderCost(
    attributeOrder: Seq[String],
    schemas: Seq[Seq[String]],
    statisticMap: Map[Seq[String], StatisticKeeper]
) {

  val arity = attributeOrder.size
  val maximalPos = arity - 1

  //relative orders for attribute inside each schema
  val schema2RelativeAttributeOrder = schemas.map { attributes =>
    (attributes, attributeOrder.filter(attributes.contains))
  }.toMap

  //relevant relation for each i-th attributes in attrOrder
  val relevantRelations = attributeOrder.map { attrId =>
    schemas.filter(_.contains(attrId))
  }

  def cost(): Long = cost(maximalPos)

  //find the relative degree for each relation between 0 to i-th attributes and 0 to i-1 -th attributes
  private def relativeDegree(schema: Seq[String], i: Int): Long = {
    val attribute = attributeOrder(i)
    val relativeAttributeOrder = schema2RelativeAttributeOrder(schema)
    val pos = relativeAttributeOrder.indexOf(attribute)
    val prefixAttrs = relativeAttributeOrder.slice(0, pos)
    val prefixAndCurAttrs = relativeAttributeOrder.slice(0, pos + 1)
    statisticMap(schema)
      .fullCardinalityStatistic()
      .relativeDegree(prefixAndCurAttrs, prefixAttrs)
  }

  //degree is the min set size when performing intersection for determining the value for i-th attributes
  private def relativeDegree(i: Int): Long = {
    relevantRelations(i).map(schema => relativeDegree(schema, i)).min
  }

  private def size(i: Int): Long = {
    if (i == 0) {
      relativeDegree(0)
    } else {
      size(i - 1) * relativeDegree(i)
    }
  }

  private def cost(i: Int): Long = {
    if (i == 0) {
      size(0)
    } else {
      cost(i - 1) + size(i - 1) * relativeDegree(i)
    }
  }
}

class LeapFrogOrderComputer(
    schemas: Seq[Seq[String]],
    statisticMap: Map[Seq[String], StatisticKeeper]
) {

  val attributes = schemas.flatMap(identity).distinct.toArray

  def genAllAttributeOrder(): Seq[Seq[String]] = {
    attributes.permutations.map(_.toSeq).toSeq
  }

  def genAllAttributeOrderCost(): Seq[(Seq[String], Long)] = {
    val allOrder = genAllAttributeOrder()
    val allCost =
      allOrder.map(attributeOrder =>
        LeapFrogOrderCost(attributeOrder, schemas, statisticMap).cost()
      )
    allOrder.zip(allCost)
  }

  def optimalOrder(): (Seq[String], Long) = {
    genAllAttributeOrderCost().minBy(_._2)
  }

  def optimalOrderUnderPreferredAttributes(
      preferredAttributeSet: Set[String],
      preferenceLevel: PreferenceLevel
  ): (Seq[String], Long) = {

    val commonPreferredAttributeSet =
      preferredAttributeSet.intersect(attributes.toSet)
    val commonPreferredAttributeSize = commonPreferredAttributeSet.size
    val preferredAttributeSize = preferredAttributeSet.size

    preferenceLevel match {
      case PreferenceLevel.SatisfyAll =>
        genAllAttributeOrderCost()
          .filter {
            case (attributeOrder, _) =>
              attributeOrder
                .take(preferredAttributeSize)
                .toSet
                .subsetOf(preferredAttributeSet)
          }
          .minBy(_._2)
      case PreferenceLevel.SatisfyOne =>
        genAllAttributeOrderCost()
          .filter {
            case (attributeOrder, _) =>
              attributeOrder
                .take(1)
                .toSet
                .intersect(commonPreferredAttributeSet)
                .nonEmpty
          }
          .minBy(_._2)
      case PreferenceLevel.MaybeSatisfy =>
        val sortedAttributeOrders = genAllAttributeOrderCost().sortBy(_._2)

        val minCost = sortedAttributeOrders.head._2
        val minCostAttributeOrders =
          sortedAttributeOrders.filter(_._2 == minCost)

//        //DEBUG
//        pprint.pprintln(sortedAttributeOrders)
//        pprint.pprintln(minCostAttributeOrders)

        val matchedMinCostAttributeOrder = minCostAttributeOrders.sortBy {
          case (attributeOrder, _) =>
            attributeOrder
              .take(commonPreferredAttributeSize)
              .toSet
              .intersect(commonPreferredAttributeSet)
              .size
        }.reverse
        if (matchedMinCostAttributeOrder.nonEmpty) {
          matchedMinCostAttributeOrder.head
        } else {
          minCostAttributeOrders.head
        }
    }

  }
}
