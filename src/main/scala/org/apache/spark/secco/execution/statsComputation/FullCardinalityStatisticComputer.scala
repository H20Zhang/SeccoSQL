package org.apache.spark.secco.execution.statsComputation

import org.apache.spark.secco.execution.{OldInternalBlock, RowBlockOld}
import org.apache.spark.secco.optimization.statsEstimation.Statistics
import org.apache.spark.secco.util.`extension`.SeqExtension
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/** The computer for computing only the full cardinality (cardinality per subset attributes) statistic of the content
  */
object FullCardinalityStatisticComputer extends StatisticComputer {

  def compute(
      attributes: Seq[String],
      content: RDD[OldInternalBlock]
  ): Statistics = {
    val allAttrSubsets = SeqExtension.subset(attributes)
    val allAttrSubSetsAndPos = allAttrSubsets
      .map { subsetAttrs =>
        subsetAttrs.map(f => (f, attributes.indexOf(f)))
      }
      .map(f => (f.map(_._1).toSet, f.map(_._2).toArray))
    val allAttrSubSetSize = new mutable.HashMap[Set[String], Long]

    //count distinct numbers of value of attributes and
    // cardinality of the relation.
    allAttrSubSetsAndPos.foreach { case (subsetAttrs, pos) =>
      if (pos.size < attributes.size) {
        allAttrSubSetSize(subsetAttrs) = content
          .mapPartitions { blocks =>
            blocks.flatMap { block =>
              block match {
                case RowBlockOld(output, blockContent) =>
                  blockContent.content.iterator.map(tuple => pos.map(tuple))
                case _ =>
                  throw new Exception("only RowBlock supports statistic")
              }
            }
          }
          .countApproxDistinct(0.10)
      } else {
        allAttrSubSetSize(subsetAttrs) = content
          .mapPartitions { blocks =>
            blocks.flatMap { block =>
              block match {
                case RowBlockOld(output, blockContent) =>
                  blockContent.content.iterator.map(tuple => pos.map(tuple))
                case _ =>
                  throw new Exception("only RowBlock supports statistic")
              }
            }
          }
          .count()
      }

    }

    Statistics(
      rowCount = allAttrSubSetSize(attributes.toSet),
      fullCardinality = allAttrSubSetSize
    )
  }
}
