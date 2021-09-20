package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.analysis.{
  MultiInstanceRelation,
  NoSuchTableException
}
import org.apache.spark.secco.catalog.{CachedDataManager, Catalog}
import org.apache.spark.secco.execution.InternalBlock
import org.apache.spark.secco.execution.statsComputation.HistogramStatisticComputer
import org.apache.spark.secco.expression.{Attribute, AttributeReference}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.statsEstimation.Statistics
import org.apache.spark.rdd.RDD

abstract class LeafNode extends LogicalPlan {
  override final def children: Seq[LogicalPlan] = Nil

  /** Leaf nodes that can survive analysis must define their own statistics. */
  def computeStats(): Statistics = throw new UnsupportedOperationException
}

/**
  * An operator that output table specified by [[tableName]]
  * @param tableName name of the table to scan
  * @param mode execution mode
  */
case class Relation(tableName: String, mode: ExecMode = ExecMode.Atomic)
    extends LeafNode
    with MultiInstanceRelation {

  override def primaryKeys: Seq[String] = {
    val catalog = dlSession.sessionState.catalog

    if (catalog.getTable(tableName).nonEmpty) {
      val table = catalog.getTable(tableName).get
      table.primaryKeys.map(_.columnName)
    } else {
      throw new NoSuchTableException(currentDatabase, tableName)
    }
  }

  /** The output attributes */
  override def outputOld: Seq[String] = {

    if (catalog.getTable(tableName).nonEmpty) {
      val table = catalog.getTable(tableName).get
      table.schema.map(_.columnName)
    } else {
      throw new NoSuchTableException(currentDatabase, tableName)
    }
  }

  override def output: Seq[Attribute] = {

    val cols = if (catalog.getTable(tableName).nonEmpty) {
      val table = catalog.getTable(tableName).get
      table.schema
    } else {
      throw new NoSuchTableException(currentDatabase, tableName)
    }

    cols.map(col =>
      AttributeReference(col.columnName, col.dataType)(qualifier =
        Some(tableName)
      )
    )
  }

  override def relationalSymbol: String = tableName

  override def computeStats(): Statistics = {

    if (catalog.getTable(tableName).nonEmpty) {
      val tableCatalog = catalog.getTable(tableName).get
      tableCatalog.stats match {
        case Some(statistics) => statistics
        case None =>
          val rawData =
            cachedDataManager(tableName).get.asInstanceOf[RDD[InternalBlock]]
          val attributes = outputOld
          val statistics =
            HistogramStatisticComputer.compute(attributes, rawData)
          catalog.alterTable(tableCatalog.copy(stats = Some(statistics)))

          statistics
      }

    } else {
      throw new NoSuchTableException(currentDatabase, tableName)
    }
  }

}

/**
  * An operator that holds an GHD node
  * @param chi attributes of the GHD node
  * @param lambda relations inside the GHD node
  * @param mode execution mode
  */
case class GHDNode(chi: Seq[LogicalPlan], lambda: Seq[String], mode: ExecMode)
    extends LeafNode {

  /** The output attributes */
  override def outputOld: Seq[String] = lambda
}

/**
  * An operator that acts as an placeholder to i-th input
  * @param pos i
  * @param outputOld output attributes
  */
case class PlaceHolder(pos: Int, outputOld: Seq[String]) extends LeafNode {
  override def mode: ExecMode = ExecMode.Atomic

  override def relationalSymbol: String = s"[${pos.toString}]"
}
