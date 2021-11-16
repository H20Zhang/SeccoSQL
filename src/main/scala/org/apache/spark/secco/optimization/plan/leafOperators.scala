package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.analysis.{
  MultiInstanceRelation,
  NoSuchTableException
}
import org.apache.spark.secco.catalog.{
  CachedDataManager,
  Catalog,
  TableIdentifier
}
import org.apache.spark.secco.execution.InternalBlock
import org.apache.spark.secco.execution.statsComputation.HistogramStatisticComputer
import org.apache.spark.secco.expression.{Attribute, AttributeReference}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.statsEstimation.Statistics
import org.apache.spark.rdd.RDD

/* ---------------------------------------------------------------------------------------------------------------------
 * This file contains logical plans with no child.
 *
 * 0.  LeafNode: base class of logical plan with no child.
 * 1.  Relation: logical plan that represent the relation in the database.
 * 2.  GHDNode: logical plan that represent an GHDNode in GHD.
 * 3.  PlaceHolder: logical plan that remembers an placeholder.
 * ---------------------------------------------------------------------------------------------------------------------
 */

/** A [[LogicalPlan]] with no child. */
abstract class LeafNode extends LogicalPlan {
  override final def children: Seq[LogicalPlan] = Nil

  /** Leaf nodes that can survive analysis must define their own statistics. */
  def computeStats(): Statistics = throw new UnsupportedOperationException
}

//case class LocalRelation

/** An operator that output table specified by [[tableIdentifier]]
  * @param tableIdentifier identifier of the table
  * @param mode execution mode
  */
case class Relation(
    tableIdentifier: TableIdentifier,
    mode: ExecMode = ExecMode.Atomic
) extends LeafNode
    with MultiInstanceRelation {

  override def primaryKey: Seq[Attribute] = {
    val cols = output
    val catalogTable =
      catalog.getTable(tableIdentifier.table, tableIdentifier.database).get

    cols.filter(attr =>
      catalogTable.primaryKeys.map(_.columnName).contains(attr.name)
    )
  }

  override lazy val output: Seq[Attribute] = {

    val cols =
      if (
        catalog
          .getTable(tableIdentifier.table, tableIdentifier.database)
          .nonEmpty
      ) {
        val table =
          catalog.getTable(tableIdentifier.table, tableIdentifier.database).get
        table.schema
      } else {
        throw new NoSuchTableException(
          tableIdentifier.database.getOrElse(currentDatabase),
          tableIdentifier.table
        )
      }

    cols.map(col =>
      AttributeReference(col.columnName, col.dataType)(qualifier =
        Some(tableIdentifier.table)
      )
    )
  }

  override def relationalSymbol: String = tableIdentifier.toString

  override def computeStats(): Statistics = {

    if (
      catalog.getTable(tableIdentifier.table, tableIdentifier.database).nonEmpty
    ) {
      val tableCatalog =
        catalog.getTable(tableIdentifier.table, tableIdentifier.database).get
      tableCatalog.stats match {
        case Some(statistics) => statistics
        case None =>
          val rawData =
            cachedDataManager(tableIdentifier.table).get
              .asInstanceOf[RDD[InternalBlock]]
          val attributes = output
          val attributeInString = attributes.map(_.name)
          val statistics =
            HistogramStatisticComputer.compute(attributeInString, rawData)
          catalog.alterTable(tableCatalog.copy(stats = Some(statistics)))

          statistics
      }

    } else {
      throw new NoSuchTableException(
        tableIdentifier.database.getOrElse(currentDatabase),
        tableIdentifier.table
      )
    }
  }

  override def newInstance(): LogicalPlan = copy()
}

/** An operator that holds an GHD node
  * @param chi attributes of the GHD node
  * @param lambda relations inside the GHD node
  * @param mode execution mode
  */
case class GHDNode(
    chi: Seq[LogicalPlan],
    lambda: Seq[Attribute],
    mode: ExecMode
) extends LeafNode {

  override def output: Seq[Attribute] = lambda
}

/** An operator that acts as an placeholder to i-th input
  * @param pos i
  * @param output output attributes
  */
case class PlaceHolder(pos: Int, override val output: Seq[Attribute])
    extends LeafNode {
  override def mode: ExecMode = ExecMode.Atomic

  override def relationalSymbol: String = s"[${pos.toString}]"
}
