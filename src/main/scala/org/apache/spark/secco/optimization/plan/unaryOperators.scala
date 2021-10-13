package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.{Catalog, CatalogColumn, TableIdentifier}
import org.apache.spark.secco.expression.{
  Attribute,
  AttributeReference,
  Expression,
  NamedExpression
}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.execution.SharedParameter
import org.apache.spark.secco.expression.utils.AttributeSet
import scala.collection.mutable

/* ---------------------------------------------------------------------------------------------------------------------
 * This file contains logical plans with only one child.
 *
 * 0.  UnaryNode: base class of logical plan with only one child.
 * 1.  Cache: cache the child.
 * 2.  Transform(deprecated): transform the tuples of child by transformation functions.
 * 3.  RootNode: root node of the logical plan.
 * 4.  Filter: filter the tuples of child by predicates.
 * 5.  Project: project the columns of tuples of the child.
 * 6.  Distinct: prune duplicate tuples of the tuples of the child.
 * 7.  Limit: return limit numbers of tuples of the child
 * 8.  Sort: sort the tuples of the child by sorting functions
 * 9.  Aggregate: aggregate the tuples of the child by aggregate expression and grouping expression
 * 10. Partition: partition the tuples of the child into partitions.
 * 11. Assign: assign the tuples of the child to an catalogTable.
 * 12. Update: update the tuples of a relation by tuples of the child
 * 13. Rename: rename the attributes name of the child
 * 14. SubqueryAlias: assign the tuples of the child to an temporary catalogTable
 * 15. Iterative: iteratively evaluate the child until fixed numbers of round or converge, i.e., output of the child is
 *     empty. After that returns the tuples of the table specificed by returnTableIdentifier.
 * ---------------------------------------------------------------------------------------------------------------------
 */

/** A [[LogicalPlan]] with single child. */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan
  override final def children: Seq[LogicalPlan] = child :: Nil
}

/** A [[LogicalPlan]] that cache the output of child
  * @param child child logical plan
  */
case class Cache(child: LogicalPlan, mode: ExecMode = ExecMode.Atomic)
    extends UnaryNode {

  override def output: Seq[Attribute] = child.output
}

/** An operator that apply a serials of function f of form "Ax+b" to transform the output of [[child]]
  * @param child child logical plan
  */
@deprecated
case class Transform(
    child: LogicalPlan,
    f: Seq[String],
    override val output: Seq[Attribute],
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def primaryKey: Seq[Attribute] = child.primaryKey
}

/** A [[LogicalPlan]] that marks the root node of the operator tree.
  *
  * @param child child logical plan
  * @param mode execution mode
  */
case class RootNode(
    child: LogicalPlan,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = child.output
}

/** A [[LogicalPlan]] that filers the output of child by condition
  *
  * @param child child logical plan
  * @param condition predicates for filtering the tuples.
  * @param mode execution mode
  */
case class Filter(
    child: LogicalPlan,
    condition: Expression,
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = child.output

  override def relationalSymbol: String = {
    s"𝜎[${condition}]"
  }
}

/** A [[LogicalPlan]] that projects the output of child by projectionList
  *
  * @param child child logical plan
  * @param projectionList projection functions to be applied on tuples of child.
  * @param mode execution mode
  */
case class Project(
    child: LogicalPlan,
    projectionList: Seq[NamedExpression] = Seq(),
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = {
    val childPrimaryKeySet = AttributeSet(child.primaryKey)
    childPrimaryKeySet.intersect(outputSet).toSeq
  }

  override def output: Seq[Attribute] = projectionList.map(_.toAttribute)

  override def relationalSymbol: String =
    s"∏[${projectionList.mkString(",")}]"
}

/** An operator that remove duplicate tuples the output of [[child]]
  *
  * @param child child logical plan
  * @param mode execution mode
  */
case class Distinct(
    child: LogicalPlan,
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = child.output

  override def relationalSymbol: String =
    s"D"
}

/** An operator that limit numbers output of [[child]]
  *
  * @param child child logical plan
  * @param maxTuples max numbers of tuples to preserve
  * @param mode execution mode
  */
case class Limit(
    child: LogicalPlan,
    maxTuples: Int,
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = child.output

  override def relationalSymbol: String =
    s"L[${maxTuples}]"
}

/** An operator that sort output of `child` according to sort order
  *
  * @param child child logical plan
  * @param sortOrder the order to sort the output of `child`, by default we assume ascending order
  * @param mode execution mode
  */
case class Sort(
    child: LogicalPlan,
    sortOrder: Seq[(NamedExpression, Boolean)],
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = child.output

  override def relationalSymbol: String =
    s"Sort[${sortOrder.mkString(",")}]"
}

/** A [[LogicalPlan]] that aggregates output of [[child]]
  * @param child child logical plan
  * @param aggregateExpressions expressions used for aggregate the results
  * @param groupingExpressions attributes used for grouping the results
  * @param mode execution mode
  */
case class Aggregate(
    child: LogicalPlan,
    aggregateExpressions: Seq[NamedExpression],
    groupingExpressions: Seq[NamedExpression] = Seq(),
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = {
    val childPrimaryKeySet = AttributeSet(child.primaryKey)
    childPrimaryKeySet.intersect(outputSet).toSeq
  }

  override def output: Seq[Attribute] =
    aggregateExpressions.map(_.toAttribute)

  override def relationalSymbol: String =
    s"[${groupingExpressions.mkString(",")}]𝝪[${aggregateExpressions.mkString(",")}]"

}

object Aggregate {
  val counter = SeccoSession.currentSession.sessionState.counterManager
    .getOrCreateCounter("plan", "aggregate")
}

/** A [[LogicalPlan]] that partitions output of [[child]]
  *
  * @param child child plan to partition
  * @param sharedRestriction shared restrictions of hash functions for partitioning
  * @param mode [[ExecMode.Communication]]
  */
//TODO: we need to handle cases other than natural join, e.g., equi-join,
// where current sharedRestriction cannot work properly.
case class Partition(
    child: LogicalPlan,
    sharedRestriction: SharedParameter[mutable.HashMap[String, Int]],
    mode: ExecMode = ExecMode.Communication
) extends UnaryNode {

  /** shared restrictions for hash functions for partitioning */
  def restriction: Map[String, Int] = sharedRestriction.res.toMap

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = child.output

}

/** An operator that assign table specified by tableIdentifier using output of child
  *
  * @param child child logical plan
  * @param tableIdentifier the identifier of the table to assign the results of child
  * @param mode execution mode
  */
case class Assign(
    child: LogicalPlan,
    tableIdentifier: TableIdentifier,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = child.output

}

/** An [[LogicalPlan]] that updates table specified by tableIdentifier and delta table specified by deltaTableIdentifier.
  *
  * The output is a relation specified by deltaTableIdentifier.
  *
  * @param child child logical plan
  * @param tableIdentifier table to update
  * @param deltaTableIdentifier delta table to update
  * @param key key column
  * @param mode [[ExecMode.Atomic]]
  */
case class Update(
    child: LogicalPlan,
    tableIdentifier: TableIdentifier,
    deltaTableIdentifier: TableIdentifier,
    key: Seq[Attribute],
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = child.output
}

/** A [[LogicalPlan]] that return child with attributes replaced according to attrRenameMap.
  *
  * @param child child logical plan
  * @param attrRenameMap mapping between attributes of [[child]] and user-defined new attributes
  * @param mode [[ExecMode.Atomic]]
  */
@deprecated
case class Rename(
    child: LogicalPlan,
    attrRenameMap: Map[String, String],
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def primaryKeyOld: Seq[String] = child.primaryKeyOld

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = child.output
}

/** A [[LogicalPlan]] that execute a subquery and assign its results a name `alias` */
case class SubqueryAlias(
    alias: String,
    child: LogicalPlan,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {

  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] =
    child.output.map(_.withQualifier(Some(alias)))

}

/** An operator that executes [[child]] till fixed point and return table specified by [[returnTableIdentifier]]
  *
  * @param child the child logical plan to execute, usually the child returns a delta relation
  * @param returnTableIdentifier the table identifier of the return of this Operator
  * @param numRun the maximum numbers of times the op tree below iterative are run
  * @param mode [[ExecMode.Computation]]
  */
case class Iterative(
    child: LogicalPlan,
    returnTableIdentifier: TableIdentifier,
    numRun: Int,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def primaryKey: Seq[Attribute] = child.primaryKey

  override def output: Seq[Attribute] = {
    val catalog = dlSession.sessionState.catalog
    catalog.getTable(
      returnTableIdentifier.table,
      returnTableIdentifier.database
    ) match {
      case Some(table) =>
        table.schema.map(col =>
          AttributeReference(col.columnName, col.dataType)(qualifier =
            Some(returnTableIdentifier.table)
          )
        )
      case None =>
        throw new Exception(
          s"No such returnTableIdentifier:${returnTableIdentifier}"
        )
    }
  }
}
