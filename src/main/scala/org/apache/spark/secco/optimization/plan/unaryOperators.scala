package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.{Catalog, CatalogColumn}
import org.apache.spark.secco.expression.{Expression, NamedExpression}
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.plan.Aggregate.counter
import org.apache.spark.secco.execution.SharedParameter
import org.apache.spark.secco.util.counter.Counter

import scala.collection.mutable

abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan
  override final def children: Seq[LogicalPlan] = child :: Nil
}

/**
  * An operator that cache the output of [[child]]
  * @param child child logical plan
  */
case class Cache(child: LogicalPlan, mode: ExecMode = ExecMode.Atomic)
    extends UnaryNode {

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld
}

/**
  * An operator that apply a serials of function f of form "Ax+b" to transform the output of [[child]]
  * @param child child logical plan
  */
case class Transform(
    child: LogicalPlan,
    f: Seq[String],
    outputOld: Seq[String],
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def primaryKeys: Seq[String] = {
//    //DEBUG
//    println(child.primaryKeys)
//    println(child)
    child.primaryKeys
  }
}

/**
  * RootNode of the logical plan
  *
  * @param child child logical plan
  * @param mode execution mode
  */
case class RootNode(
    child: LogicalPlan,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {

  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld

  override def output: Seq[Attribute] = child.output
}

/**
  * An operator that filers the output of [[child]] using [[selectionExprs]]
  *
  * @param child child logical plan
  * @param selectionExprs SelectionExpr is of form "A < B".
  * @param mode execution mode
  */
case class Filter(
    child: LogicalPlan,
    selectionExprs: Seq[(String, String, String)],
    mode: ExecMode,
    condition: Option[Expression] = None
) extends UnaryNode {

  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld

  override def output: Seq[Attribute] = child.output

  override def relationalSymbol: String =
    s"𝜎[${selectionExprs.map(f => s"${f._1}${f._2}${f._3}").mkString("&&")}]"
}

/**
  * An operator that project the output of [[child]] to attributes in [[projectionListOld]]
  *
  * @param child child logical plan
  * @param projectionListOld attributes to retain
  * @param mode execution mode
  */
case class Project(
    child: LogicalPlan,
    projectionListOld: Seq[String],
    mode: ExecMode,
    projectionList: Seq[NamedExpression] = Seq()
) extends UnaryNode {

  override def primaryKeys: Seq[String] = {
    val childPrimaryKeys = child.primaryKeys
    if (childPrimaryKeys.toSet.subsetOf(outputOld.toSet)) {
      childPrimaryKeys
    } else {
      Seq()
    }
  }

  /** The output attributes */
  override def outputOld: Seq[String] = projectionListOld

  override def output: Seq[Attribute] = projectionList.map(_.toAttribute)

  override def relationalSymbol: String =
    s"∏[${projectionListOld.mkString(",")}]"
}

/**
  * An operator that remove duplicate tuples the output of [[child]]
  *
  * @param child child logical plan
  * @param mode execution mode
  */
case class Distinct(
    child: LogicalPlan,
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {

  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld

  override def output: Seq[Attribute] = child.output

  override def relationalSymbol: String =
    s"∏"
}

/**
  * An operator that limit numbers output of [[child]]
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

  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld

  override def output: Seq[Attribute] = child.output

  override def relationalSymbol: String =
    s"Limit[${maxTuples}]"
}

/**
  * An operator that sort output of [[child]] according to sort order
  *
  * @param child child logical plan
  * @param mode execution mode
  */
case class Sort(
    child: LogicalPlan,
    sortOrder: Seq[(NamedExpression, Boolean)],
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {

  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld

  override def output: Seq[Attribute] = child.output

  override def relationalSymbol: String =
    s"Sort"
}

/**
  * An operator that aggregates output of [[child]]
  * @param child child logical plan
  * @param groupingListOld attributes to group
  * @param semiringListOld semi-ring, supported type: sum, count, min, max, i.e. (sum, A)
  * @param __producedOutputOld names of produced output, this param will be automatically computed
  * @param mode execution mode
  */
case class Aggregate(
    child: LogicalPlan,
    groupingListOld: Seq[String],
    semiringListOld: (String, String),
    private var __producedOutputOld: Seq[String] = Seq(),
    mode: ExecMode,
    groupingExpressions: Seq[Expression] = Seq(),
    aggregateExpressions: Seq[NamedExpression] = Seq()
) extends UnaryNode {

  override def primaryKeys: Seq[String] = {
    val childPrimaryKeys = child.primaryKeys
    if (childPrimaryKeys.toSet.subsetOf(outputOld.toSet)) {
      childPrimaryKeys
    } else {
      Seq()
    }
  }

  lazy val _producedOutput = {
    counter.increment()
    val id = counter.value
    s"${semiringListOld._1.head}${id}" :: Nil
  }

  override def producedOutput: Seq[String] =
    __producedOutputOld.size == 0 match {
      case true => {
        __producedOutputOld = _producedOutput
        __producedOutputOld
      }
      case false => __producedOutputOld
    }

  /** The output attributes */
  override def outputOld: Seq[String] = {
    val producedAttribute = producedOutput.head
    val column = CatalogColumn(producedAttribute)
    groupingListOld :+ producedAttribute
  }

  override def output: Seq[Attribute] =
    aggregateExpressions.map(_.toAttribute)

  override def relationalSymbol: String =
    s"[${groupingListOld.mkString(",")}]𝝪[${s"${semiringListOld._1}(${semiringListOld._2})"}]"

}

object Aggregate {
  val counter = SeccoSession.currentSession.sessionState.counterManager
    .getOrCreateCounter("plan", "aggregate")
}

/**
  * An operator that partition output of [[child]] with restriction on partition functions
  * specified by [[restriction]] and [[sibling]]
  *
  * Warning: This class should be used with care.
  * You should always copy "sibling", when you want to make a copy of this class
  *
  * @param child child plan to partition
  * @param mode [[ExecMode.Communication]]
  */
case class Partition(
    child: LogicalPlan,
    sharedRestriction: SharedParameter[mutable.HashMap[String, Int]],
    mode: ExecMode = ExecMode.Communication
) extends UnaryNode {

  /** restrictions */
  def restriction: Map[String, Int] = sharedRestriction.res.toMap

  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld

}

/**
  * An operator that assign table spefieced by [[tableIdentifier]] using output of [[child]]
  *
  * @param child child logical plan
  * @param tableIdentifier table to assign
  * @param mode [[ExecMode.Atomic]]
  */
case class Assign(
    child: LogicalPlan,
    tableIdentifier: String,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {

  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld
}

/**
  * An operator that updates table specified by [[tableIdentifier]] and delta table specified by [[deltaTableName]].
  * The output is relation specified by [[deltaTableName]]
  *
  * @param child child logical plan
  * @param tableIdentifier table to update
  * @param deltaTableName delta table to update
  * @param key key column
  * @param mode [[ExecMode.Atomic]]
  */
case class Update(
    child: LogicalPlan,
    tableIdentifier: String,
    deltaTableName: String,
    key: Seq[String],
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {

  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld
}

/**
  * An operator that return [[child]] with attributes replaced according to [[attrRenameMap]]
  *
  * @param child child logical plan
  * @param attrRenameMap mapping between attributes of [[child]] and user-defined new attributes
  * @param mode [[ExecMode.Atomic]]
  */
case class Rename(
    child: LogicalPlan,
    attrRenameMap: Map[String, String],
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = {
    child.outputOld.map(f => attrRenameMap.get(f).getOrElse(f))
  }
}

case class SubqueryAlias(
    alias: String,
    child: LogicalPlan,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {

  override def output: Seq[Attribute] =
    child.output.map(_.withQualifier(Some(alias)))

  /** The output attributes */
  override def outputOld: Seq[String] = child.outputOld
}

/**
  * An operator that executes [[child]] till fixed point and return table specified by [[returnTableIdentifier]]
  *
  * @param child the child logical plan to execute, usually the child returns a delta relation
  * @param returnTableIdentifier the table identifier of the return of this Operator
  * @param numRun the maximum numbers of times the op tree below iterative are run
  * @param mode [[ExecMode.Computation]]
  */
case class Iterative(
    child: LogicalPlan,
    returnTableIdentifier: String,
    numRun: Int,
    mode: ExecMode = ExecMode.Atomic
) extends UnaryNode {
  override def primaryKeys: Seq[String] = child.primaryKeys

  /** The output attributes */
  override def outputOld: Seq[String] = {
    val catalog = dlSession.sessionState.catalog
    catalog.getTable(returnTableIdentifier) match {
      case Some(table) => table.schema.map(_.columnName)
      case None =>
        throw new Exception(
          s"No such returnTableIdentifier:${returnTableIdentifier}"
        )
    }
  }
}
