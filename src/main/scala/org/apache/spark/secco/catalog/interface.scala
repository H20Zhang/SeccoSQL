package org.apache.spark.secco.catalog

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.execution.{SeccoPlan, InternalBlock}
import org.apache.spark.secco.optimization.statsEstimation.{
  ColumnStat,
  Statistics
}
import org.apache.spark.secco.types.{DataType, DoubleType}
import org.apache.spark.secco.util.counter.Counter
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * A function defined in the catalog.
  *
  * @param identifier name of the function
  * @param className fully qualified class name, e.g. "org.apache.spark.util.MyFunc"
  */
case class CatalogFunction(
    identifier: FunctionIdentifier,
    className: String
) {
  override def toString: String = s"$identifier"
}

object CatalogFunction {
  def apply(functionName: String, className: String): CatalogFunction =
    new CatalogFunction(FunctionIdentifier(functionName), className)
}

/**
  * A column of table in the catalog
  * @param columnName name of the column
  * @param dataType type of the data in the column, i.e., DoubleType, IntType
  * @param stat statistic of the column
  */
case class CatalogColumn(
    columnName: String,
    dataType: DataType = DoubleType,
    stat: Option[ColumnStat] = None
) {
  override def toString: String = s"$columnName"
}

/**
  * A table in the catalog
  */
abstract class AbstractCatalogTable extends Serializable {

  def identifier: TableIdentifier

  def schema: Seq[CatalogColumn]

  def primaryKeys: Seq[CatalogColumn]

  def stats: Option[Statistics]

  /** name of the  table */
  lazy val tableName = identifier.table

  /** database of the table */
  lazy val database = identifier.database

  lazy val quantifiedName = identifier.toString

  /** numbers of attributes in the schema */
  lazy val arity = schema.size

  /** attribute names of the columns */
  lazy val attributeNames = schema.map(_.columnName)

  def containColumn(column: CatalogColumn): Boolean = {
    schema.contains(column)
  }

  private lazy val colsToPosMap = schema.zipWithIndex.toMap

  def posForColumn(column: CatalogColumn) = {
    colsToPosMap(column)
  }

  override def toString: String = {
    s"$quantifiedName(${attributeNames.mkString("(", ", ", ")")})"
  }
}

//schema for persistent relation in disk or memory
case class CatalogTable(
    identifier: TableIdentifier,
    schema: Seq[CatalogColumn],
    primaryKeys: Seq[CatalogColumn],
    stats: Option[Statistics]
) extends AbstractCatalogTable {

  def attachData(content: RDD[InternalBlock]) = {
    val dataManager =
      SeccoSession.currentSession.sessionState.cachedDataManager
    dataManager.storeRelation(identifier.toString, content)
    this
  }

  def attachData(dataAddress: String) = {
    val dataManager =
      SeccoSession.currentSession.sessionState.cachedDataManager
    dataManager.storeRelation(identifier.toString, dataAddress)
    this
  }
}

object CatalogTable {

  def apply(
      identifier: TableIdentifier,
      schema: Seq[CatalogColumn]
  ): CatalogTable = new CatalogTable(identifier, schema, Seq(), None)

  def apply(
      tableName: String,
      schema: Seq[CatalogColumn]
  ): CatalogTable =
    new CatalogTable(TableIdentifier(tableName), schema, Seq(), None)

  def apply(
      identifier: TableIdentifier,
      schema: Seq[CatalogColumn],
      primaryKeys: Seq[CatalogColumn]
  ): CatalogTable = new CatalogTable(identifier, schema, primaryKeys, None)

  def apply(
      tableName: String,
      schema: Seq[CatalogColumn],
      primaryKeys: Seq[CatalogColumn]
  ): CatalogTable =
    new CatalogTable(TableIdentifier(tableName), schema, primaryKeys, None)

  def apply(tableNameAndcolNames: String*): CatalogTable = {

    assert(
      tableNameAndcolNames.size >= 2,
      s"there should be at least one attributes in $tableNameAndcolNames"
    )

    val primaryKeyRegex = "\\[(.*)\\]".r

    val tableName = tableNameAndcolNames.head
    val secondCols = tableNameAndcolNames(1)

    primaryKeyRegex.findFirstMatchIn(secondCols) match {
      case Some(m) => {
        val primaryKey = m.group(1).split(",")
        val primaryKeyCols = primaryKey.map(f => CatalogColumn(f))
        val cols = tableNameAndcolNames.drop(2).map(f => CatalogColumn(f))
        CatalogTable(tableName, cols, primaryKeyCols)
      }
      case None => {
        val cols = tableNameAndcolNames.drop(1).map(f => CatalogColumn(f))
        CatalogTable(tableName, cols)
      }
    }
  }
}

//schema for persistent relation in memory that is created by optimizer
case class CatalogView(
    identifier: TableIdentifier,
    schema: Seq[CatalogColumn],
    primaryKeys: Seq[CatalogColumn] = Seq(),
    stats: Option[Statistics] = None,
    seccoPlan: Option[SeccoPlan] = None
) extends AbstractCatalogTable

object CatalogView {

  private val counter =
    SeccoSession.currentSession.sessionState.counterManager
      .getOrCreateCounter("catalog", "view")

//  def apply(cols: Seq[CatalogColumn]) = {
//    counter.increment()
//    val viewName = s"V${counter.getValue}"
//
//    val catalog = Catalog.defaultCatalog
//    catalog.getView(viewName) match {
//      case Some(view) => view
//      case None => {
//        val view = CatalogView(TableIdentifier(viewName), cols)
//        catalog.createView(viewName, view)
//        view
//      }
//    }
//  }

  def reset() = {
    counter.reset()
  }
}

/**
  * A database defined in the catalog.
  */
case class CatalogDatabase(
    name: String,
    description: Option[String],
    tables: mutable.HashMap[String, CatalogTable],
    functions: mutable.HashMap[String, CatalogFunction]
) {
  def addTable(table: CatalogTable): Unit = {
    tables += ((table.identifier.table, table))
  }
  def dropTable(tableName: String): Unit = {
    tables.remove(tableName)
  }

  def findTable(tableName: String): Option[CatalogTable] = {
    tables.get(tableName)
  }

  def containsTable(table: CatalogTable) = {
    tables.get(table.tableName).isDefined
  }

  def addFunction(function: CatalogFunction) =
    functions += ((function.identifier.funcName, function))
  def dropFunction(funcName: String) = functions.remove(funcName)

  def findFunction(funcName: String): Option[CatalogFunction] = {
    functions.get(funcName)
  }

  def containsFunction(function: CatalogFunction) = {
    findFunction(function.identifier.funcName).isDefined
  }
}

object CatalogDatabase {
  def apply(
      name: String
  ): CatalogDatabase =
    new CatalogDatabase(name, None, mutable.HashMap(), mutable.HashMap())

  def apply(
      name: String,
      description: String
  ): CatalogDatabase =
    new CatalogDatabase(
      name,
      Some(description),
      mutable.HashMap(),
      mutable.HashMap()
    )

}

// //deprecated apply for Catalog Table Initialization
//  def apply(
//      tableName: String,
//      cols: Seq[CatalogColumn],
//      primaryCols: Seq[CatalogColumn] = Seq()
//  ): CatalogTable = {
//
//    // not allowing table to take reserved name VXX, as it is reserved for view.
//    val viewNameRegex = "V\\d*".r
//
//    tableName match {
//      case viewNameRegex(name) =>
//        throw new Exception(
//          s"Bad table name:${tableName}, table name should not be of form T1 or V1"
//        )
//      case _ =>
//    }
//
//    val catalog = Catalog.defaultCatalog
//    catalog.getTable(tableName) match {
//      case Some(table) => {
//        table
//      }
//      case None => {
//        val table = CatalogTable(TableIdentifier(tableName), cols)
//        catalog.createTable(table)
//        table
//      }
//    }
//  }
