package org.apache.spark.secco.catalog

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.execution.{InternalBlock, SeccoPlan}
import org.apache.spark.secco.optimization.statsEstimation.{
  ColumnStat,
  Statistics
}
import org.apache.spark.secco.types.{DataType, DoubleType}
import org.apache.spark.secco.util.counter.Counter
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.analysis.{
  GraphAlreadyExistsException,
  NoSuchTableException,
  TableAlreadyExistsException
}

import scala.collection.mutable
import scala.util.Try

/** A function defined in the catalog.
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

/** A column of table in the catalog
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

/** The abstract class for all catalog classes that represent a table in the catalog */
abstract class AbstractCatalogTable extends Serializable {

  /** Identifier of the table (e.g., (dbName, tableName)). */
  def identifier: TableIdentifier

  /** Schema of the table (e.g., (a, b, c, d)). */
  def schema: Seq[CatalogColumn]

  /** Primary keys of the table. */
  def primaryKeys: Seq[CatalogColumn]

  /** Statistic for the table. */
  def stats: Option[Statistics]

  /** Simple Name of the  table without database name. */
  lazy val tableName = identifier.table

  /** Database of the table. */
  lazy val database = identifier.database

  /** Full quantified name of the table. */
  lazy val quantifiedName = identifier.toString

  /** Number of attributes in the schema. */
  lazy val arity = schema.size

  /** All attribute names of the columns. */
  lazy val attributeNames = schema.map(_.columnName)

  /** Find if a given column is in schema of the table. */
  def containColumn(column: CatalogColumn): Boolean = {
    schema.contains(column)
  }

  private lazy val colsToPosMap = schema.zipWithIndex.toMap

  /** Find the position of the given column in schema of the table. */
  def posForColumn(column: CatalogColumn) = {
    colsToPosMap(column)
  }

  override def toString: String = {
    s"$quantifiedName(${attributeNames.mkString("(", ", ", ")")})"
  }
}

/** The catalog class for representing a table in the catalog */
case class CatalogTable(
    identifier: TableIdentifier,
    schema: Seq[CatalogColumn],
    primaryKeys: Seq[CatalogColumn],
    stats: Option[Statistics]
) extends AbstractCatalogTable {

  /** Link data (i.e., RDD[InternalBlock]) with the catalog table. */
  def attachData(content: RDD[InternalBlock]) = {
    val dataManager =
      SeccoSession.currentSession.sessionState.cachedDataManager
    dataManager.storeRelation(identifier.toString, content)
    this
  }

  /** Link data (i.e., url location for storing the data) with the catalog table. */
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

//  def apply(tableNameAndcolNames: String*): CatalogTable = {
//
//    assert(
//      tableNameAndcolNames.size >= 2,
//      s"there should be at least one attributes in $tableNameAndcolNames"
//    )
//
//    val primaryKeyRegex = "\\[(.*)\\]".r
//
//    val tableName = tableNameAndcolNames.head
//    val secondCols = tableNameAndcolNames(1)
//
//    primaryKeyRegex.findFirstMatchIn(secondCols) match {
//      case Some(m) => {
//        val primaryKey = m.group(1).split(",")
//        val primaryKeyCols = primaryKey.map(f => CatalogColumn(f))
//        val cols = tableNameAndcolNames.drop(2).map(f => CatalogColumn(f))
//        CatalogTable(tableName, cols, primaryKeyCols)
//      }
//      case None => {
//        val cols = tableNameAndcolNames.drop(1).map(f => CatalogColumn(f))
//        CatalogTable(tableName, cols)
//      }
//    }
//  }
}

/** The catalog class for representing a node table in the catalog */
case class CatalogNodeTable(
    identifier: TableIdentifier,
    nodeIdColumn: CatalogColumn,
    nodeLabelColumn: Option[CatalogColumn],
    nodePropertyColumns: Seq[CatalogColumn],
    stats: Option[Statistics]
) extends AbstractCatalogTable {

  override def schema: Seq[CatalogColumn] =
    Seq(nodeIdColumn) ++ nodeLabelColumn.toSeq ++ nodePropertyColumns

  override def primaryKeys: Seq[CatalogColumn] = Seq()
}

object CatalogNodeTable {

  def apply(
      name: String
  ): CatalogNodeTable = new CatalogNodeTable(
    TableIdentifier(name),
    CatalogColumn("id"),
    None,
    Seq(),
    None
  )

  def apply(
      name: String,
      labelColumn: Option[CatalogColumn]
  ): CatalogNodeTable = new CatalogNodeTable(
    TableIdentifier(name),
    CatalogColumn("id"),
    labelColumn,
    Seq(),
    None
  )

  def apply(
      name: String,
      labelColumn: Option[CatalogColumn],
      propertyColumns: Seq[CatalogColumn]
  ): CatalogNodeTable = new CatalogNodeTable(
    TableIdentifier(name),
    CatalogColumn("id"),
    labelColumn,
    propertyColumns,
    None
  )
}

/** A edge relation defined in the catalog. */
case class CatalogEdgeTable(
    identifier: TableIdentifier,
    srcIdColumn: CatalogColumn,
    dstIdColumn: CatalogColumn,
    edgeLabelColumn: Option[CatalogColumn],
    edgePropertyColumns: Seq[CatalogColumn],
    stats: Option[Statistics]
) extends AbstractCatalogTable {

  override def schema: Seq[CatalogColumn] = Seq(
    srcIdColumn,
    dstIdColumn
  ) ++ edgeLabelColumn.toSeq ++ edgePropertyColumns

  override def primaryKeys: Seq[CatalogColumn] = Seq()
}

object CatalogEdgeTable {

  def apply(
      name: String
  ): CatalogEdgeTable = new CatalogEdgeTable(
    TableIdentifier(name),
    CatalogColumn("src"),
    CatalogColumn("dst"),
    None,
    Seq(),
    None
  )

  def apply(
      name: String,
      labelColumn: Option[CatalogColumn]
  ): CatalogEdgeTable = new CatalogEdgeTable(
    TableIdentifier(name),
    CatalogColumn("src"),
    CatalogColumn("dst"),
    labelColumn,
    Seq(),
    None
  )

  def apply(
      name: String,
      labelColumn: Option[CatalogColumn],
      propertyColumns: Seq[CatalogColumn]
  ): CatalogEdgeTable = new CatalogEdgeTable(
    TableIdentifier(name),
    CatalogColumn("src"),
    CatalogColumn("dst"),
    labelColumn,
    propertyColumns,
    None
  )

}

/** A graph relation defined in the catalog. */
case class CatalogGraphTable(
    identifier: TableIdentifier,
    nodeTable: CatalogNodeTable,
    edgeTable: CatalogEdgeTable,
    stats: Option[Statistics]
) extends AbstractCatalogTable {

  /** Simple name of the graph without database name. */
  lazy val graphName: String = tableName

  /** Schema of the node table. */
  def nodeSchema: Seq[CatalogColumn] = nodeTable.schema

  /** Schema of the edge table. */
  def edgeSchema: Seq[CatalogColumn] = edgeTable.schema

  override def schema: Seq[CatalogColumn] = {

    val srcNodeLabelColumn = nodeTable.nodeLabelColumn.map(col =>
      col.copy(columnName = s"src_${col.columnName}")
    )

    val dstNodeLabelColumn = nodeTable.nodeLabelColumn.map(col =>
      col.copy(columnName = s"dst_${col.columnName}")
    )

    val srcNodePropertyColumn = nodeTable.nodePropertyColumns.map(col =>
      col.copy(columnName = s"src_${col.columnName}")
    )

    val dstNodePropertyColumn = nodeTable.nodePropertyColumns.map(col =>
      col.copy(columnName = s"dst_${col.columnName}")
    )

    Seq(
      edgeTable.srcIdColumn,
      edgeTable.dstIdColumn
    ) ++ edgeTable.edgeLabelColumn.toSeq ++
      srcNodeLabelColumn.toSeq ++
      dstNodeLabelColumn.toSeq ++
      srcNodePropertyColumn ++
      dstNodePropertyColumn

  }

  /** Primary keys of the table. */
  override def primaryKeys: Seq[CatalogColumn] = Seq()

  override def toString: String =
    s"${identifier.table}(E:${edgeTable}, V:${nodeTable})"

}

/** A database defined in the catalog.
  */
case class CatalogDatabase(
    name: String,
    description: Option[String],
    tables: mutable.HashMap[String, AbstractCatalogTable],
    functions: mutable.HashMap[String, CatalogFunction]
) {

  /** Add a new table to the database. */
  def addTable(table: CatalogTable): Unit = {
    if (findTable(table.tableName).isEmpty) {
      tables += ((table.tableName, table))
    } else {
      throw new TableAlreadyExistsException(this.name, table.tableName)
    }
  }

  /** Drop a new table out of the database. */
  def dropTable(tableName: String): Unit = {
    if (findTable(tableName).isDefined) {
      tables.remove(tableName)
    }
  }

  /** Find if the table with `tableName` exists in the database, if so return [[Option[CatalogTable]]] */
  def findTable(tableName: String): Option[CatalogTable] = {
    Try {
      tables.get(tableName).map(_.asInstanceOf[CatalogTable])
    }.getOrElse(
      throw new NoSuchTableException(name, tableName)
    )
  }

  /** Find if the table exists in the database. */
  def containsTable(table: CatalogTable) = {
    findTable(table.tableName).isDefined
  }

  /** Add a new graph to the database. */
  def addGraph(graph: CatalogGraphTable): Unit = {
    if (findGraph(graph.graphName).isEmpty) {
      tables += ((graph.graphName, graph))
    } else {
      throw new GraphAlreadyExistsException(this.name, graph.graphName)
    }

  }

  /** Drop the graph with `graphName` in the current database. */
  def dropGraph(graphName: String): Unit = {
    if (findGraph(graphName).isDefined) {
      tables.remove(graphName)
    }
  }

  /** Find the graph with `graphName` in the current database, if so, return [[Option[CatalogGraphTable]]] */
  def findGraph(graphName: String): Option[CatalogGraphTable] = {
    Try {
      tables.get(graphName).map(_.asInstanceOf[CatalogGraphTable])
    }.getOrElse(
      throw new NoSuchTableException(name, graphName)
    )
  }

  /** Find if the graph with `graphName` exists in the current database */
  def containsGraph(graph: CatalogGraphTable) = {
    findGraph(graph.graphName).isDefined
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

object CatalogGraphTable {

  def apply(
      name: String
  ): CatalogGraphTable = new CatalogGraphTable(
    TableIdentifier(name),
    CatalogNodeTable(s"${name}_V"),
    CatalogEdgeTable(s"${name}_E"),
    None
  )

  def apply(
      name: String,
      nodeLabel: Option[CatalogColumn],
      edgeLabel: Option[CatalogColumn]
  ): CatalogGraphTable = new CatalogGraphTable(
    TableIdentifier(name),
    CatalogNodeTable(s"${name}_V", nodeLabel),
    CatalogEdgeTable(s"${name}_E", edgeLabel),
    None
  )

  def apply(
      name: String,
      nodeLabel: Option[CatalogColumn],
      edgeLabel: Option[CatalogColumn],
      nodePropertyColumns: Seq[CatalogColumn],
      edgePropertyColumns: Seq[CatalogColumn]
  ): CatalogGraphTable = new CatalogGraphTable(
    TableIdentifier(name),
    CatalogNodeTable(s"${name}_V", nodeLabel, nodePropertyColumns),
    CatalogEdgeTable(s"${name}_E", edgeLabel, edgePropertyColumns),
    None
  )

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

////schema for persistent relation in memory that is created by optimizer
//case class CatalogView(
//                        identifier: TableIdentifier,
//                        schema: Seq[CatalogColumn],
//                        primaryKeys: Seq[CatalogColumn] = Seq(),
//                        stats: Option[Statistics] = None,
//                        seccoPlan: Option[SeccoPlan] = None
//                      ) extends AbstractCatalogTable
//
//object CatalogView {
//
//  private val counter =
//    SeccoSession.currentSession.sessionState.counterManager
//      .getOrCreateCounter("catalog", "view")
//
//  //  def apply(cols: Seq[CatalogColumn]) = {
//  //    counter.increment()
//  //    val viewName = s"V${counter.getValue}"
//  //
//  //    val catalog = Catalog.defaultCatalog
//  //    catalog.getView(viewName) match {
//  //      case Some(view) => view
//  //      case None => {
//  //        val view = CatalogView(TableIdentifier(viewName), cols)
//  //        catalog.createView(viewName, view)
//  //        view
//  //      }
//  //    }
//  //  }
//
//  def reset() = {
//    counter.reset()
//  }
//}
