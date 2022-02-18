package org.apache.spark.secco.catalog

import org.apache.spark.secco.analysis.{
  NoSuchDatabaseException,
  NoSuchFunctionException,
  NoSuchTableException,
  TableAlreadyExistsException
}

import scala.collection.mutable

class CatalogException(
    val message: String
) extends Exception(message, null)
    with Serializable {}

/** Catalog for storing the meta data of the database (of functions, tables, views, databases)
  *
  * We assume there exists only one database per catalog.
  */
abstract class Catalog extends Serializable {

  /** create the database in the catalog
    * @param database catalog of the database
    */
  def createDatabase(database: CatalogDatabase): Unit

  /** drop the database
    * @param dbName name of the database
    */
  def dropDatabase(dbName: String): Unit

  /** alter the catalog of the database specified by `database.name`
    * @param database catalog of the database
    */
  def alterDatabase(database: CatalogDatabase): Unit

  /** get the database with name
    * @param dbName name of the database
    */
  def getDatabase(dbName: String): Option[CatalogDatabase]

  /** list all databases
    */
  def listDatabase(): Seq[CatalogDatabase]

  /** create an entry in catalog
    * @param table the catalog of the table
    */
  def createTable(table: CatalogTable, replaceExisting: Boolean = false)

  /** drop the table
    * @param tableName name of the table
    * @param dbName name of the database
    */
  def dropTable(tableName: String, dbName: Option[String] = None): Unit

  /** get the table
    * @param tableName name of the table
    * @param dbName name of the database
    * @return catalog of the table
    */
  def getTable(
      tableName: String,
      dbName: Option[String] = None
  ): Option[CatalogTable]

  /** alter the catalog of the table specified by the tableName
    * @param table catalog of the table
    */
  def alterTable(table: CatalogTable): Unit

  /** list the tables in a database
    * @param dbName name of the database
    * @return Seq of the catalog of the table
    */
  def listTable(dbName: String): Seq[CatalogTable]

  /** Create a graph in the database
    * @param graph a catalog of the graph
    */
  def createGraph(
      graph: CatalogGraphTable,
      replaceExisting: Boolean = false
  ): Unit

  /** Drop a graph in the current database
    * @param graphName name of the view
    */
  def dropGraph(graphName: String, dbName: Option[String] = None): Unit

  /** List all graphs in the current database
    */
  def listGraph(dbName: String): Seq[CatalogGraphTable]

  /** Get the graph with given name in the current database
    * @param graphName name of the graph
    * @return catalog of the graph
    */
  def getGraph(
      graphName: String,
      dbName: Option[String] = None
  ): Option[CatalogGraphTable]

  /** Alter the catalog of the graph as specified in `graph.identifier`
    * @param graph new catalog of the graph
    */
  def alterGraph(graph: CatalogGraphTable): Unit

  /** create function in the catalog
    * @param function catalog of the function
    * @param replaceExisting whether to replace existing function with name  `function.identified` in db
    */
  def createFunction(
      function: CatalogFunction,
      replaceExisting: Boolean = false
  )

  /** get the function
    * @param functionName name of the function
    * @param dbName name of the database
    * @return
    */
  def getFunction(
      functionName: String,
      dbName: Option[String] = None
  ): Option[CatalogFunction]

  /** drop the function in the current database
    * @param functionName name of the function
    * @param dbName name of the database
    */
  def dropFunction(functionName: String, dbName: Option[String] = None)

  /** rename the function in the current database to new name
    * @param oldName old name of the function
    * @param newName new name of the function
    * @param dbName name of the database
    */
  def renameFunction(
      oldName: String,
      newName: String,
      dbName: Option[String] = None
  )

  /** list function in the catalog
    * @return Seq of catalog of function
    */
  def listFunctions(dbName: Option[String] = None): Seq[CatalogFunction]

  /** reset the catalog */
  def reset(): Unit
}

object Catalog {
  val defaultDBName = "default"
  lazy val defaultCatalog: InMemoryCatalog = newDefaultCatalog

  def newDefaultCatalog: InMemoryCatalog = {
    InMemoryCatalog()
  }

  def reset(): Unit = {
    defaultCatalog.reset()
  }
}

/** An in-memory catalog for storing the meta-data temporarily
  */
class InMemoryCatalog extends Catalog {

  import org.apache.spark.secco.catalog.Catalog.defaultDBName

  private var catalogMap = mutable.HashMap[String, CatalogDatabase]()

  override def createDatabase(database: CatalogDatabase): Unit = {
    catalogMap += ((database.name, database))
  }

  override def dropDatabase(dbName: String): Unit = {
    catalogMap.remove(dbName)
  }

  override def alterDatabase(database: CatalogDatabase): Unit = {
    catalogMap.get(database.name) match {
      case Some(db) => catalogMap(database.name) = database
      case None     => throw new NoSuchDatabaseException(database.name)
    }
  }

  override def getDatabase(dbName: String): Option[CatalogDatabase] = {
    catalogMap.get(dbName)
  }

  override def listDatabase(): Seq[CatalogDatabase] = {
    catalogMap.values.toSeq
  }

  override def createTable(
      table: CatalogTable,
      replaceExisting: Boolean
  ): Unit = {
    val dbName = table.database.getOrElse(defaultDBName)

    catalogMap.get(dbName) match {
      case Some(db) =>
        db.containsTable(table) match {
          case true =>
            if (replaceExisting) {
              db.dropTable(table.tableName)
              db.addTable(table)
            } else {
              throw new TableAlreadyExistsException(
                table = table.tableName,
                db = dbName
              )
            }
          case false => db.addTable(table)
        }

      case None => throw new NoSuchDatabaseException(db = dbName)
    }
  }

  override def dropTable(
      tableName: String,
      dbName: Option[String] = None
  ): Unit = {
    val dbName_ = dbName.getOrElse(defaultDBName)
    catalogMap.get(dbName_) match {
      case Some(db) => db.dropTable(tableName)
      case None     => throw new NoSuchDatabaseException(db = dbName_)
    }
  }

  override def getTable(
      tableName: String,
      dbName: Option[String] = None
  ): Option[CatalogTable] = {
    val dbName_ = dbName.getOrElse(defaultDBName)
    catalogMap.get(dbName_) match {
      case Some(db) => db.findTable(tableName)
      case None     => throw new NoSuchDatabaseException(db = dbName_)
    }
  }

  override def alterTable(table: CatalogTable): Unit = {
    val dbName = table.database.getOrElse(defaultDBName)
    catalogMap.get(dbName) match {
      case Some(db) =>
        db.findTable(table.tableName) match {
          case Some(_) =>
            db.dropTable(table.tableName)
            db.addTable(table)
          case None =>
            throw new NoSuchTableException(db = dbName, table = table.tableName)
        }

      case None => throw new NoSuchDatabaseException(db = dbName)
    }
  }

  override def listTable(dbName: String): Seq[CatalogTable] = {
    catalogMap.get(dbName) match {
      case Some(db) =>
        db.tables.values.toSeq
          .filter(_.isInstanceOf[CatalogTable])
          .map(_.asInstanceOf[CatalogTable])
      case None => throw new NoSuchDatabaseException(db = dbName)
    }
  }

  override def createGraph(
      graph: CatalogGraphTable,
      replaceExisting: Boolean
  ): Unit = {
    val dbName = graph.database.getOrElse(defaultDBName)

    catalogMap.get(dbName) match {
      case Some(db) =>
        db.containsGraph(graph) match {
          case true =>
            if (replaceExisting) {
              db.dropGraph(graph.graphName)
              db.addGraph(graph)
            } else {
              throw new TableAlreadyExistsException(
                table = graph.graphName,
                db = dbName
              )
            }
          case false => db.addGraph(graph)
        }

      case None => throw new NoSuchDatabaseException(db = dbName)
    }
  }

  override def dropGraph(
      graphName: String,
      dbName: Option[String] = None
  ): Unit = {
    val dbName_ = dbName.getOrElse(defaultDBName)
    catalogMap.get(dbName_) match {
      case Some(db) => db.dropGraph(graphName)
      case None     => throw new NoSuchDatabaseException(db = dbName_)
    }
  }

  override def getGraph(
      graphName: String,
      dbName: Option[String] = None
  ): Option[CatalogGraphTable] = {
    val dbName_ = dbName.getOrElse(defaultDBName)
    catalogMap.get(dbName_) match {
      case Some(db) => db.findGraph(graphName)
      case None     => throw new NoSuchDatabaseException(db = dbName_)
    }
  }

  override def alterGraph(graph: CatalogGraphTable): Unit = {
    val dbName = graph.database.getOrElse(defaultDBName)
    catalogMap.get(dbName) match {
      case Some(db) =>
        db.findGraph(graph.graphName) match {
          case Some(_) =>
            db.dropGraph(graph.graphName)
            db.addGraph(graph)
          case None =>
            throw new NoSuchTableException(db = dbName, table = graph.graphName)
        }

      case None => throw new NoSuchDatabaseException(db = dbName)
    }
  }

  override def listGraph(dbName: String): Seq[CatalogGraphTable] = {
    catalogMap.get(dbName) match {
      case Some(db) =>
        db.tables.values.toSeq
          .filter(_.isInstanceOf[CatalogGraphTable])
          .map(_.asInstanceOf[CatalogGraphTable])
      case None => throw new NoSuchDatabaseException(db = dbName)
    }
  }

  override def createFunction(
      function: CatalogFunction,
      replaceIfExists: Boolean = false
  ): Unit = {
    val dbName = function.identifier.database.getOrElse(defaultDBName)
    catalogMap.get(dbName) match {
      case Some(db) =>
        db.containsFunction(function) match {
          case true =>
            if (replaceIfExists) {
              db.dropFunction(function.identifier.funcName)
              db.addFunction(function)
            } else {
              throw new NoSuchFunctionException(
                func = function.identifier.funcName,
                db = dbName
              )
            }
          case false => db.addFunction(function)
        }

      case None => throw new NoSuchDatabaseException(db = dbName)
    }
  }

  override def getFunction(
      functionName: String,
      dbName: Option[String] = None
  ): Option[CatalogFunction] = {
    val dbName_ = dbName.getOrElse(defaultDBName)
    catalogMap.get(dbName_) match {
      case Some(db) => db.findFunction(functionName)
      case None     => throw new NoSuchDatabaseException(db = dbName_)
    }
  }

  override def dropFunction(
      functionName: String,
      dbName: Option[String] = None
  ): Unit = {
    val dbName_ = dbName.getOrElse(defaultDBName)
    catalogMap.get(dbName_) match {
      case Some(db) =>
        db.findFunction(functionName) match {
          case Some(_) => db.dropFunction(functionName)
          case None =>
            throw new NoSuchFunctionException(
              func = functionName,
              db = dbName_
            )
        }
      case None => throw new NoSuchDatabaseException(db = dbName_)
    }
  }

  /** rename the function in the current database to new name
    *
    * @param oldName old name of the function
    * @param newName new name of the function
    */
  override def renameFunction(
      oldName: String,
      newName: String,
      dbName: Option[String] = None
  ): Unit = {
    val dbName_ = dbName.getOrElse(defaultDBName)
    catalogMap.get(dbName_) match {
      case Some(db) =>
        db.findFunction(oldName) match {
          case Some(oldFunc) =>
            val newFunc = oldFunc.copy(identifier =
              oldFunc.identifier.copy(funcName = newName)
            )
            db.dropFunction(oldName)
            db.addFunction(newFunc)
          case None =>
            throw new NoSuchFunctionException(
              func = oldName,
              db = dbName_
            )
        }
      case None => throw new NoSuchDatabaseException(db = dbName_)
    }
  }

  /** list function in the catalog
    *
    * @return Seq of catalog of function
    */
  override def listFunctions(
      dbName: Option[String] = None
  ): Seq[CatalogFunction] = {
    val dbName_ = dbName.getOrElse(defaultDBName)
    catalogMap.get(dbName_) match {
      case Some(db) => db.functions.values.toSeq
      case None     => throw new NoSuchDatabaseException(db = dbName_)
    }
  }

  /** reset the catalog */
  override def reset(): Unit = {
    catalogMap = mutable.HashMap[String, CatalogDatabase]()

    /** reinitialize default database */
    createDatabase(
      CatalogDatabase(
        Catalog.defaultDBName,
        Some("The default database"),
        mutable.HashMap(),
        mutable.HashMap()
      )
    )
  }
}

object InMemoryCatalog {
  def apply(): InMemoryCatalog = {
    val catalog = new InMemoryCatalog()
    catalog.createDatabase(
      CatalogDatabase(
        Catalog.defaultDBName,
        Some("The default database"),
        mutable.HashMap(),
        mutable.HashMap()
      )
    )
    catalog
  }
}
