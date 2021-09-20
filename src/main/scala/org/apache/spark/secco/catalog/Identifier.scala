package org.apache.spark.secco.catalog

trait Identifier {}

/**
  * An identifier that optionally specifies a database.
  *
  * Format (unquoted): "name" or "db.name"
  * Format (quoted): "`name`" or "`db`.`name`"
  */
trait IdentifierWithDatabase {
  val identifier: String

  def database: Option[String]

  /*
   * Escapes back-ticks within the identifier name with double-back-ticks.
   */
  private def quoteIdentifier(name: String): String = name.replace("`", "``")

  def quotedString: String = {
    val replacedId = quoteIdentifier(identifier)
    val replacedDb = database.map(quoteIdentifier(_))

    if (replacedDb.isDefined) s"`${replacedDb.get}`.`$replacedId`"
    else s"`$replacedId`"
  }

  def unquotedString: String = {
    if (database.isDefined) s"${database.get}.$identifier" else identifier
  }

  override def toString: String = unquotedString
}

/**
  * Identifies a table in a database.
  * If `database` is not defined, the current database is used.
  * When we register a permanent function in the FunctionRegistry, we use
  * unquotedString as the function name.
  */
case class TableIdentifier(table: String, database: Option[String])
    extends IdentifierWithDatabase {

  override val identifier: String = table

  def this(table: String) = this(table, None)
}

/** A fully qualified identifier for a table (i.e., database.tableName) */
case class QualifiedTableName(database: String, name: String) {
  override def toString: String = s"$database.$name"
}

object TableIdentifier {
  def apply(tableName: String): TableIdentifier = new TableIdentifier(tableName)
}

/**
  * Identifies a function in a database.
  * If `database` is not defined, the current database is used.
  */
case class FunctionIdentifier(funcName: String, database: Option[String])
    extends IdentifierWithDatabase {

  override val identifier: String = funcName

  def this(funcName: String) = this(funcName, None)

  override def toString: String = unquotedString
}

object FunctionIdentifier {
  def apply(funcName: String): FunctionIdentifier =
    new FunctionIdentifier(funcName)
}
