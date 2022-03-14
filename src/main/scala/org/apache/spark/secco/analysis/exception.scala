package org.apache.spark.secco.analysis

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.optimization.LogicalPlan

/** Thrown when a query fails to analyze, usually because the query itself is invalid.
  */
class AnalysisException(
    val message: String,
    val line: Option[Int] = None,
    val startPosition: Option[Int] = None,
    // Some plans fail to serialize due to bugs in scala collections.
    @transient val plan: Option[LogicalPlan] = None,
    val cause: Option[Throwable] = None
) extends Exception(message, cause.orNull)
    with Serializable {

  def withPosition(
      line: Option[Int],
      startPosition: Option[Int]
  ): AnalysisException = {
    val newException = new AnalysisException(message, line, startPosition)
    newException.setStackTrace(getStackTrace)
    newException
  }

  override def getMessage: String = {
    val planAnnotation = Option(plan).flatten.map(p => s";\n$p").getOrElse("")
    getSimpleMessage + planAnnotation
  }

  // Outputs an exception without the logical plan.
  // For testing only
  def getSimpleMessage: String = {
    val lineAnnotation = line.map(l => s" line $l").getOrElse("")
    val positionAnnotation = startPosition.map(p => s" pos $p").getOrElse("")
    s"$message;$lineAnnotation$positionAnnotation"
  }
}

/** Thrown by a catalog when an item already exists. The analyzer will rethrow the exception
  * as an [[AnalysisException]] with the correct position information.
  */
class DatabaseAlreadyExistsException(db: String)
    extends AnalysisException(s"Database '$db' already exists")

class TableAlreadyExistsException(db: String, table: String)
    extends AnalysisException(
      s"Table '$table' already exists in database '$db'"
    )

class ViewAlreadyExistsException(db: String, table: String)
    extends AnalysisException(
      s"View '$table' already exists in database '$db'"
    )

class GraphAlreadyExistsException(db: String, table: String)
    extends AnalysisException(
      s"Graph '$table' already exists in database '$db'"
    )

class FunctionAlreadyExistsException(db: String, func: String)
    extends AnalysisException(
      s"Function '$func' already exists in database '$db'"
    )

/** Thrown by a catalog when an item cannot be found. The analyzer will rethrow the exception
  * as an [[AnalysisException]] with the correct position information.
  */
class NoSuchDatabaseException(val db: String)
    extends AnalysisException(s"Database '$db' not found")

class NoSuchTableException(db: String, table: String)
    extends AnalysisException(
      s"Table '$table' not found in database '$db'"
    )

class NoSuchGraphException(db: String, graph: String)
    extends AnalysisException(
      s"Graph '$graph' not found in database '$db'"
    )

class NoSuchFunctionException(db: String, func: String)
    extends AnalysisException(
      s"Undefined function: '$func'. This function is neither a registered temporary function nor " +
        s"a permanent function registered in the database '$db'."
    )
