package org.apache.spark.secco

import org.apache.spark.SparkContext
import org.apache.spark.secco.catalog.CatalogTable
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.OldInternalRow
import org.apache.spark.secco.optimization.plan.Relation
import org.apache.spark.secco.util.misc.SparkSingle
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.analysis.NoSuchTableException
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.types.{DataType, StructType}
import org.apache.spark.sql.catalyst.util.RandomIndicesGenerator

/** The entry point to programming Secco with the Dataset API.
  */
class SeccoSession(
    @transient val sparkContext: SparkContext,
    @transient val sessionState: SessionState
) { self =>

  /** Create an [[Dataframe]] from [[RDD]] of [[OldInternalRow]]
    *
    * @param rdd a rdd that stores a set of [[InternalRow]]
    * @param schema schema of this [[Dataframe]]
    * @return a new [[Dataframe]]
    */
  def createDatasetFromRDD(
      rdd: RDD[InternalRow],
      schema: StructType
  ): Dataframe = {
    Dataframe.fromRDD(rdd, schema, this)
  }

  /** Create an [[Dataframe]] from a [[Seq]] of [[InternalRow]]
    *
    * @param seq           the [[Seq]] that stores the data
    * @param attributeName attribute names of this [[Dataframe]]
    * @return a new [[Dataframe]]
    */
  def createDatasetFromSeq(
      seq: Seq[InternalRow],
      schema: StructType
  ): Dataframe = {
    Dataframe.fromSeq(seq, schema, this)
  }

  /** Create an empty [[Dataframe]]
    *
    * @param schema schema of this [[Dataframe]]
    * @return a new empty [[Dataframe]]
    */
  def createEmptyDataset(
      schema: StructType
  ): Dataframe = {
    Dataframe.empty(schema, this)
  }

  /** Returns the specified table/view as a `DataFrame`.
    *
    * @param tableName is either a qualified or unqualified name that designates a table or view.
    *                  If a database is specified, it identifies the table/view from the database.
    *                  Otherwise, it first attempts to find a temporary view with the given name
    *                  and then match the table/view from the current database.
    *                  Note that, the global temporary view database is also valid here.
    */
  def table(tableName: String): Dataframe = {
    try {
      Dataframe(
        self,
        sessionState.tempViewManager.getView(tableName).get
      )
    }
  }

  /** Executes a SQL query and return the result as a `Dataset`.
    */
  def sql(sqlText: String): Dataframe = {
    Dataframe(self, sessionState.sqlParser.parsePlan(sqlText))
  }

  /** Stop [[SeccoSession]]
    */
  def stop(): Unit = {
    sparkContext.stop()
  }

}

object SeccoSession {

  /** Constructor for default session. */
  def newDefaultSession = {
    new SeccoSession(
      SparkSingle.getSparkContext(),
      SessionState.newDefaultSessionState
    )
  }

  /** Constructor for session with given configuration. */
  def newSessionWithConf(conf: SeccoConfiguration) = {
    new SeccoSession(
      SparkSingle.getSparkContext(),
      SessionState.newSessionStateWithConf(conf)
    )
  }

  private var _currentSession: Option[SeccoSession] = None

  /** Return the current session, if current session is not set, it will return default session */
  def currentSession: SeccoSession = {
    _currentSession match {
      case Some(session) => session
      case None =>
        setCurrentSession(newDefaultSession)
        currentSession
    }
  }

  /** Set current session */
  def setCurrentSession(session: SeccoSession) = {
    _currentSession = Some(session)
  }
}
