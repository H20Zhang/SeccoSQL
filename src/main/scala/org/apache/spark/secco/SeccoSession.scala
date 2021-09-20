package org.apache.spark.secco

import org.apache.spark.SparkContext
import org.apache.spark.secco.catalog.CatalogTable
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.OldInternalRow
import org.apache.spark.secco.optimization.plan.Relation
import org.apache.spark.secco.util.misc.SparkSingle
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.RandomIndicesGenerator

/** The entry point to programming Secco with the Dataset API.
  */
class SeccoSession(
    @transient val sparkContext: SparkContext,
    @transient val sessionState: SessionState
) { self =>

  /** Create an [[Dataset]] from a [[Seq]] of [[OldInternalRow]]
    *
    * @param seq the [[Seq]] that stores the data
    * @param _relationName the name of the dataset, if None, it will be assigned an temporary name, e.g., T1
    * @param _schema the schema of the dataset, if None, it will be deduced from the tuples of the dataset, e.g., T1(1, 2, 3)
    * @param _primaryKey the primary key of the schema
    * @return a new [[Dataset]]
    */
  def createDatasetFromSeq(
      seq: Seq[OldInternalRow],
      _relationName: Option[String] = None,
      _schema: Option[Seq[String]] = None,
      _primaryKey: Option[Seq[String]] = None
  ): Dataset = {
    Dataset.fromSeq(seq, _relationName, _schema, _primaryKey, this)
  }

  /** Create an [[Dataset]] from [[RDD]] of [[OldInternalRow]]
    *
    * @param rdd the rdd that stores the data
    * @param _relationName the name of the dataset, if None, it will be assigned an temporary name, e.g., T1
    * @param _schema the schema of the dataset, if None, it will be deduced from the tuples of the dataset, e.g., T1(1, 2, 3)
    * @param _primaryKey the primary key of the schema
    * @return a new [[Dataset]]
    */
  def createDatasetFromRDD(
      rdd: RDD[OldInternalRow],
      _relationName: Option[String] = None,
      _schema: Option[Seq[String]] = None,
      _primaryKey: Option[Seq[String]] = None
  ): Dataset = {
    Dataset.fromRDD(rdd, _relationName, _schema, _primaryKey, this)
  }

  /** Create an [[Dataset]] from file
    * @param path the path that point to the location of datasets
    * @param _relationName the name of the dataset, if None, it will be assigned an temporary name, e.g., T1
    * @param _schema the schema of the dataset, if None, it will be deduced from the tuples of the dataset, e.g., T1(1, 2, 3)
    * @param _primaryKey the primary key of the schema
    * @return a new [[Dataset]]
    */
  def createDatasetFromFile(
      path: String,
      _relationName: Option[String] = None,
      _schema: Option[Seq[String]] = None,
      _primaryKey: Option[Seq[String]] = None
  ): Dataset = {
    Dataset.fromFile(path, _relationName, _schema, _primaryKey, this)
  }

  /** Create an empty [[Dataset]]
    * @param relationName the name of the dataset, if None, it will be assigned an temporary name, e.g., T1
    * @param schema the schema of the dataset, if None, it will be deduced from the tuples of the dataset, e.g., T1(1, 2, 3)
    * @param _primaryKey the primary key of the schema
    * @return a new empty [[Dataset]]
    */
  def createEmptyDataset(
      relationName: String,
      schema: Seq[String],
      _primaryKey: Option[Seq[String]] = None
  ): Dataset = {
    Dataset.empty(relationName, schema, _primaryKey, this)
  }

  /** Returns the specified table/view as a `DataFrame`.
    *
    * @param tableName is either a qualified or unqualified name that designates a table or view.
    *                  If a database is specified, it identifies the table/view from the database.
    *                  Otherwise, it first attempts to find a temporary view with the given name
    *                  and then match the table/view from the current database.
    *                  Note that, the global temporary view database is also valid here.
    */
  def table(tableName: String): Dataset = {
    Dataset(
      self,
      Relation(sessionState.sqlParser.parseTableIdentifier(tableName).table)
    )
  }

  /** Executes a SQL query and return the result as a `Dataset`.
    */
  def sql(sqlText: String): Dataset = {
    Dataset(self, sessionState.sqlParser.parsePlan(sqlText))
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
