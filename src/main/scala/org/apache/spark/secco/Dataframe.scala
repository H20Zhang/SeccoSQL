package org.apache.spark.secco

import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.secco.catalog.{
  CatalogColumn,
  CatalogTable,
  TableIdentifier
}
import org.apache.spark.secco.execution.{QueryExecution}
import org.apache.spark.secco.expression.{
  Attribute,
  Expression,
  NamedExpression
}
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.util.misc.DataLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.GraphFrame.EdgeMetaData
import org.apache.spark.secco.analysis.{
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedPattern,
  UnresolvedSubgraphQuery
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  BinaryJoin,
  Distinct,
  EdgeRelation,
  Except,
  Filter,
  Graph,
  GraphRelation,
  Inner,
  Intersection,
  JoinType,
  Limit,
  LocalRows,
  RDDBlocks,
  RDDRows,
  MessagePassing,
  NodeRelation,
  Project,
  Recursion,
  Relation,
  SubqueryAlias,
  Union
}
import org.apache.spark.secco.types.{DataType, StructType}

import scala.util.Try

/** A Dataset is a strongly typed collection of domain-specific objects that can be transformed
  * in parallel using functional or relational operations.
  */
class Dataframe(
    @transient val seccoSession: SeccoSession,
    @transient val queryExecution: QueryExecution
) {

  /* == schema related == */

  /** Return schema of the datasets. */
  def schema: Seq[Attribute] = queryExecution.analyzedPlan.output

  /** Show first k rows of datasets.
    * @param numRow the first k rows
    */
  def show(numRow: Int): Unit = {
    val seq = queryExecution.executionPlan.collectSeq().take(numRow)
    pprint.pprintln(seq)
  }

  /** Show plans used to compute the dataset. */
  def explain(): Unit = {
    println(queryExecution.toString)
  }

  /** Assign the results of the dataset to an relation in DB. */
  def createOrReplaceTable(tableName: String): Unit = {
    seccoSession.sessionState.tempViewManager
      .createOrReplaceView(tableName, logical)
  }

  /** logical plan of this dataset */
  def logical: LogicalPlan = queryExecution.logical

  /* == actions == */

  /** Cache the dataframe. */
  def cache(): Dataframe = {
    val rdd = queryExecution.executionPlan.execute().cache()
    val outputs = queryExecution.logical.output
    val schema = StructType.fromAttributes(outputs)
    val qualifier = queryExecution.logical.output.map(_.qualifier).head

    val logicPlan = qualifier match {
      case Some(prefix) => SubqueryAlias(RDDBlocks(rdd, schema), prefix)
      case None         => RDDBlocks(rdd, schema)
    }

    Dataframe(
      seccoSession,
      logicPlan
    )

  }

  /** Return rows of the dataset in RDD. */
  def rdd(): RDD[InternalRow] = queryExecution.executionPlan.rdd()

  /** Return rows of the dataset in Seq. */
  def collect(): Seq[InternalRow] = queryExecution.executionPlan.collectSeq()

  /** Return the numbers of row of the dataset. */
  def count(): Long = queryExecution.executionPlan.count()

  /* == relational algebra operations == */

  /** Perform selection on dataset. (same as select operation)
    * @param predicates the predicates used to perform the selection, e.g., R1.select("a < b")
    * @return a new dataset
    */
  def filter(predicates: String): Dataframe = {
    select(predicates)
  }

  /** Perform selection on dataset.
    * @param predicates the predicates used to perform the selection, e.g., R1.select("a < b")
    * @return a new dataset
    */
  def select(predicates: String): Dataframe = {
    Dataframe(
      seccoSession,
      Filter(
        queryExecution.logical,
        seccoSession.sessionState.sqlParser.parseExpression(predicates)
      )
    )
  }

  /** Perform a distinct projection on dataset.
    * @param projectionList the list of columns to preserve after projection, e.g., R1.project("a").
    * @return a new dataset.
    */
  def project(projectionList: String): Dataframe = {

    val namedProjectionList = projectionList
      .replaceAll(",\\s+", ",") //trim whitespace
      .split(",")
      .map(projection =>
        UnresolvedAlias(
          seccoSession.sessionState.sqlParser.parseNamedExpression(projection)
        )
      )

    Dataframe(
      seccoSession,
      Project(
        queryExecution.logical,
        namedProjectionList
      )
    )
  }

  /** Perform join between this dataset and other datasets.
    * @param others other datasets to be joined, e.g., R1.join(R2).
    * @return a new dataset.
    */
  def join(
      others: Dataframe,
      joinCondition: String = "",
      joinType: JoinType = Inner
  ): Dataframe = {
    Dataframe(
      seccoSession,
      BinaryJoin(
        queryExecution.logical,
        others.queryExecution.logical,
        joinType,
        Some(joinCondition)
          .flatMap(str => if (str == "") None else Some(str))
          .map(seccoSession.sessionState.sqlParser.parseExpression)
      )
    )
  }

  /** Perform aggregation over this dataset.
    * @param aggregateFunctions aggregate functions to be performed, e.g., R1.aggregate(count(*) by a).
    * @return a new dataset.
    */
  def aggregate(
      aggregateExpressions: Seq[String],
      groupByExpressions: Seq[String] = Seq()
  ): Dataframe = {

    val parsedAggregateExpressions = aggregateExpressions.map(aggExpr =>
      UnresolvedAlias(
        seccoSession.sessionState.sqlParser.parseExpression(aggExpr)
      )
    )

    val parsedGroupByExpressions = groupByExpressions.map { groupByAttr =>
      Try(
        seccoSession.sessionState.sqlParser
          .parseExpression(groupByAttr)
          .asInstanceOf[Attribute]
      ).getOrElse(
        throw new Exception(
          s"groupBy Attributes:${groupByAttr} cannot be parsed as an attribute"
        )
      )
    }

    Dataframe(
      seccoSession,
      Aggregate(
        queryExecution.logical,
        parsedAggregateExpressions,
        parsedGroupByExpressions
      )
    )
  }

  /** Rename the dataset.
    * @param newName the new name for the dataset
    * @return a new dataset.
    */
  def alias(newName: String): Dataframe = {
    Dataframe(
      seccoSession,
      SubqueryAlias(
        queryExecution.logical,
        newName
      )
    )
  }

  /* == set operations == */

  /** Perform union between this dataset and other datasets.
    * @param others other datasets to be unioned.
    * @return a new dataset.
    */
  def union(others: Dataframe*): Dataframe = {
    val children =
      queryExecution.logical +: others.map(_.queryExecution.logical)

    Dataframe(seccoSession, Distinct(Union(children)))
  }

  /** Perform union between this dataset and other datasets, and only retains distinct tuples
    * @param others other datasets to be unioned.
    * @return a new dataset.
    */
  def unionAll(others: Dataframe*): Dataframe = {
    val children =
      queryExecution.logical +: others.map(_.queryExecution.logical)

    Dataframe(seccoSession, Union(children))
  }

  /** Perform difference between this dataset and the other dataset.
    * @param other the other dataset to be unioned.
    * @return a new dataset.
    */
  def difference(other: Dataframe): Dataframe = {
    Dataframe(
      seccoSession,
      Except(queryExecution.logical, other.queryExecution.logical)
    )
  }

  /** Perform intersection between this dataset and the other dataset.
    * @param other the other dataset to be unioned.
    * @return a new dataset.
    */
  def intersection(other: Dataframe): Dataframe = {
    Dataframe(
      seccoSession,
      Intersection(queryExecution.logical, other.queryExecution.logical)
    )
  }

  /* == other SQL operations == */

  /** Perform distinction of this datasets by only retain distinctive tuples.
    * @return a new dataset.
    */
  def distinct(): Dataframe = {
    Dataframe(
      seccoSession,
      Distinct(queryExecution.logical)
    )
  }

  /** Return only k results */
  def limit(k: Int): Dataframe = {
    Dataframe(
      seccoSession,
      Limit(queryExecution.logical, k)
    )
  }

  /* == dataset type transformation == */

  /** Create an edge-only GraphFrame from this Dataset.
    * @param edgeMetaData the meta data for edge.
    * @return
    */
  def toGraph(edgeMetaData: EdgeMetaData = EdgeMetaData()): GraphFrame = {
    GraphFrame(this, edgeMetaData)
  }

//  def toIndex(): Dataset = ???
//  def toMatrix(): Dataset = ???

//  /** Iteratively evaluate this dataset until numRun is reached, after that it'll return a dataset with name of
//    * returnTableIdentifier.
//    * @param returnTableIdentifier the table to return.
//    * @param numRun maximum number of iterations.
//    * @return a dataset of table with name `returnTableIdentifier`.
//    */
//  def withRecursive(
//      returnTableIdentifier: String,
//      numRun: Int = seccoSession.sessionState.conf.recursionNumRun
//  ) = {
//    Dataset(
//      seccoSession,
//      RelationAlgebraWithAnalysis.iterative(
//        this.logical,
//        returnTableIdentifier,
//        numRun
//      )
//    )
//  }
//
//  /** Assign this datasets to relation with name `tableName`, if the relation is not empty, it'll be overwritten.
//    * @param tableName the table name to assign.
//    * @return a new dataset
//    */
//  def assign(tableName: String) = {
//    Dataset(
//      seccoSession,
//      RelationAlgebraWithAnalysis.assign(tableName, logical)
//    )
//  }
//
//  /** Update the table specified by tableName by key, the difference of old table and new table is output to table with
//    * deltaTableName.
//    * @param tableName the table to update
//    * @param deltaTableName the delta table to hold the difference between old table and new table
//    * @param key the key to match on table to update.
//    * @return a new dataset with content of deltaTable.
//    */
//  def update(tableName: String, deltaTableName: String, key: Seq[String]) = {
//    Dataset(
//      seccoSession,
//      RelationAlgebraWithAnalysis.update(
//        tableName,
//        deltaTableName,
//        key,
//        this.logical
//      )
//    )
//  }

}

object Dataframe {

  /** Create an instance of [[Dataframe]]
    *
    * @param seSession the [[SeccoSession]] to create the dataset
    * @param logicalPlan    the logical plan of the dataset
    * @return a new [[Dataframe]]
    */
  def apply(
      seSession: SeccoSession,
      logicalPlan: LogicalPlan
  ): Dataframe = {
    val qe = new QueryExecution(seSession, logicalPlan)
    new Dataframe(seSession, qe)
  }

  /** Create an [[Dataframe]] from [[RDD]]
    *
    * @param rdd a rdd that stores a set of [[InternalRow]]
    * @param schema schema of this [[Dataframe]]
    * @param attributeName attribute names of this [[Dataframe]]
    * @param dlSession    the [[SeccoSession]] to create the [[Dataframe]]
    * @return a new [[Dataframe]]
    */
  def fromRDD(
      rdd: RDD[InternalRow],
      schema: StructType,
      dlSession: SeccoSession = SeccoSession.currentSession
  ): Dataframe = {
    Dataframe(dlSession, RDDRows(rdd, schema))
  }

  /** Create an [[Dataframe]] from [[Seq]]
    *
    * @param seq           the [[Seq]] that stores the data
    * @param schema schema of this [[Dataframe]]
    * @param attributeName attribute names of this [[Dataframe]]
    * @param dlSession            the [[SeccoSession]] to create the dataset
    * @return a new [[Dataframe]]
    */
  def fromSeq(
      seq: Seq[InternalRow],
      schema: StructType,
      dlSession: SeccoSession = SeccoSession.currentSession
  ): Dataframe = {
    Dataframe(dlSession, LocalRows(seq, schema))
  }

  /** Create an empty [[Dataframe]]
    *
    * @param schema schema of this [[Dataframe]]
    * @param attributeName attribute names of this [[Dataframe]]
    * @param dlSession           the [[SeccoSession]] to create the dataset
    * @return a new empty [[Dataframe]]
    */
  def empty(
      schema: StructType,
      dlSession: SeccoSession = SeccoSession.currentSession
  ): Dataframe = {
    fromSeq(Seq(), schema, dlSession)
  }
}
