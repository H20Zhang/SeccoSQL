package org.apache.spark.secco

import org.apache.spark.secco.execution.QueryExecution
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.SeccoGraphFrame.EdgeMetaData
import org.apache.spark.secco.analysis.UnresolvedAlias
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  BinaryJoin,
  Distinct,
  Except,
  Filter,
  Inner,
  Intersection,
  JoinType,
  Limit,
  LocalRows,
  PartitionedRDDRows,
  Project,
  RDDRows,
  SubqueryAlias,
  Union
}
import org.apache.spark.secco.types.{
  BooleanType,
  DataType,
  DataTypes,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{sql => sparksql}

import scala.util.Try

/** A Dataset is a strongly typed collection of domain-specific objects that can be transformed
  * in parallel using functional or relational operations.
  */
class SeccoDataFrame(
    @transient val seccoSession: SeccoSession,
    @transient val queryExecution: QueryExecution
) {

  /* == schema related == */

  /** Return schema of the datasets. */
  def schema: Seq[Attribute] = queryExecution.analyzedPlan.output

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
  def cache(): SeccoDataFrame = {
    val rdd = queryExecution.executionPlan.execute().cache()
    val outputs = queryExecution.logical.output
    val schema = StructType.fromAttributes(outputs)
    val qualifier = queryExecution.logical.output.map(_.qualifier).head

    val logicPlan = qualifier match {
      case Some(prefix) =>
        SubqueryAlias(PartitionedRDDRows(rdd, schema), prefix)
      case None => PartitionedRDDRows(rdd, schema)
    }

    SeccoDataFrame(
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

  /** Show first k rows of datasets.
    * @param numRow the first k rows
    */
  def show(numRow: Int): Unit = {
    val seq = collect().take(numRow)
    pprint.pprintln(seq)
  }

  /* == relational algebra operations == */

  /** Perform selection on dataset. (same as select operation)
    * @param predicates the predicates used to perform the selection, e.g., R1.select("a < b")
    * @return a new dataset
    */
  def filter(predicates: String): SeccoDataFrame = {
    select(predicates)
  }

  /** Perform selection on dataset.
    * @param predicates the predicates used to perform the selection, e.g., R1.select("a < b")
    * @return a new dataset
    */
  def select(predicates: String): SeccoDataFrame = {
    SeccoDataFrame(
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
  def project(projectionList: String): SeccoDataFrame = {

    val namedProjectionList = projectionList
      .replaceAll(",\\s+", ",") //trim whitespace
      .split(",")
      .map(projection =>
        UnresolvedAlias(
          seccoSession.sessionState.sqlParser.parseNamedExpression(projection)
        )
      )

    SeccoDataFrame(
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
      others: SeccoDataFrame,
      joinCondition: String = "",
      joinType: JoinType = Inner
  ): SeccoDataFrame = {
    SeccoDataFrame(
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
  ): SeccoDataFrame = {

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

    SeccoDataFrame(
      seccoSession,
      Aggregate(
        queryExecution.logical,
        parsedAggregateExpressions,
        parsedGroupByExpressions
      )
    )
  }

  /** Rename the dataset and materialize its results.
    * @param newName the new name for the dataset
    * @return a new dataset.
    */
  def subqueryAlias(newName: String): SeccoDataFrame = {
    SeccoDataFrame(
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
  def union(others: SeccoDataFrame*): SeccoDataFrame = {
    val children =
      queryExecution.logical +: others.map(_.queryExecution.logical)

    SeccoDataFrame(seccoSession, Distinct(Union(children)))
  }

  /** Perform union between this dataset and other datasets, and only retains distinct tuples
    * @param others other datasets to be unioned.
    * @return a new dataset.
    */
  def unionAll(others: SeccoDataFrame*): SeccoDataFrame = {
    val children =
      queryExecution.logical +: others.map(_.queryExecution.logical)

    SeccoDataFrame(seccoSession, Union(children))
  }

  /** Perform difference between this dataset and the other dataset.
    * @param other the other dataset to be unioned.
    * @return a new dataset.
    */
  def difference(other: SeccoDataFrame): SeccoDataFrame = {
    SeccoDataFrame(
      seccoSession,
      Except(queryExecution.logical, other.queryExecution.logical)
    )
  }

  /** Perform intersection between this dataset and the other dataset.
    * @param other the other dataset to be unioned.
    * @return a new dataset.
    */
  def intersection(other: SeccoDataFrame): SeccoDataFrame = {
    SeccoDataFrame(
      seccoSession,
      Intersection(queryExecution.logical, other.queryExecution.logical)
    )
  }

  /* == other SQL operations == */

  /** Perform distinction of this datasets by only retain distinctive tuples.
    * @return a new dataset.
    */
  def distinct(): SeccoDataFrame = {
    SeccoDataFrame(
      seccoSession,
      Distinct(queryExecution.logical)
    )
  }

  /** Return only k results */
  def limit(k: Int): SeccoDataFrame = {
    SeccoDataFrame(
      seccoSession,
      Limit(queryExecution.logical, k)
    )
  }

  /* == dataset type transformation == */

  /** Create an edge-only GraphFrame from this Dataset.
    * @param edgeMetaData the meta data for edge.
    * @return
    */
  def toGraph(edgeMetaData: EdgeMetaData = EdgeMetaData()): SeccoGraphFrame = {
    SeccoGraphFrame(this, edgeMetaData)
  }

  /** Create an SparkSQL's DataFrame from this SeccoDataFrame. */
  def toSparkSQLDataFrame(): sparksql.DataFrame = {
    val spark = SparkSession.getActiveSession.get

    // convert to SparkSQL's row
    val sparkSQLRDD =
      rdd().map(row => sparksql.Row(row.toSeq(schema.map(_.dataType))))

    // convert to SparkSQL's schema
    val sparkSQLSchema = sparksql.types.StructType(schema.map { attribute =>
      attribute.dataType match {
        case BooleanType =>
          sparksql.types.StructField(attribute.name, sparksql.types.BooleanType)
        case IntegerType =>
          sparksql.types.StructField(attribute.name, sparksql.types.IntegerType)
        case LongType =>
          sparksql.types.StructField(attribute.name, sparksql.types.LongType)
        case FloatType =>
          sparksql.types.StructField(attribute.name, sparksql.types.FloatType)
        case DoubleType =>
          sparksql.types.StructField(attribute.name, sparksql.types.DoubleType)
        case StringType =>
          sparksql.types.StructField(attribute.name, sparksql.types.StringType)
      }
    })

    spark.createDataFrame(sparkSQLRDD, sparkSQLSchema)
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

object SeccoDataFrame {

  /** Create an instance of [[SeccoDataFrame]]
    *
    * @param seccoSession the [[SeccoSession]] to create the dataset
    * @param logicalPlan    the logical plan of the dataset
    * @return a new [[SeccoDataFrame]]
    */
  def apply(
      seccoSession: SeccoSession,
      logicalPlan: LogicalPlan
  ): SeccoDataFrame = {
    val qe = new QueryExecution(seccoSession, logicalPlan)
    new SeccoDataFrame(seccoSession, qe)
  }

  /** Create an instance of [[SeccoDataFrame]] from [[sparksql.DataFrame]]
    * @param df SparkSQL's dataframe
    * @param seccoSession session of Secco
    * @return an instance of [[SeccoDataFrame]]
    */
  def fromSparkSQL(
      df: sparksql.DataFrame,
      seccoSession: SeccoSession = SeccoSession.currentSession,
      primaryKeyNames: Seq[String] = Seq()
  ): SeccoDataFrame = {
    val sparkSQLSchema = df.schema

    // convert from SparkSQL's schema to SeccoSQL's schema
    val schema = StructType(sparkSQLSchema.map { field =>
      field.dataType match {
        case sparksql.types.BooleanType =>
          StructField(field.name, DataTypes.BooleanType, field.nullable)
        case sparksql.types.IntegerType =>
          StructField(field.name, DataTypes.IntegerType, field.nullable)
        case sparksql.types.LongType =>
          StructField(field.name, DataTypes.LongType, field.nullable)
        case sparksql.types.FloatType =>
          StructField(field.name, DataTypes.FloatType, field.nullable)
        case sparksql.types.DoubleType =>
          StructField(field.name, DataTypes.DoubleType, field.nullable)
        case sparksql.types.StringType =>
          StructField(field.name, DataTypes.StringType, field.nullable)
        case _ =>
          throw new Exception(s"not supported DataType:${field.dataType}")
      }
    })

    // convert from SparkSQL's row to SeccoSQL's row
    val rdd = df.rdd.map { row =>
      InternalRow(row.toSeq)
    }

    fromRDD(rdd, schema, seccoSession, primaryKeyNames)
  }

  /** Create an instance of [[SeccoDataFrame]] from [[RDD]]
    *
    * @param rdd a rdd that stores a set of [[InternalRow]]
    * @param schema schema of this [[SeccoDataFrame]]
    * @param attributeName attribute names of this [[SeccoDataFrame]]
    * @param seccoSession    the [[SeccoSession]] to create the [[SeccoDataFrame]]
    * @return an instance of [[SeccoDataFrame]]
    */
  def fromRDD(
      rdd: RDD[InternalRow],
      schema: StructType,
      seccoSession: SeccoSession = SeccoSession.currentSession,
      primaryKeyNames: Seq[String] = Seq()
  ): SeccoDataFrame = {
    SeccoDataFrame(seccoSession, RDDRows(rdd, schema, primaryKeyNames))
  }

  /** Create an instance of [[SeccoDataFrame]] from [[Seq]]
    *
    * @param seq           the [[Seq]] that stores the data
    * @param schema schema of this [[SeccoDataFrame]]
    * @param attributeName attribute names of this [[SeccoDataFrame]]
    * @param seccoSession            the [[SeccoSession]] to create the dataset
    * @return an instance of [[SeccoDataFrame]]
    */
  def fromSeq(
      seq: Seq[InternalRow],
      schema: StructType,
      seccoSession: SeccoSession = SeccoSession.currentSession,
      primaryKeyNames: Seq[String] = Seq()
  ): SeccoDataFrame = {
    SeccoDataFrame(seccoSession, LocalRows(seq, schema, primaryKeyNames))
  }

  /** Create an instance of empty [[SeccoDataFrame]]
    *
    * @param schema schema of this [[SeccoDataFrame]]
    * @param attributeName attribute names of this [[SeccoDataFrame]]
    * @param seccoSession           the [[SeccoSession]] to create the dataset
    * @return an instance of empty [[SeccoDataFrame]]
    */
  def empty(
      schema: StructType,
      seccoSession: SeccoSession = SeccoSession.currentSession,
      primaryKeyNames: Seq[String] = Seq()
  ): SeccoDataFrame = {
    fromSeq(Seq(), schema, seccoSession, primaryKeyNames)
  }
}
