package org.apache.spark.secco

import java.util.concurrent.atomic.AtomicLong
import org.apache.spark.secco.catalog.{
  CatalogColumn,
  CatalogTable,
  TableIdentifier
}
import org.apache.spark.secco.execution.{
  InternalBlock,
  OldInternalRow,
  QueryExecution,
  RowBlock,
  RowBlockContent
}
import org.apache.spark.secco.expression.{
  Attribute,
  Expression,
  NamedExpression
}
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.util.misc.DataLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.analysis.UnresolvedAlias
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  BinaryJoin,
  Distinct,
  Except,
  Filter,
  Inner,
  Intersection,
  JoinType,
  Project,
  Relation,
  SubqueryAlias,
  Union
}

import scala.util.Try

/** A Dataset is a strongly typed collection of domain-specific objects that can be transformed
  * in parallel using functional or relational operations.
  */
class Dataset(
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
  def createOrReplaceTable(tableName: String): Unit = ???

  /** logical plan of this dataset */
  def logical: LogicalPlan = queryExecution.logical

  /* == actions == */

  /** Return rows of the dataset in RDD. */
  def rdd(): RDD[OldInternalRow] = queryExecution.executionPlan.rdd()

  /** Return rows of the dataset in Seq. */
  def collect(): Seq[OldInternalRow] = queryExecution.executionPlan.collectSeq()

  /** Return the numbers of row of the dataset. */
  def count(): Long = queryExecution.executionPlan.count()

  /* == relational algebra transformation == */

  /** Perform selection on dataset. (same as select operation)
    * @param predicates the predicates used to perform the selection, e.g., R1.select("a < b")
    * @return a new dataset
    */
  def filter(predicates: String): Dataset = {
    select(predicates)
  }

  /** Perform selection on dataset.
    * @param predicates the predicates used to perform the selection, e.g., R1.select("a < b")
    * @return a new dataset
    */
  def select(predicates: String): Dataset = {
    Dataset(
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
  def project(projectionList: Seq[String]): Dataset = {

    val namedProjectionList = projectionList.map(projection =>
      UnresolvedAlias(
        seccoSession.sessionState.sqlParser.parseExpression(projection)
      )
    )

    Dataset(
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
      others: Dataset,
      joinCondition: Option[Expression] = None,
      joinType: JoinType = Inner
  ): Dataset = {
    Dataset(
      seccoSession,
      BinaryJoin(
        queryExecution.logical,
        others.queryExecution.logical,
        joinType,
        joinCondition
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
  ): Dataset = {

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

    Dataset(
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
  def alias(newName: String): Dataset = {
    Dataset(
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
  def union(others: Dataset*): Dataset = {
    val children =
      queryExecution.logical +: others.map(_.queryExecution.logical)

    Dataset(seccoSession, Union(children))
  }

  /** Perform union between this dataset and other datasets, and only retains distinct tuples
    * @param others other datasets to be unioned.
    * @return a new dataset.
    */
  def unionAll(others: Dataset*): Dataset = {
    val children =
      queryExecution.logical +: others.map(_.queryExecution.logical)

    Dataset(seccoSession, Distinct(Union(children)))
  }

  /** Perform difference between this dataset and the other dataset.
    * @param other the other dataset to be unioned.
    * @return a new dataset.
    */
  def difference(other: Dataset): Dataset = {
    Dataset(
      seccoSession,
      Except(queryExecution.logical, other.queryExecution.logical)
    )
  }

  /** Perform intersection between this dataset and the other dataset.
    * @param other the other dataset to be unioned.
    * @return a new dataset.
    */
  def intersection(other: Dataset): Dataset = {
    Dataset(
      seccoSession,
      Intersection(queryExecution.logical, other.queryExecution.logical)
    )
  }

  /** Perform distinction of this datasets by only retain distinctive tuples.
    * @return a new dataset.
    */
  def distinct(): Dataset = {
    Dataset(
      seccoSession,
      Distinct(queryExecution.logical)
    )
  }

  /* == iterative operations == */

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

object Dataset {

  private val tempTableId = new AtomicLong()
  private val tempTableNamePrefix = "T"

  /** Create an instance of [[Dataset]]
    *
    * @param seSession the [[SeccoSession]] to create the dataset
    * @param logicalPlan    the logical plan of the dataset
    * @return a new [[Dataset]]
    */
  def apply(
      seSession: SeccoSession,
      logicalPlan: LogicalPlan
  ): Dataset = {
    val qe = new QueryExecution(seSession, logicalPlan)
    new Dataset(seSession, qe)
  }

  /** Create an [[Dataset]] from [[RDD]]
    *
    * @param rdd          the rdd that stores the data
    * @param catalogTable the catalog of the dataset
    * @param dlSession    the [[SeccoSession]] to create the dataset
    * @return a new [[Dataset]]
    */
  def fromRDD(
      rdd: RDD[OldInternalRow],
      catalogTable: CatalogTable,
      dlSession: SeccoSession
  ): Dataset = {

    val catalog = dlSession.sessionState.catalog
    val cachedDataManager = dlSession.sessionState.cachedDataManager
    val relationName = catalogTable.identifier.table
    val schema = catalogTable.schema.map(_.columnName)

    // register catalogTable in Catalog
    catalog.createTable(catalogTable)

    // store rdd in cachedDataManager
    val internalRDD = rdd
      .mapPartitions { it =>
        val blockContent = RowBlockContent(it.toArray)
        val rowBlock =
          RowBlock(schema, blockContent).asInstanceOf[InternalBlock]
        Iterator(rowBlock)
      }
      .persist(dlSession.sessionState.conf.rddCacheLevel)

    cachedDataManager.storeRelation(relationName, internalRDD)

    Dataset(dlSession, Relation(TableIdentifier(relationName)))
  }

  /** Create an [[Dataset]] from [[RDD]]
    *
    * @param rdd           the rdd that stores the data
    * @param _relationName the name of the dataset, if None, it will be assigned an temporary name, e.g., T1
    * @param _schema       the schema of the dataset, if None, it will be deduced from the tuples of the dataset, e.g., T1(1, 2, 3)
    * @param _primaryKey   the primary key of the schema
    * @param dl            the [[SeccoSession]] to create the dataset
    * @return a new [[Dataset]]
    */
  def fromRDD(
      rdd: RDD[OldInternalRow],
      _relationName: Option[String] = None,
      _schema: Option[Seq[String]] = None,
      _primaryKey: Option[Seq[String]] = None,
      dl: SeccoSession = SeccoSession.currentSession
  ): Dataset = {

    val relationName = _relationName match {
      case Some(relationName) => relationName
      case None               => s"${tempTableNamePrefix}${tempTableId.incrementAndGet()}"
    }

    val schema = _schema match {
      case Some(schema) => schema
      case None =>
        assert(
          rdd.count() != 0,
          "size of rdd must be greater than 0 if no schema is provided."
        )

        val arity = rdd.take(1)(0).size

        assert(
          rdd.map(f => f.size == arity).reduce { case (l, r) =>
            l != false && r != false
          },
          s"internal row must be have same width:${arity}"
        )

        val schema = 0 until (arity) map (f => s"${f}")

        schema
    }

    val catalogTable = CatalogTable(
      relationName,
      schema.map(f => CatalogColumn(f)),
      _primaryKey.map(_.map(f => CatalogColumn(f))).getOrElse(Seq())
    )

    fromRDD(
      rdd,
      catalogTable,
      dl
    )
  }

  /** Create an [[Dataset]] from [[Seq]]
    *
    * @param seq           the [[Seq]] that stores the data
    * @param _relationName the name of the dataset, if None, it will be assigned an temporary name, e.g., T1
    * @param _schema       the schema of the dataset, if None, it will be deduced from the tuples of the dataset, e.g., T1(1, 2, 3)
    * @param _primaryKey   the primary key of the schema
    * @param dl            the [[SeccoSession]] to create the dataset
    * @return a new [[Dataset]]
    */
  def fromSeq(
      seq: Seq[OldInternalRow],
      _relationName: Option[String] = None,
      _schema: Option[Seq[String]] = None,
      _primaryKey: Option[Seq[String]] = None,
      dl: SeccoSession = SeccoSession.currentSession
  ): Dataset = {
    val sc = dl.sessionState.sc
    val rdd = sc.parallelize(seq)
    fromRDD(rdd, _relationName, _schema, _primaryKey, dl)
  }

  /** Create an [[Dataset]] from file
    *
    * @param path          the path that point to the location of datasets
    * @param _relationName the name of the dataset, if None, it will be assigned an temporary name, e.g., T1
    * @param _schema       the schema of the dataset, if None, it will be deduced from the tuples of the dataset, e.g., T1(1, 2, 3)
    * @param _primaryKey   the primary key of the schema
    * @param dl            the [[SeccoSession]] to create the dataset
    * @return a new [[Dataset]]
    */
  def fromFile(
      path: String,
      _relationName: Option[String] = None,
      _schema: Option[Seq[String]] = None,
      _primaryKey: Option[Seq[String]] = None,
      dl: SeccoSession = SeccoSession.currentSession
  ): Dataset = {
    val rdd = DataLoader.loadTSV(path)
    fromRDD(rdd, _relationName, _schema, _primaryKey, dl)
  }

  /** Create an empty [[Dataset]]
    *
    * @param relationName the name of the dataset, if None, it will be assigned an temporary name, e.g., T1
    * @param schema       the schema of the dataset, if None, it will be deduced from the tuples of the dataset, e.g., T1(1, 2, 3)
    * @param _primaryKey  the primary key of the schema
    * @param dl           the [[SeccoSession]] to create the dataset
    * @return a new empty [[Dataset]]
    */
  def empty(
      relationName: String,
      schema: Seq[String],
      _primaryKey: Option[Seq[String]] = None,
      dl: SeccoSession = SeccoSession.currentSession
  ): Dataset = {
    fromSeq(Seq(), Some(relationName), Some(schema), _primaryKey, dl)
  }

}
