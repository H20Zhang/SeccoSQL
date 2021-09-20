package org.apache.spark.secco.benchmark

import java.time.Duration

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.execution.InternalDataType
import org.apache.spark.secco.optimization.plan.Relation
import org.apache.spark.secco.util.misc.{DataLoader, SparkSingle}
import org.apache.spark.secco.{Dataset, SeccoSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/** A class that stores the results of a [[Benchmark]] */
case class BenchmarkResult(
    benchmarkName: String,
    loadingTime: Duration,
    initTime: Duration,
    optimizationTime: Duration,
    executionTime: Duration,
    others: Map[String, String] = Map()
) {

  def totalTime: Duration =
    loadingTime.plus(initTime).plus(optimizationTime).plus(executionTime)

  override def toString: String = {
    s"""
       |== Benchmark Results ==
       |benchmark: $benchmarkName
       |loading time: ${loadingTime.toMillis}ms
       |init time: ${initTime.toMillis}ms
       |optimization time: ${optimizationTime.toMillis}ms
       |execution time: ${executionTime.toMillis}ms
       |total time: ${totalTime.toMillis}ms
       |others:${others.toString()}
       |""".stripMargin

  }

}

/**
  * Base class that represents a benchmark.
  *
  * A benchmark contains following steps:
  *   1. loadData: load the data related to benchmark
  *   2. preprocess: preprocess the data loaded
  *   3. genQuery: generate the query of the test-case w.r.t preprocessed data
  *   4. test: perform the benchmark with preprocessed data and generated query
  */
abstract class Benchmark {

  lazy val dlSession: SeccoSession = SeccoSession.currentSession

  /** This attribute will be set during `test`` and `query` */
  protected var _inputPath: Option[String] = None

  /** Name for this benchmark, automatically inferred based on class name. */
  val benchmarkName: String = {
    val className = getClass.getSimpleName
    if (className endsWith "$") className.dropRight(1) else className
  }

  /** load the data */
  protected def loadData(path: String): Map[String, Dataset]

  /** preprocess the data */
  protected def preprocess(
      inputData: Map[String, Dataset]
  ): Map[String, Dataset]

  /** generate the [[Dataset]] */
  protected def genQuery(inputData: Map[String, Dataset]): Dataset

  /** perform the testing by
    *   1. loading the data
    *   2. preprocess the data
    *   3. generate the query
    *   4. execute the query and [[BenchmarkResult]]
    */
  def test(path: String): BenchmarkResult = {

    //init Session
    val sc = dlSession.sparkContext
    val counterManger = dlSession.sessionState.counterManager

    //load data
    val time1 = System.currentTimeMillis()

    _inputPath = Some(path)
    val inputData = loadData(path)

    val time2 = System.currentTimeMillis()
    val loadingTime = Duration.ofMillis(time2 - time1)

    //perform some preprocessing on input data
    val initData = preprocess(inputData)

    val time3 = System.currentTimeMillis()
    val initTime = Duration.ofMillis(time3 - time2)

    //perform query optimization
    val ds = genQuery(initData)

    val optimizedPlan = ds.queryExecution.executionPlan

    println(s"""
         |== Testing Benchmark ==
         |${benchmarkName}
         |""".stripMargin)

    println(ds.queryExecution)

    val time4 = System.currentTimeMillis()
    val optimizationTime = Duration.ofMillis(time4 - time3)

    //execute the optimized plan

    println(s"""
         |== Start Executing Physical Plan ==
         |""".stripMargin)
    val count = optimizedPlan.count()
    counterManger
      .getOrCreateCounter("benchmark", "result_count")
      .increment(count)
    val time5 = System.currentTimeMillis()
    var executionTime = Duration.ofMillis(time5 - time4)

    // we need to deduct the additional time to perform an additional round of communication.
    if (dlSession.sessionState.conf.recordCommunicationTime) {
      executionTime.minusMillis(
        counterManger
          .getOrCreateCounter("benchmark", "communicationTime(ms)")
          .value
      )
    }

    BenchmarkResult(
      benchmarkName,
      loadingTime,
      initTime,
      optimizationTime,
      executionTime,
      counterManger
        .getCounters("benchmark")
        .map(f => ((s"${f.scope}.${f.name}"), f.value.toString))
        .toMap
    )
  }

  /** Return the [[Dataset]] of the query to be tested */
  def query(path: String): Dataset = {

    //init Session
    val sc = dlSession.sparkContext

    //load data
    _inputPath = Some(path)
    val inputData = loadData(path)

    //perform some preprocessing on input data
    val initData = preprocess(inputData)

    //gen query
    genQuery(initData)
  }

}

/** Base class of benchmarks related to Graph query. */
abstract class GraphBenchmark extends Benchmark {}

/** Base class of benchmarks related to SQL query. */
abstract class SQLBenchmark extends Benchmark {

  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
  def relationsWithAddress: Map[CatalogTable, String]

  override protected def loadData(path: String): Map[String, Dataset] = {
    relationsWithAddress.map {
      case (catalogTable, suffix) =>
        val dataPath = s"${path}/${suffix}"
        val rdd = DataLoader.loadTSV(dataPath)

        (catalogTable.tableName, Dataset.fromRDD(rdd, catalogTable, dlSession))
    }
  }

  override protected def preprocess(
      inputData: Map[String, Dataset]
  ): Map[String, Dataset] = {
    val conf = dlSession.sessionState.conf

    //compute the histogram of the relations
    conf.delayStrategy match {
      case "DP" | "Greedy" =>
        inputData.foreach {
          case (key, value) =>
            value.logical.asInstanceOf[Relation].computeStats()
        }
      case "Heuristic" | "NoDelay" | "AllDelay" | "JoinDelay" =>
      case _ =>
        throw new BenchmarkException(
          s"${conf.delayStrategy} is not supported in preprocess of ${this.getClass.getName}"
        )
    }

    inputData
  }

}

/** Base class of benchmarks related to subgraph query. */
abstract class SubgraphBenchmark extends GraphBenchmark {

  /** Subgraph query to be executed, which is to be override.
    * Each edge is used to create a dataset in [[loadData]]
    * Query is of form: "node1-node2;node3-node4;node5-node6"
    * e.g.,
    *   a-b;b-c;c-a (A triangle query)
    *      /\
    *     /__\
    */
  def edgesOfSubgraphQuery: String

  /** Parse the subgraph query into CatalogTable, which is later assigned to edge relation of the graph */
  private def parseEdgesOfSubgraphQuery: Seq[CatalogTable] = {
    val edges = edgesOfSubgraphQuery.split(";").map(_.split("-")).map {
      rawEdge =>
        try {
          val src = rawEdge(0)
          val dst = rawEdge(1)
          (src, dst)
        } catch {
          case e: Throwable =>
            throw new BenchmarkException(
              s"${edgesOfSubgraphQuery} does not follow form: node1-node2;node3-node4;node5-node6"
            )
        }
    }

    val prefix = "G"
    var idx = 1

    val catalogTables = edges.map { edge =>
      val tableName = s"$prefix$idx"
      idx += 1
      val schemas = Seq(CatalogColumn(edge._1), CatalogColumn(edge._2))
      CatalogTable(tableName, schemas)
    }

    catalogTables
  }

  override protected def loadData(path: String): Map[String, Dataset] = {

    val undirectedGraphPath = s"${path}/undirected"
    val g =
      DataLoader.loadGraph(SparkSingle.getSparkSession(), undirectedGraphPath)
    val catalogEdges = parseEdgesOfSubgraphQuery
    val datasetMap = catalogEdges.map { catalogEdge =>
      catalogEdge.tableName -> dlSession.createDatasetFromRDD(
        g,
        Some(catalogEdge.tableName),
        Some(catalogEdge.schema.map(_.columnName))
      )
    }.toMap

    datasetMap
  }

  override protected def preprocess(
      inputData: Map[String, Dataset]
  ): Map[String, Dataset] = {
    val conf = dlSession.sessionState.conf

    //compute the histogram of the relations
    conf.delayStrategy match {
      case "DP" | "Greedy" =>
        inputData.foreach {
          case (key, value) =>
            value.logical.asInstanceOf[Relation].computeStats()
        }
      case "Heuristic" | "NoDelay" | "AllDelay" | "JoinDelay" =>
      case _ =>
        throw new BenchmarkException(
          s"${conf.delayStrategy} is not supported in preprocess of ${this.getClass.getName}"
        )
    }

    inputData
  }

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val datasets = inputData.values.toSeq
    val joinedTable = datasets.reduceLeft[Dataset] {
      case (l: Dataset, r: Dataset) => l.naturalJoin(r)
    }

    joinedTable
  }

}

/** Base class of benchmarks related to Graph Analytic Query. */
abstract class GraphAnalyticBenchmark extends GraphBenchmark {

  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
  def relationsWithAddress: Map[CatalogTable, String]

  override protected def loadData(path: String): Map[String, Dataset] = {
    relationsWithAddress.map {
      case (catalogTable, suffix) =>
        val dataPath = s"${path}/${suffix}"
        val rdd = DataLoader.loadTSV(dataPath)

        (catalogTable.tableName, Dataset.fromRDD(rdd, catalogTable, dlSession))
    }
  }
}
