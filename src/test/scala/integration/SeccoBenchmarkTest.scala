package integration
import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.benchmark.testcases._
import org.apache.spark.secco.catalog.Catalog
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.benchmark.{
  SeccoBenchmarkExecutor,
  GraphAnalyticBenchmark
}
import org.apache.spark.secco.optimization.statsEstimation.exact.{
  ExactLogicalPlanEstimation,
  ExactStatsPlanVisitor
}
import util.{SeccoFunSuite, IntegrationTestTag, TestData}
import org.scalatest.Tag

/** This class contains test related to benchmarks */
class SeccoBenchmarkTest extends SeccoFunSuite {

  val wiki = "./datasets/wiki"
  val debug = "./datasets/debugData"
  val imdb = "./datasets/imdb"
  val workloadExp = "./datasets/workload_exp_demo/W1/Low"

  /** Test SeccoBenchmarkExecutor */
  test("debugQuery", IntegrationTestTag) {
    //create Benchmark
    val query = "Debug1"
    val benchmarkExecutor = new SeccoBenchmarkExecutor

    //find benchmark
    val DebugOpt = benchmarkExecutor.findBenchmark(query)
    assert(DebugOpt.nonEmpty)

    //execute benchmark
    benchmarkExecutor.execute(
      query,
      debug,
      isSessionReset = true,
      externalConf = None
    )
  }

  /** Test test-cases of subgraphQuery, S1-S8 and C1-C4 */
  test("subgraphQuery", IntegrationTestTag) {

    val benchmarkExecutor = new SeccoBenchmarkExecutor
    val dataPath = wiki

    val subgraphQueryBenchmarkNames = benchmarkExecutor
      .benchmarkNameList()
      .filter(str => str.startsWith("S") || str.startsWith("C"))
      .filter(_ == "S1")

    val externalConf = new SeccoConfiguration
    externalConf.setDelayStrategy("Heuristic")
//    externalConf.setRecordCommunicationTime(true)

    //execute benchmark
    subgraphQueryBenchmarkNames.sorted.foreach { query =>
      benchmarkExecutor.execute(
        query,
        dataPath,
        isSessionReset = true,
        externalConf = Some(externalConf)
      )
    }
  }

  /** Test test-cases of subgraphQuery, O1-O12 */
  test("sqlQuery", IntegrationTestTag) {

    val benchmarkExecutor = new SeccoBenchmarkExecutor
    val dataPath = imdb

    val sqlQueryBenchmarkNames = benchmarkExecutor
      .benchmarkNameList()
      .filter(str => str.startsWith("O"))
      .filter(_ == "O7")

    val externalConf = new SeccoConfiguration

    //execute benchmark
    sqlQueryBenchmarkNames.sorted.foreach { query =>
      benchmarkExecutor.execute(
        query,
        dataPath,
        isSessionReset = true,
        externalConf = Some(externalConf)
      )
    }
  }

  /** Test test-cases of workload experiment queries, W1-W10 */
  test("workloadExp", IntegrationTestTag) {

    val benchmarkExecutor = new SeccoBenchmarkExecutor
    val dataPathPrefix = "./datasets/workload_exp_demo"

    val workloadQueryBenchmarkNames = benchmarkExecutor
      .benchmarkNameList()
      .filter(str => str.startsWith("W"))
      .filter(benchmarkName => benchmarkName == "W1"
//        Seq("W6", "W7", "W8", "W9", "W10").contains(benchmarkName)
      )

    val externalConf = new SeccoConfiguration
    externalConf.setDelayStrategy("DP")
    externalConf.setEstimator("Exact")
    externalConf.setEnableOnlyDecoupleOptimization(true)
    externalConf.setEnableEarlyAggregationOptimization(false)
    externalConf.setRecordCommunicationTime(true)

    //execute benchmark
    workloadQueryBenchmarkNames.sorted.foreach { query =>
      val dataPath = s"$dataPathPrefix/${query}/Low/"
      benchmarkExecutor.execute(
        query,
        dataPath,
        isSessionReset = true,
        externalConf = Some(externalConf)
      )
    }
  }

  /** Test test-cases of graph analytic query, I1-I3, G1I1-G2I3 */
  test("graphAnalyticQuery", IntegrationTestTag) {

    val benchmarkExecutor = new SeccoBenchmarkExecutor
    val dataPath = wiki

    val subgraphQueryBenchmarkNames = benchmarkExecutor
      .benchmarkNameList()
      .filter(str => str.startsWith("I") || str.startsWith("G"))
      .filter(_ == "I1")
//      .filter(_.startsWith("I"))

    val externalConf = new SeccoConfiguration
    externalConf.setDelayStrategy("Heuristic")
    externalConf.setLandmark(0.0)
    //    externalConf.setRecordCommunicationTime(true)

    //execute benchmarks
    subgraphQueryBenchmarkNames.sorted.foreach { query =>
      benchmarkExecutor.execute(
        query,
        dataPath,
        isSessionReset = true,
        externalConf = Some(externalConf)
      )
    }
  }

  test("main", IntegrationTestTag) {
//    val testDatasetPath = "./datasets/workload_exp_demo/W1/Low/"
    val testDatasetPath = wiki
//    val delayStrategy = "HeuristicSize"
    val delayStrategy = "JoinDelay"
//    val estimator = "Exact"
    val estimator = "Histogram"
    val landmark = 1
    val recordCommunicationTime = true
    val enableOnlyDecoupleOptimization = false
    val query = "S5"

    val args =
      s"-q $query -d ${testDatasetPath} --kwargs landmark=${landmark},secco.optimizer.delay_strategy=${delayStrategy},secco.pair.record_communication_time=${recordCommunicationTime},secco.optimizer.estimator=${estimator},secco.optimizer.enable_only_decouple_optimization=${enableOnlyDecoupleOptimization}"
    SeccoBenchmarkExecutor.main(args.split("\\s"))
  }

  test("demo") {
    // Obtain SeccoSession via singleton.
    val dlSession = SeccoSession.currentSession

    // Create datasets.
    val seq1 = Seq(Array(1.0, 2.0), Array(2.0, 2.0))
    val tableName = "R1"
    val schema = Seq("A", "B")
    val ds1 =
      dlSession.createDatasetFromSeq(seq1, Some(tableName), Some(schema))

    // Construct RA expression via relational algebra like API.
    val ds2 = ds1.select("A < B")

    // Explain the query execution of ds1 and ds2. It will show parsed plan, analyzed plan, optimized plan, execution plan.
    ds1.explain()
    ds2.explain()
  }
}
