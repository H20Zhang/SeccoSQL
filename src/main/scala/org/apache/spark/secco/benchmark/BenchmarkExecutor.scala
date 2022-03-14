//package org.apache.spark.secco.benchmark
//
//import org.apache.spark.secco.SeccoSession
//import org.apache.spark.secco.config.SeccoConfiguration
//
///**
//  * Thrown when exception happens during benchmark.
//  */
//class BenchmarkException(
//    val message: String,
//    val cause: Option[Throwable] = None
//) extends Exception(message, cause.orNull)
//
///**
//  * Thrown when the benchmark is not found.
//  */
//class NoSuchBenchmarkException(
//    val benchmarkName: String,
//    val benchmarkExecutor: BenchmarkExecutor
//) extends BenchmarkException(
//      s"No benchmark:${benchmarkName} exists in ${benchmarkExecutor.getClass.toString}"
//    )
//
///** Base class of executor for executing benchmark. */
//abstract class BenchmarkExecutor {
//
//  /** Available benchmark for testing, which includes built-in benchmarks and extended benchmarks. */
//  def benchmarks: Seq[Benchmark] = builtinBenchmarks ++ extendedBenchmarks
//
//  /** Benchmarks built into the systems. */
//  def builtinBenchmarks: Seq[Benchmark]
//
//  /** User provided extended benchmarks. */
//  def extendedBenchmarks: Seq[Benchmark]
//
//  private lazy val benchmarkMap: Map[String, Benchmark] =
//    benchmarks.map(f => (f.benchmarkName, f)).toMap
//
//  /** Find the benchmark with given name. */
//  def findBenchmark(benchmarkName: String): Option[Benchmark] =
//    benchmarkMap.get(benchmarkName)
//
//  /** List the benchmark names. */
//  def benchmarkNameList(): Seq[String] =
//    benchmarkMap.values.map(_.benchmarkName).toSeq
//
//  /**
//    * Execute the benchmark with name "q" using dataset in path "dataPath", each time a new [[SeccoSession]] will be
//    * created by default.
//    *
//    * @param q name of the benchmark to run
//    * @param dataPath path to input datasets
//    * @param isSessionReset whether a new [[SeccoSession]] be created to run the benchmark
//    * @param externalConf external configuration passed to newly created [[SeccoSession]]
//    */
//  def execute(
//      q: String,
//      dataPath: String,
//      isSessionReset: Boolean = true,
//      externalConf: Option[SeccoConfiguration] = None
//  ): Unit = {
//
//    // reset session to make sure no interface happens between benchmarks
//    if (isSessionReset) {
//      externalConf match {
//        case Some(conf) =>
//          SeccoSession.setCurrentSession(
//            SeccoSession.newSessionWithConf(conf)
//          )
//        case None =>
//          SeccoSession.setCurrentSession(SeccoSession.newDefaultSession)
//      }
//    }
//
//    // find the benchmark
//    val benchmark = findBenchmark(q) match {
//      case Some(benchmark) => benchmark
//      case None            => throw new NoSuchBenchmarkException(q, this)
//    }
//
//    // execute the benchmark
//    val result = benchmark.test(dataPath)
//
//    // print the results
//    println(result)
//  }
//}
