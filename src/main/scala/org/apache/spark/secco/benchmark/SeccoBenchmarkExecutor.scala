package org.apache.spark.secco.benchmark

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.benchmark.testcases._
import org.apache.spark.secco.benchmark.util.FutureTask
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.OldInternalDataType
import org.apache.spark.secco.util.misc.SparkSingle
import org.apache.spark.rdd.RDD

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationLong

/** An implementation of BenchmarkExecutor, which includes built-in benchmarks supported by Secco. */
class SeccoBenchmarkExecutor extends BenchmarkExecutor {

  /** Benchmarks built into the systems. */
  override def builtinBenchmarks: Seq[Benchmark] =
    Seq(
      Debug1,
      S1,
      S2,
      S3,
      S4,
      S5,
      S6,
      S7,
      S8,
      C1,
      C2,
      C3,
      C4,
      I1,
      I2,
      I3,
      G1I1,
      G1I2,
      G1I3,
      G2I1,
      G2I2,
      G2I3,
      O1,
      O2,
      O3,
      O4,
      O5,
      O6,
      O7,
      O8,
      O9,
      O10,
      O11,
      O12,
      W1,
      W2,
      W3,
      W4,
      W5,
      W6,
      W7,
      W8,
      W9,
      W10
    )

  /** User provided extended benchmarks. */
  override def extendedBenchmarks: Seq[Benchmark] = Nil
}

object SeccoBenchmarkExecutor {

  /** The commandline tool for issuing Secco's benchmark. */
  def main(args: Array[String]): Unit = {
    import scopt.OParser

    case class InputConfig(
        query: String = "",
        method: String = "",
        data: String = "",
        platform: String = "",
        kwargs: Map[String, String] = Map()
    )

    //parser for args
    val builder = OParser.builder[InputConfig]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("SeccoBenchmark"),
        head("SeccoBenchmark", "0.1"),
        opt[String]('q', "query")
          .action((x, c) => c.copy(query = x))
          .text("query, i.e. 'S1'"),
        opt[String]('d', "data")
          .action((x, c) => c.copy(data = x))
          .text("path to data graph"),
        opt[Map[String, String]]("kwargs")
          .valueName("k1=v1,k2=v2...")
          .action((x, c) => c.copy(kwargs = x))
          .text("other arguments")
      )
    }

    //parse the args
    val config = OParser.parse(parser1, args, InputConfig()).get

    //set the sparkApp's name
    SparkSingle.appName =
      s"SeccoBenchmark-data:${config.data}-query:${config.query}-delay_strategy:${config
        .kwargs("secco.optimizer.delay_strategy")}"

    //inject user-defined key-value pair into Secco's configuration

    val externalConf = new SeccoConfiguration

    //set user-defined configuration entries.
    config.kwargs.foreach { case (key, value) =>
      externalConf.setString(key, value)
    }

    //set up the timeout timer
    val someTask =
      FutureTask.schedule(
        SeccoConfiguration.newDefaultConf().timeoutDuration seconds
      ) {
        SparkSingle.getSparkContext().cancelAllJobs()
        println(
          s"timeout after ${SeccoConfiguration.newDefaultConf().timeoutDuration} seconds"
        )
      }

    val benchmarkExecutor = new SeccoBenchmarkExecutor

    benchmarkExecutor.execute(
      config.query,
      config.data,
      externalConf = Some(externalConf)
    )
  }

}
