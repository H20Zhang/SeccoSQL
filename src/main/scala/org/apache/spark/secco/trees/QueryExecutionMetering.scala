package org.apache.spark.secco.trees

import com.google.common.util.concurrent.AtomicLongMap

import scala.collection.JavaConverters._

case class QueryExecutionMetering() {
  private val timeMap = AtomicLongMap.create[String]()
  private val numRunsMap = AtomicLongMap.create[String]()
  private val numEffectiveRunsMap = AtomicLongMap.create[String]()
  private val timeEffectiveRunsMap = AtomicLongMap.create[String]()

  /** Resets statistics about time spent running specific rules */
  def resetMetrics(): Unit = {
    timeMap.clear()
    numRunsMap.clear()
    numEffectiveRunsMap.clear()
    timeEffectiveRunsMap.clear()
  }

  def totalTime: Long = {
    timeMap.sum()
  }

  def totalNumRuns: Long = {
    numRunsMap.sum()
  }

  def incExecutionTimeBy(ruleName: String, delta: Long): Unit = {
    timeMap.addAndGet(ruleName, delta)
  }

  def incTimeEffectiveExecutionBy(ruleName: String, delta: Long): Unit = {
    timeEffectiveRunsMap.addAndGet(ruleName, delta)
  }

  def incNumEffectiveExecution(ruleName: String): Unit = {
    numEffectiveRunsMap.incrementAndGet(ruleName)
  }

  def incNumExecution(ruleName: String): Unit = {
    numRunsMap.incrementAndGet(ruleName)
  }

  /** Dump statistics about time spent running specific rules. */
  def dumpTimeSpent(): String = {
    val map = timeMap.asMap().asScala
    val maxLengthRuleNames = map.keys.map(_.toString.length).max

    val colRuleName = "Rule".padTo(maxLengthRuleNames, " ").mkString
    val colRunTime = "Effective Time / Total Time".padTo(len = 47, " ").mkString
    val colNumRuns = "Effective Runs / Total Runs".padTo(len = 47, " ").mkString

    val ruleMetrics = map.toSeq
      .sortBy(_._2)
      .reverseMap {
        case (name, time) =>
          val timeEffectiveRun = timeEffectiveRunsMap.get(name)
          val numRuns = numRunsMap.get(name)
          val numEffectiveRun = numEffectiveRunsMap.get(name)

          val ruleName = name.padTo(maxLengthRuleNames, " ").mkString
          val runtimeValue =
            s"$timeEffectiveRun / $time".padTo(len = 47, " ").mkString
          val numRunValue =
            s"$numEffectiveRun / $numRuns".padTo(len = 47, " ").mkString
          s"$ruleName $runtimeValue $numRunValue"
      }
      .mkString("\n", "\n", "")

    s"""
       |=== Metrics of Analyzer/Optimizer Rules ===
       |Total number of runs: $totalNumRuns
       |Total time: ${totalTime / 1000000000d} seconds
       |
       |$colRuleName $colRunTime $colNumRuns
       |$ruleMetrics
     """.stripMargin
  }
}
