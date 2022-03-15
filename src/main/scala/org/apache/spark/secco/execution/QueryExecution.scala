package org.apache.spark.secco.execution

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.optimization.plan.RootNode
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.execution.storage.row.InternalRow

/** The primary workflow for executing relational queries using Spark.  Designed to allow easy
  * access to the intermediate phases of query execution for developers.
  *
  * While this is not a public class, we should avoid changing the function names for the sake of
  * changing them, because a lot of developers use the feature for debugging.
  */
class QueryExecution(
    val seccoSession: SeccoSession,
    val logical: LogicalPlan
) {

  lazy val analyzedPlan: LogicalPlan = {
    seccoSession.sessionState.analyzer.execute(logical)
  }

  lazy val optimizedPlan: LogicalPlan = {
    val rootedAnalyzedPlan = RootNode(analyzedPlan)
    seccoSession.sessionState.optimizer.execute(rootedAnalyzedPlan)
  }

  lazy val executionPlan: SeccoPlan = {
    seccoSession.sessionState.planner.plan(optimizedPlan).next()
  }

  lazy val toRdd: RDD[InternalRow] = executionPlan.rdd()

  protected def stringOrError[A](f: => A): String =
    try f.toString
    catch { case e: Exception => e.toString }

  def simpleString: String = {
    s"""== Physical Plan ==
       |${stringOrError(executionPlan.treeString(verbose = false))}
      """.stripMargin.trim
  }

  override def toString: String = completeString(false)

  private def completeString(appendStats: Boolean): String = {
    def output =
      analyzedPlan.output.map(o => s"${o.name}: ${o.dataType.simpleString}")
    val analyzedPlanString = Seq(
      stringOrError(output),
      stringOrError(analyzedPlan.treeString(verbose = true))
    ).filter(_.nonEmpty).mkString("\n")

    val optimizedPlanString = if (appendStats) {
      // trigger to compute stats for logical plans
      optimizedPlan.treeString(verbose = true, addSuffix = true)
    } else {
      optimizedPlan.treeString(verbose = true)
    }

    s"""== Parsed Logical Plan ==
       |${stringOrError(logical.treeString(verbose = true))}
       |== Analyzed Logical Plan ==
       |$analyzedPlan
       |== Optimized Logical Plan ==
       |${stringOrError(optimizedPlanString)}
       |== Physical Plan ==
       |${stringOrError(executionPlan.treeString(verbose = true))}
    """.stripMargin.trim
  }

}
