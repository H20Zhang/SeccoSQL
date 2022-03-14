package org.apache.spark.secco.optimization.support

import org.apache.spark.SparkContext
import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.analysis.Analyzer
import org.apache.spark.secco.catalog.{
  TempViewManager,
  Catalog,
  FunctionRegistry
}
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.planning.SeccoPlanner
import org.apache.spark.secco.optimization.SeccoOptimizer
import org.apache.spark.secco.parsing.ParserInterface

/** A trait that provides convenient methods for accessing session states's components.
  */
trait SessionSupport {
  def seccoSession = SeccoSession.currentSession

  def sc: SparkContext = seccoSession.sessionState.sc
  def conf: SeccoConfiguration = seccoSession.sessionState.conf
  def catalog: Catalog = seccoSession.sessionState.catalog
  def currentDatabase: String = seccoSession.sessionState.currentDatabase
  def functionRegistry: FunctionRegistry =
    seccoSession.sessionState.functionRegistry
  def sqlParser: ParserInterface = seccoSession.sessionState.sqlParser
  def analyzer: Analyzer = seccoSession.sessionState.analyzer
  def optimizer: SeccoOptimizer = seccoSession.sessionState.optimizer
  def planner: SeccoPlanner = seccoSession.sessionState.planner
  def cachedDataManager: TempViewManager =
    seccoSession.sessionState.tempViewManager

}
