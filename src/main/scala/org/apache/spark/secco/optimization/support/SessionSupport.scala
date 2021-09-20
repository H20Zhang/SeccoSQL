package org.apache.spark.secco.optimization.support

import org.apache.spark.SparkContext
import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.analysis.Analyzer
import org.apache.spark.secco.catalog.{
  CachedDataManager,
  Catalog,
  FunctionRegistry
}
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.planning.SeccoPlanner
import org.apache.spark.secco.optimization.SeccoOptimizer
import org.apache.spark.secco.parsing.ParserInterface

/**
  * A trait that provides convenient methods for accessing session states's components.
  */
trait SessionSupport {
  def dlSession = SeccoSession.currentSession

  def sc: SparkContext = dlSession.sessionState.sc
  def conf: SeccoConfiguration = dlSession.sessionState.conf
  def catalog: Catalog = dlSession.sessionState.catalog
  def currentDatabase: String = dlSession.sessionState.currentDatabase
  def functionRegistry: FunctionRegistry =
    dlSession.sessionState.functionRegistry
  def sqlParser: ParserInterface = dlSession.sessionState.sqlParser
  def analyzer: Analyzer = dlSession.sessionState.analyzer
  def optimizer: SeccoOptimizer = dlSession.sessionState.optimizer
  def planner: SeccoPlanner = dlSession.sessionState.planner
  def cachedDataManager: CachedDataManager =
    dlSession.sessionState.cachedDataManager

}
