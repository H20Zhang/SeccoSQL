package org.apache.spark.secco
import org.apache.spark.SparkContext
import org.apache.spark.secco.analysis.Analyzer
import org.apache.spark.secco.catalog.{
  CachedDataManager,
  Catalog,
  FunctionRegistry
}
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.planning.SeccoPlanner
import org.apache.spark.secco.optimization.SeccoOptimizer
import org.apache.spark.secco.parsing.{ParserInterface, SQLParser}
import org.apache.spark.secco.util.counter.CounterManager
import org.apache.spark.secco.util.misc.SparkSingle

/**
  * A class that holds all session-specific state in a given [[SeccoSession]].
  *
  * @param sc: The context of Spark.
  * @param conf SQL-specific key-value configurations.
  * @param functionRegistry Internal catalog for managing functions registered by the user.
  * @param catalog Internal catalog for managing table and database states.
  * @param sqlParser Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
  * @param analyzer Logical query plan analyzer for resolving unresolved attributes and relations.
  * @param optimizer Logical query plan optimizer.
  * @param planner Planner that converts optimized logical plans to physical plans.
  */
class SessionState(
    val sc: SparkContext,
    val conf: SeccoConfiguration,
    val catalog: Catalog,
    val currentDatabase: String,
    val functionRegistry: FunctionRegistry,
    val sqlParser: ParserInterface,
    val analyzer: Analyzer,
    val optimizer: SeccoOptimizer,
    val planner: SeccoPlanner,
    val cachedDataManager: CachedDataManager,
    val counterManager: CounterManager
) {}

object SessionState {

  /** The SessionState with default configuration. */
  lazy val defaultSessionState = newDefaultSessionState

  /** Create a new SessionState with default configuration. */
  def newDefaultSessionState: SessionState = {
    val sc = SparkSingle.getSparkContext()
    val conf = new SeccoConfiguration
    val catalog = Catalog.newDefaultCatalog
    val currentDB = Catalog.defaultDBName
    val functionRegistry = FunctionRegistry.newBuiltin
    val analyzer =
      new Analyzer(catalog, conf, functionRegistry, conf.maxIteration)
    val optimizer = new SeccoOptimizer(conf)
    val planner = new SeccoPlanner(sc, conf)
    val cachedDataManager = CachedDataManager.newDefaultDataManager
    val counterManager = CounterManager.newDefaultCounterManager

    new SessionState(
      sc,
      conf,
      catalog,
      currentDB,
      functionRegistry,
      SQLParser,
      analyzer,
      optimizer,
      planner,
      cachedDataManager,
      counterManager
    )
  }

  /**
    * Create a new SessionState with given configuration.
    * @param conf user provided configuration
    * @return new SessionState initialized with given configuration
    */
  def newSessionStateWithConf(conf: SeccoConfiguration): SessionState = {

    val sc = SparkSingle.getSparkContext()
    val catalog = Catalog.newDefaultCatalog
    val currentDB = Catalog.defaultDBName
    val functionRegistry = FunctionRegistry.newBuiltin
    val analyzer =
      new Analyzer(catalog, conf, functionRegistry, conf.maxIteration)
    val optimizer = new SeccoOptimizer(conf)
    val planner = new SeccoPlanner(sc, conf)
    val cachedDataManager = CachedDataManager.newDefaultDataManager
    val counterManager = CounterManager.newDefaultCounterManager

    new SessionState(
      sc,
      conf,
      catalog,
      currentDB,
      functionRegistry,
      SQLParser,
      analyzer,
      optimizer,
      planner,
      cachedDataManager,
      counterManager
    )
  }
}
