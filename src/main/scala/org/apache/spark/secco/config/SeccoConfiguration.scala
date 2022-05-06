package org.apache.spark.secco.config

import org.apache.spark.storage.StorageLevel

/** Configurations for [[org.apache.spark.secco.SeccoSession]] */
class SeccoConfiguration extends Configuration {

  /* Keys */
  private val NUM_PARTITION_KEY = "secco.num_partition"
  private val NUM_CORE_KEY = "secco.num_core"
  private val TIMEOUT_KEY = "secco.timeout"
  private val IS_YARN_KEY = "secco.is_yarn"

  private val Pair_MEMORY_BUDGET_KEY = "secco.pair.budget"
  private val RECORD_COMMUNICATION_TIME_KEY =
    "secco.pair.record_communication_time"

  private val ENABLE_COMPATIBLE_KEY = "secco.analyzer.enable_compatible"

  private val MAX_ITERATION_KEY = "secco.optimizer.max_iteration"
  private val NUM_BIN_HISTOGRAM_KEY =
    "secco.optimizer.stats.num_bin_histogram"
  private val DELAY_STRATEGY_KEY = "secco.optimizer.delay_strategy"
  private val ESTIMATOR_KEY =
    "secco.optimizer.estimator"
  private val EXACT_CARDINALITY_MODE_KEY =
    "secco.optimizer.exact_cardinality_mode"
  private val ENABLE_ONLY_DECOUPLE_OPTIMIZATION_KEY =
    "secco.optimizer.enable_only_decouple_optimization"
  private val ENABLE_EARLY_AGGREGATION_OPTIMIZATION_KEY =
    "secco.optimizer.enable_early_aggregation_optimization"
  private val OPTIMIZER_STATS_COMPUTER_KEY = "secco.optimizer.stats_computer"

  private val CACHE_SIZE_KEY = "secco.local.cache_size"
  private val ENABLE_CACHE_KEY = "secco.local.cache_enable"
  private val SMALL_CHANGE_KEY = "secco.local.small_change"
  private val RDD_Cache_Level_KEY = "secco.local.rdd.cache_level"

  private val ENABLE_LOCAL_PREPROCESSING_OPTIMIZATION_KEY =
    "secco.planner.local_preprocessing_optimization"
//  private val PLANNER_STATS_COMPUTER_KEY = "secco.planner.stats_computer"

  private val RECURSION_NUMRUN_KEY = "secco.recursion.numRun"

  private val VERBOSE_OUTPUT_KEY = "secco.debug.plan.verboseOutput"

  /* == Temporary Keys == */
  private val LANDMARK_KEY = "LANDMARK"
  private val UPDATE_OP_KEY = "UPDATE_OP"

  /* == General Parameters == */
  /** numbers of partitions (this is only a minimal value, the numbers of partition will automatically increase in case of large datasets) */
  def numPartition = getInt(NUM_PARTITION_KEY)
  def setNumPartition(value: Int) = setInt(NUM_PARTITION_KEY, value)

  /** numbers of cores */
  def numCore = getInt(NUM_CORE_KEY)
  def setNumCore(value: Int) = setInt(NUM_CORE_KEY, value)

  /** timeout in terms of seconds */
  def timeoutDuration = getDurationAsSeconds(TIMEOUT_KEY)
  def setTimeoutDurationBySeconds(value: Long) =
    setDurationAsSeconds(TIMEOUT_KEY, value)

  /** whether running the system on yarn or locally */
  def isYarn = getBoolean(IS_YARN_KEY)

  /* == Analyzer Parameters == */
  /** whether enable compatible layer of analyzer with previous secco */
  def enableCompatiblity = getBoolean(ENABLE_COMPATIBLE_KEY)
  def setEnableCompatibility(value: Boolean) =
    setBoolean(ENABLE_COMPATIBLE_KEY, value)

  /* == Optimizer Parameters == */
  /** maximum numbers of iteration for applying optimizer's rule */
  def maxIteration = getInt(MAX_ITERATION_KEY)
  def setMaxIteration(value: Int) = setInt(MAX_ITERATION_KEY, value)

  /** numbers of bin for historgram */
  def numBinHistogram = getInt(NUM_BIN_HISTOGRAM_KEY)
  def setNumBinHistogram(value: Int) = setInt(NUM_BIN_HISTOGRAM_KEY, value)

  /** the delay strategy used in rule `MarkDelay` */
  def delayStrategy = getString(DELAY_STRATEGY_KEY)
  def setDelayStrategy(value: String) = setString(DELAY_STRATEGY_KEY, value)

  /** the estimator used estimating statistic during optimization. */
  def estimator = getString(ESTIMATOR_KEY)
  def setEstimator(value: String) = setString(ESTIMATOR_KEY, value)

  /** the mode for providing exact cardinality */
  def exactCardinalityMode = getString(EXACT_CARDINALITY_MODE_KEY)
  def setExactCardinalityMode(value: String) =
    setString(EXACT_CARDINALITY_MODE_KEY, value)

  /** flag in optimizer to indicate only enable optimization rules after decouple stage */
  def enableOnlyDecoupleOptimization =
    getBoolean(ENABLE_ONLY_DECOUPLE_OPTIMIZATION_KEY)
  def setEnableOnlyDecoupleOptimization(value: Boolean) =
    setBoolean(ENABLE_ONLY_DECOUPLE_OPTIMIZATION_KEY, value)

  /** flag in optimizer to indicate whether enable early local aggregation optimization */
  def enableEarlyAggregationOptimization =
    getBoolean(ENABLE_EARLY_AGGREGATION_OPTIMIZATION_KEY)
  def setEnableEarlyAggregationOptimization(value: Boolean) =
    setBoolean(ENABLE_EARLY_AGGREGATION_OPTIMIZATION_KEY, value)

  /** the stats computer used in optimizer */
  def optimizerStatsComputer = getString(OPTIMIZER_STATS_COMPUTER_KEY)
  def setOptimizerStatsComputer(value: String) =
    setString(OPTIMIZER_STATS_COMPUTER_KEY, value)

  /* == Planner Parameters == */
  /** flag in planner to enable local preprocessing optimization */
  def enableLocalPreprocessingOptimization =
    getBoolean(ENABLE_LOCAL_PREPROCESSING_OPTIMIZATION_KEY)
  def setEnableLocalPreprocessingOptimization(value: Boolean) =
    setBoolean(ENABLE_LOCAL_PREPROCESSING_OPTIMIZATION_KEY, value)

  /** the stats computer used in planner */
//  def plannerStatsComputer = getString(PLANNER_STATS_COMPUTER_KEY)
//  def setPlannerStatsComputer(value: String) =
//    setString(PLANNER_STATS_COMPUTER_KEY, value)

  /* == Communication Parameters == */
  /** memory budget allocated for each partition in terms of Bytes */
  def pairMemoryBudget = getSizeAsKB(Pair_MEMORY_BUDGET_KEY) * 1e3
  def setPairMemoryBudget(value: Long) =
    setSizeAsKB(Pair_MEMORY_BUDGET_KEY, (value / 1e3).toLong)

  /** whether record communication time incurred by communication. */
  def recordCommunicationTime = getBoolean(RECORD_COMMUNICATION_TIME_KEY)
  def setRecordCommunicationTime(value: Boolean) =
    setBoolean(RECORD_COMMUNICATION_TIME_KEY, value)

  /* == Computation Parameters == */
  /** default cache size for LRU */
  def cacheSize = getInt(CACHE_SIZE_KEY)
  def setCacheSize(value: Int) = setInt(CACHE_SIZE_KEY, value)

  /** enable cache */
  def enableCache = getBoolean(ENABLE_CACHE_KEY)
  def setEnableCache(value: Boolean) = setBoolean(ENABLE_CACHE_KEY, value)

  /** changes of value in iterative algorithms that are considered small */
  def smallChange = getDouble(SMALL_CHANGE_KEY)
  def setSmallChange(value: Double) = setDouble(SMALL_CHANGE_KEY, value)

  /** default cache level of RDD */
  def rddCacheLevel = StorageLevel.fromString(getString(RDD_Cache_Level_KEY))
  def setRDDCacheLevel(value: String) = setString(RDD_Cache_Level_KEY, value)

  /* == Recursion Parameters == */
  def recursionNumRun = getInt(RECURSION_NUMRUN_KEY)
  def setRecursionNumRun(value: Int) = setInt(RECURSION_NUMRUN_KEY, value)

//  /* Debug Parameters */
//  /** if LOP content will be shown ??? */
//  def showLOPContent = getBoolean(SHOW_LOP_CONTENT_KEY)
//  def setShowLOPContent(value: Boolean) =
//    setBoolean(SHOW_LOP_CONTENT_KEY, value)

  /* == Debug Related Parameters */
  def verboseOuptut = getBoolean(VERBOSE_OUTPUT_KEY)
  def setVerboseOutput(value: Boolean) = setBoolean(VERBOSE_OUTPUT_KEY, value)

  //todo: replace following temporary solution by a more user friendly one.
  /* Graph Algorithm Parameter */
  def landmark = getDouble(LANDMARK_KEY)
  def setLandmark(value: Double) = setDouble(LANDMARK_KEY, value)

  def updateOp = "min"
  def setUpdateOp(value: String) = setString(UPDATE_OP_KEY, value)

}

object SeccoConfiguration {
  def newDefaultConf() = {
    new SeccoConfiguration()
  }
}
