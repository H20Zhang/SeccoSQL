package org.apache.spark.dolphin.config

import org.apache.spark.storage.StorageLevel

class DolphinConfiguration extends Configuration {

  /* Keys */
  private val NUM_PARTITION_KEY = "dolphin.num_partition"
  private val NUM_CORE_KEY = "dolphin.num_core"
  private val TIMEOUT_KEY = "dolphin.timeout"
  private val IS_YARN_KEY = "dolphin.is_yarn"

  private val HCUBE_MEMORY_BUDGET_KEY = "dolphin.hcube.budget"
  private val RECORD_COMMUNICATION_TIME_KEY =
    "dolphin.hcube.record_communication_time"

  private val ENABLE_COMPATIBLE_KEY = "dolphin.analyzer.enable_compatible"

  private val MAX_ITERATION_KEY = "dolphin.optimizer.max_iteration"
  private val NUM_BIN_HISTOGRAM_KEY =
    "dolphin.optimizer.stats.num_bin_histogram"
  private val DELAY_STRATEGY_KEY = "dolphin.optimizer.delay_strategy"
  private val ESTIMATOR_KEY =
    "dolphin.optimizer.estimator"
  private val EXACT_CARDINALITY_MODE_KEY =
    "dolphin.optimizer.exact_cardinality_mode"
  private val ENABLE_ONLY_DECOUPLE_OPTIMIZATION_KEY =
    "dolphin.optimizer.enable_only_decouple_optimization"
  private val OPTIMIZER_STATS_COMPUTER_KEY = "dolphin.optimizer.stats_computer"

  private val CACHE_SIZE_KEY = "dolphin.local.cache_size"
  private val ENABLE_CACHE_KEY = "dolphin.local.cache_enable"
  private val SMALL_CHANGE_KEY = "dolphin.local.small_change"
  private val RDD_Cache_Level_KEY = "dolphin.local.rdd.cache_level"

  private val SHOW_LOP_CONTENT_KEY = "dolphin.debug.show_LOp_Content"

  private val ENABLE_LOCAL_PREPROCESSING_OPTIMIZATION_KEY =
    "dolphin.planner.local_preprocessing_optimization"
  //  private val PLANNER_STATS_COMPUTER_KEY = "dolphin.planner.stats_computer"

  /* Temporary Keys */
  private val LANDMARK_KEY = "LANDMARK"
  private val UPDATE_OP_KEY = "UPDATE_OP"

  /* General Parameters */
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

  /* Analyzer Parameters */
  /** whether enable compatible layer of analyzer with previous dolphin */
  def enableCompatiblity = getBoolean(ENABLE_COMPATIBLE_KEY)
  def setEnableCompatibility(value: Boolean) =
    setBoolean(ENABLE_COMPATIBLE_KEY, value)

  /* Optimizer Parameters */
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

  /** the stats computer used in optimizer */
  def optimizerStatsComputer = getString(OPTIMIZER_STATS_COMPUTER_KEY)
  def setOptimizerStatsComputer(value: String) =
    setString(OPTIMIZER_STATS_COMPUTER_KEY, value)

  /* Planner Parameters */
  /** flag in planner to enable local preprocessing optimization */
  def enableLocalPreprocessingOptimization =
    getBoolean(ENABLE_LOCAL_PREPROCESSING_OPTIMIZATION_KEY)
  def setEnableLocalPreprocessingOptimization(value: Boolean) =
    setBoolean(ENABLE_LOCAL_PREPROCESSING_OPTIMIZATION_KEY, value)

  /** the stats computer used in planner */
  //  def plannerStatsComputer = getString(PLANNER_STATS_COMPUTER_KEY)
  //  def setPlannerStatsComputer(value: String) =
  //    setString(PLANNER_STATS_COMPUTER_KEY, value)

  /* HCube Parameters */
  /** memory budget allocated for each partition in terms of Bytes */
  def hcubeMemoryBudget = getSizeAsKB(HCUBE_MEMORY_BUDGET_KEY) * 1e3
  def setHcubeMemoryBudget(value: Long) =
    setSizeAsKB(HCUBE_MEMORY_BUDGET_KEY, (value / 1e3).toLong)

  /** whether record communication time incurred by hcube. */
  def recordCommunicationTime = getBoolean(RECORD_COMMUNICATION_TIME_KEY)
  def setRecordCommunicationTime(value: Boolean) =
    setBoolean(RECORD_COMMUNICATION_TIME_KEY, value)

  /* Local Execution Parameters */
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

  /* Debug Parameters */
  /** if LOP content will be shown ??? */
  def showLOPContent = getBoolean(SHOW_LOP_CONTENT_KEY)
  def setShowLOPContent(value: Boolean) =
    setBoolean(SHOW_LOP_CONTENT_KEY, value)

  //todo: replace following temporary solution by a more user friendly one.
  /* Graph Algorithm Parameter */
  def landmark = getDouble(LANDMARK_KEY)
  def setLandmark(value: Double) = setDouble(LANDMARK_KEY, value)

  def updateOp = "min"
  def setUpdateOp(value: String) = setString(UPDATE_OP_KEY, value)

}

object DolphinConfiguration {
  def newDefaultConf() = {
    new DolphinConfiguration()
  }
}
