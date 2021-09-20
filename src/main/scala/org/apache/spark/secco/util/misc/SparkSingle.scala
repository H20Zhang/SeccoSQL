package org.apache.spark.secco.util.misc

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Simple Class which wrap around SparkContext, and SparkSession for easy testing
  *
  * Note: The Default [[SeccoConfiguration]] is used in this class regardless of what [[SeccoConfiguration]] is used in
  *       [[SeccoSession]].
  */
object SparkSingle {

  private val conf = getConf()

  private def getConf() = {
    new SparkConf()
  }

  val dlConf = SeccoConfiguration.newDefaultConf()

  var isCluster = dlConf.isYarn
  var appName = "Secco"

  private def getSparkInternal() = {
    isCluster match {
      case true =>
        SparkSession
          .builder()
          .master("yarn")
          .config(getConf())
          .appName(appName)
          .config("spark.kryo.unsafe", "true")
          .config("spark.shuffle.file.buffer", "1M")
          .config("spark.network.timeout", "10000000")
          .config("spark.yarn.maxAppAttempts", "1")
          .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer"
          )
          .config(
            "spark.sql.shuffle.partitions",
            dlConf.numPartition
          )
          .getOrCreate()

      case false =>
        SparkSession
          .builder()
          .master(s"local[${dlConf.numCore}]")
          .config(getConf())
          .appName(appName)
          .config("spark.shuffle.file.buffer", "1M")
          .config("spark.kryo.unsafe", "true")
          .config("spark.network.timeout", "10000000")
          .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer"
          )
          .config(
            "spark.sql.shuffle.partitions",
            SeccoConfiguration.newDefaultConf().numPartition
          )
          .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
          .getOrCreate()
    }
  }

  private var spark: SparkSession = _
  private var sc: SparkContext = _

  var counter = 0

  def getSpark() = {
    spark = getSparkInternal()
    sc = spark.sparkContext

    (spark, sc)
  }

  def getSparkContext() = {
    getSpark()._2
  }

  def getSparkSession() = {
    getSpark()._1
  }

  def close(): Unit = {
    if (spark != null) {
      spark.close()
    }
  }

}
