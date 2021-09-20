package org.apache.spark.secco.config

import com.typesafe.config
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable

trait ConfigurationInterface {

  val externalConf: config.Config

  def set(key: String, value: String): Unit
  def setBoolean(key: String, value: Boolean): Unit
  def setInt(key: String, value: Int): Unit
  def setLong(key: String, value: Long): Unit
  def setFloat(key: String, value: Float): Unit
  def setDouble(key: String, value: Double): Unit
  def setString(key: String, value: String): Unit
  def setSizeAsKB(key: String, value: Long): Unit
  def setSizeAsMB(key: String, value: Long): Unit
  def setSizeAsGB(key: String, value: Long): Unit
  def setDurationAsSeconds(key: String, value: Long): Unit
  def setDurationAsMinutes(key: String, value: Long): Unit
  def setDurationAsHours(key: String, value: Long): Unit

  def get(key: String): String
  def getBoolean(key: String): Boolean
  def getInt(key: String): Int
  def getLong(key: String): Long
  def getFloat(key: String): Float
  def getDouble(key: String): Double
  def getString(key: String): String
  def getSizeAsKB(key: String): Long
  def getSizeAsMB(key: String): Long
  def getSizeAsGB(key: String): Long
  def getDurationAsSeconds(key: String): Long
  def getDurationAsMinutes(key: String): Long
  def getDurationAsHours(key: String): Long

  def contains(key: String): Boolean
}

class Configuration extends ConfigurationInterface {

  private val _internalStore = mutable.HashMap[String, String]()
  override val externalConf: Config = ConfigFactory.load

  def set(key: String, value: String): Unit = { // for those specified keys, if value is of incorrect data type, an exception will be thrown
    _internalStore.put(key, value)
  }

  def setBoolean(key: String, value: Boolean): Unit = set(key, value.toString)
  def setInt(key: String, value: Int): Unit = set(key, value.toString)
  def setLong(key: String, value: Long): Unit = set(key, value.toString)
  def setFloat(key: String, value: Float): Unit = set(key, value.toString)
  def setDouble(key: String, value: Double): Unit = set(key, value.toString)
  def setString(key: String, value: String): Unit = set(key, value)
  def setSizeAsKB(key: String, value: Long): Unit = setLong(key, value * 1000)
  def setSizeAsMB(key: String, value: Long): Unit =
    setLong(key, value * 1000 * 1000)
  def setSizeAsGB(key: String, value: Long): Unit =
    setLong(key, value * 1000 * 1000 * 1000)
  def setDurationAsSeconds(key: String, value: Long): Unit =
    setLong(key, value * 1000)
  def setDurationAsMinutes(key: String, value: Long): Unit =
    setLong(key, value * 60 * 1000)
  def setDurationAsHours(key: String, value: Long): Unit =
    setLong(key, value * 60 * 60 * 1000)

  override def get(key: String): String = {
    _internalStore.get(key) match {
      case Some(res) => res
      case None      => externalConf.getString(key)
    }
  }

  def getBoolean(key: String): Boolean = get(key).toBoolean

  def getInt(key: String): Int = get(key).toInt

  def getLong(key: String): Long = get(key).toLong

  def getFloat(key: String): Float = get(key).toFloat

  def getDouble(key: String): Double = get(key).toDouble

  def getString(key: String): String = get(key)

  def getSizeAsKB(key: String): Long = {
    _internalStore.get(key) match {
      case Some(res) => (res.toLong / 1e3).round
      case None      => (externalConf.getBytes(key) / 1e3).round
    }
  }

  def getSizeAsMB(key: String): Long = (getSizeAsKB(key) / 1e3).round
  def getSizeAsGB(key: String): Long = (getSizeAsKB(key) / (1e3 * 1e3)).round

  def getDurationAsSeconds(key: String): Long = {
    _internalStore.get(key) match {
      case Some(res) => (res.toLong / 1e3).round
      case None =>
        (externalConf.getDuration(
          key,
          java.util.concurrent.TimeUnit.MILLISECONDS
        ) / 1e3).round
    }
  }

  def getDurationAsMinutes(key: String): Long =
    (getDurationAsSeconds(key) / 6e1).round

  def getDurationAsHours(key: String): Long =
    (getDurationAsSeconds(key) / (6e1 * 6e1)).round

  def contains(key: String): Boolean =
    _internalStore.contains(key) || externalConf.hasPath(key)

}
