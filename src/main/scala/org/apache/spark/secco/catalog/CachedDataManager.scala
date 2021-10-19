package org.apache.spark.secco.catalog

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.execution.OldInternalBlock
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class CachedDataManager {

  private val store: mutable.HashMap[String, Any] = mutable.HashMap()

  def storeRelation(tableName: String, dataAddress: String): Unit = {
    store(tableName) = dataAddress
  }

  def storeRelation(tableName: String, data: RDD[OldInternalBlock]): Unit = {
    store.get(tableName) match {
      case Some(x) =>
        val oldBlock = x.asInstanceOf[RDD[OldInternalBlock]]
        data.persist(
          SeccoSession.currentSession.sessionState.conf.rddCacheLevel
        )
        data.count()
        store(tableName) = data

        oldBlock.unpersist(false)
      case None =>
        data.persist(
          SeccoSession.currentSession.sessionState.conf.rddCacheLevel
        )
        data.count()
        store(tableName) = data
    }

  }

  def apply(key: String): Option[Any] = store.get(key)

}

object CachedDataManager {

  lazy val defaultDataManager = newDefaultDataManager

  def newDefaultDataManager = new CachedDataManager

}
