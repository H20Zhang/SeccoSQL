package org.apache.spark.secco.util.counter

import scala.collection.mutable

/** The manager that manages the counter. */
class CounterManager {

  private val defaultScope: String = ""
  private val counterMap: mutable.HashMap[(String, String), Counter] =
    mutable.HashMap()

  /** Get all counters under given `scope` */
  def getCounters(scope: String): Seq[Counter] = {
    counterMap.filterKeys(_._1 == scope).values.toSeq
  }

  /** Get or create an counter with given `scope` and `name`. */
  def getOrCreateCounter(scope: String, name: String): Counter = {
    counterMap.get((scope, name)) match {
      case Some(counter) =>
        assert(counter.scope == scope)
        assert(counter.name == name)
        counter
      case None =>
        counterMap((scope, name)) = Counter(scope, name)
        getOrCreateCounter(scope, name)
    }
  }

  /** Get or create an counter with defaultScope and `name`. */
  def getOrCreateCounter(name: String): Counter = {
    getOrCreateCounter(defaultScope, name)
  }

  /** Reset the counter with `scope` and `name`. */
  def resetCounter(scope: String, name: String): Unit = {
    counterMap.get((scope, name)) match {
      case Some(counter) =>
        assert(counter.scope == scope)
        assert(counter.name == name)
        counter.reset()
      case None =>
        throw new Exception(
          s"there is no counter with scope:${scope} and name:${name} for resetting."
        )
    }
  }

  /** Reset the counter with defaultScope and `name` */
  def resetCounter(name: String): Unit = {
    resetCounter(defaultScope, name)
  }
}

object CounterManager {

  lazy val globalCounterManager: CounterManager = new CounterManager

  def newDefaultCounterManager: CounterManager = new CounterManager
}
