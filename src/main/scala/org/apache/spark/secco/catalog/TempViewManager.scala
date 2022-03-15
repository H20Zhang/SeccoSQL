package org.apache.spark.secco.catalog

import org.apache.spark.secco.SeccoSession
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.analysis.ViewAlreadyExistsException
import org.apache.spark.secco.execution.plan.communication.InternalPartition
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.optimization.plan.SubqueryAlias

import scala.collection.mutable

/** The manager that manages the views in the current database. */
class TempViewManager {

  private val _store: mutable.HashMap[String, LogicalPlan] = mutable.HashMap()

  /** Register a new view, throw [[ViewAlreadyExistsException]] if view already exists. */
  def registerView(viewName: String, logicalPlan: LogicalPlan) = {
    if (_store.contains(viewName)) {
      throw new ViewAlreadyExistsException(viewName, "")
    } else {
      _store(viewName) = SubqueryAlias(logicalPlan, viewName)
    }
  }

  /** Drop a view, if there is no such view returns false. */
  def dropView(viewName: String): Boolean = {
    _store.remove(viewName).isEmpty
  }

  /** Create a new view, throw [[ViewAlreadyExistsException]] if view already exists. */
  def createView(viewName: String, logicalPlan: LogicalPlan) =
    registerView(viewName, logicalPlan)

  /** Create or replace a view */
  def createOrReplaceView(viewName: String, logicalPlan: LogicalPlan) = {
    try {
      createView(viewName, logicalPlan)
    } catch {
      case v: ViewAlreadyExistsException =>
        dropView(viewName)
        createView(viewName, logicalPlan)
    }
  }

  /** Get the view */
  def getView(viewName: String): Option[LogicalPlan] = _store.get(viewName)

  def apply(key: String): Option[LogicalPlan] = getView(key)
}

object TempViewManager {

  lazy val defaultDataManager = newDefaultDataManager

  def newDefaultDataManager = new TempViewManager

}
