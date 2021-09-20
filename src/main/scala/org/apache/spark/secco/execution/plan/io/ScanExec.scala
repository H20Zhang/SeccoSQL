package org.apache.spark.secco.execution.plan.io

import org.apache.spark.secco.catalog.CachedDataManager
import org.apache.spark.secco.execution.{SeccoPlan, InternalBlock}
import org.apache.spark.secco.execution.sources.DataLoader
import org.apache.spark.rdd.RDD

trait ScanExec extends SeccoPlan {
  override final def children: Seq[SeccoPlan] = Nil
  val tableName: String
  val outputOld: Seq[String]
}

case class DiskScanExec(
    tableName: String,
    outputOld: Seq[String],
    dataAddress: String
) extends ScanExec {

  /**
    * Produces the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalBlock] = {
    val loader = new DataLoader()
    loader.loadTable(dataAddress, ",", outputOld)
  }

}

case class InMemoryScanExec(
    tableName: String,
    outputOld: Seq[String]
) extends ScanExec {

  /**
    * Produces the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[InternalBlock] = {
    dataManager(tableName) match {
      case Some(x) => x.asInstanceOf[RDD[InternalBlock]]
      case None =>
        throw new Exception(s"no such table:${tableName} in dataManager")
    }

  }

}
