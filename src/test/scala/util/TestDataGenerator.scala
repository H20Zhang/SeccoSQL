package util

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.CachedDataManager
import org.apache.spark.secco.optimization.plan.Relation
import org.apache.spark.secco.execution.plan.io.InMemoryScanExec
import org.apache.spark.secco.execution.{
  InternalBlock,
  OldInternalRow,
  RowBlock,
  RowBlockContent
}
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.util.misc.SparkSingle
import org.apache.spark.rdd.RDD

import scala.util.Random

object TestDataGenerator {

  lazy val sc = SparkSingle.getSparkContext()

  def genRandomInternalRow(
      cardinality: Int,
      arity: Int,
      upperBound: Int
  ): Array[OldInternalRow] = {
    Range(0, cardinality)
      .map(f =>
        Range(0, arity)
          .map(f => Random.nextInt(upperBound).toDouble)
          .toArray
      )
      .toArray
  }

  def genRandomInternalRowRDD(
      cardinality: Int,
      arity: Int,
      upperBound: Int
  ): RDD[OldInternalRow] = {
    sc.parallelize(genRandomInternalRow(cardinality, arity, upperBound))
  }

  def genRandomRowBlockRDD(
      attrs: Seq[String],
      cardinality: Int,
      upperBound: Int
  ): RDD[InternalBlock] = {

    val arity = attrs.size
    val rowRDD = genRandomInternalRowRDD(cardinality, arity, upperBound)
    rowRDD.mapPartitions { it =>
      val blockContent = RowBlockContent(it.toArray)
      val rowBlock =
        RowBlock(attrs, blockContent).asInstanceOf[InternalBlock]
      Iterator(rowBlock)
    }
  }

  def genInMemoryScanExecWithRandomInternalRow(
      tableIdentifier: String,
      attrs: Seq[String],
      cardinality: Int,
      upperBound: Int
  ): InMemoryScanExec = {

    val dataManager =
      SeccoSession.currentSession.sessionState.cachedDataManager
    dataManager.storeRelation(
      tableIdentifier,
      TestDataGenerator.genRandomRowBlockRDD(attrs, cardinality, upperBound)
    )
    InMemoryScanExec(
      tableIdentifier,
      attrs
    )

  }

  def genRandomData(
      plan: LogicalPlan,
      cardinality: Int,
      upperBound: Int,
      replaceExistingData: Boolean = false
  ): LogicalPlan = {

    val dataManager =
      SeccoSession.currentSession.sessionState.cachedDataManager

    plan.foreach { childPlan =>
      childPlan match {
        case s: Relation =>
          if (replaceExistingData || dataManager(s.tableIdentifier).isEmpty) {
            genInMemoryScanExecWithRandomInternalRow(
              s.tableIdentifier,
              s.outputOld,
              cardinality,
              upperBound
            )
          }
        case _ =>
      }
    }

    plan
  }

  def assignDataForScan(
      scan: Relation,
      rows: Array[OldInternalRow]
  ): LogicalPlan = {
    val dataManager =
      SeccoSession.currentSession.sessionState.cachedDataManager
    val rdd = sc
      .parallelize(rows)
      .mapPartitions { it =>
        val blockContent = RowBlockContent(it.toArray)
        val rowBlock =
          RowBlock(scan.outputOld, blockContent).asInstanceOf[InternalBlock]
        Iterator(rowBlock)
      }
      .cache()

    rdd.count()
    dataManager.storeRelation(scan.tableIdentifier, rdd)

    scan
  }

  def assignDataForScan(
      scan: Relation,
      rowRDD: RDD[OldInternalRow]
  ): LogicalPlan = {
    val dataManager =
      SeccoSession.currentSession.sessionState.cachedDataManager
    val rdd = rowRDD
      .mapPartitions { it =>
        val blockContent = RowBlockContent(it.toArray)
        val rowBlock =
          RowBlock(scan.outputOld, blockContent).asInstanceOf[InternalBlock]
        Iterator(rowBlock)
      }
      .cache()

    rdd.count()
    dataManager.storeRelation(scan.tableIdentifier, rdd)

    scan
  }

}
