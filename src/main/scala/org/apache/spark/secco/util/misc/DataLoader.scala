package org.apache.spark.secco.util.misc

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.execution.OldInternalDataType
import org.apache.spark.secco.execution.sources.DataLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.sql.SparkSession

/** A class for loading the data for benchmark. */
object DataLoader {

  /** Load TSV file. */
  def loadTSV(path: String, separator: String = "\\s"): RDD[InternalRow] = {
    val dlSession = SeccoSession.currentSession
    val sc = SparkSingle.getSparkContext()
    val fileRDD = sc.textFile(path)
    val csvRDD = fileRDD
      .map(f =>
        f.startsWith("#") match {
          case false =>
            try {
              Some(InternalRow(f.split(separator).map(_.toDouble)))
            } catch {
              case _: Throwable => None
            }
          case true => None
        }
      )
      .repartition(dlSession.sessionState.conf.numPartition)
      .filter(_.nonEmpty)
      .map(_.get)
      .persist(dlSession.sessionState.conf.rddCacheLevel)

    csvRDD.count()

    csvRDD
  }

  /** Load the graph file. */
  def loadGraph(
      spark: SparkSession,
      graphPath: String,
      isUndirected: Boolean = true
  ): RDD[Array[OldInternalDataType]] = {

    val dlSession = SeccoSession.currentSession
    val loader = new DataLoader()

    val edge = isUndirected match {
      case true =>
        loader
          .csv(graphPath, "\\s")
          .filter(f => f != null)
          .map(f => (f(0), f(1)))
          .filter(f => f._1 != f._2)
          .flatMap(f => Iterator(f, f.swap))
          .distinct()
          .repartition(dlSession.sessionState.conf.numPartition)
          .map(f => Array(f._1, f._2))
          .persist(dlSession.sessionState.conf.rddCacheLevel)
      case false =>
        loader
          .csv(graphPath, "\\s")
          .filter(f => f != null)
          .map(f => (f(0), f(1)))
          .repartition(dlSession.sessionState.conf.numPartition)
          .map(f => Array(f._1, f._2))
          .persist(dlSession.sessionState.conf.rddCacheLevel)
    }

    edge.count()

    edge
  }
}
