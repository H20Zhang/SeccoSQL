package org.apache.spark.secco.execution.sources

import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.{
  OldInternalBlock,
  RowBlockOld,
  RowBlockContent
}
import org.apache.spark.secco.util.misc.SparkSingle

class DataLoader(
    partitionSize: Int = SeccoConfiguration.newDefaultConf().numPartition
) {

  lazy val (_, sc) = SparkSingle.getSpark()
  lazy val spark = SparkSingle.getSparkSession()

  def csv(dataAddress: String, separator: String = ",") = {
    sc.textFile(dataAddress)
      .repartition(partitionSize)
      .map { f =>
        var res: Array[Double] = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split(separator)
          res = splittedString.map(_.toDouble)
        }
        res
      }
      .filter(f => f != null)
  }

  def loadTable(
      dataAddress: String,
      separator: String = ",",
      attributes: Seq[String]
  ) = {
    val csvRDD = csv(dataAddress, separator)
    csvRDD.mapPartitions { it =>
      val rowBlockContent = RowBlockContent(it.toArray)
      Iterator(
        RowBlockOld(attributes, rowBlockContent).asInstanceOf[OldInternalBlock]
      )
    }
  }
}

//map(f => (f(0), f(1)))
//.filter(f => f._1 != f._2)
//.flatMap(f => Iterator(f, f.swap))
//.distinct()
//.map(f => Array(f._1, f._2))
//
//relationRDD.cache()
//relationRDD.count()
