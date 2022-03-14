package unit

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.types.{DataTypes, StructField, StructType}
import org.apache.spark.secco.util.misc.SparkSingle
import util.SeccoFunSuite

class SeccoSessionSuite extends SeccoFunSuite {

  test("create_dataset") {
    val sc = SparkSingle.getSparkContext()
    val dlSession = SeccoSession.currentSession
    val rows = Seq(InternalRow(0.0, 1.0), InternalRow(1.0, 1.0))
    val rdd = sc.parallelize(rows)

    val schema = StructType(
      Seq(
        StructField("a", DataTypes.DoubleType),
        StructField("b", DataTypes.DoubleType)
      )
    )

    val ds1 = dlSession.createDatasetFromSeq(rows, schema)
    val ds2 = dlSession.createDatasetFromRDD(rdd, schema)
    val ds3 = dlSession.createEmptyDataset(schema)

    println(ds1.queryExecution.logical)
    println(ds2.queryExecution.logical)
    println(ds3.queryExecution.logical)
  }

  test("operation") {
    val dlSession = SeccoSession.currentSession
    val rows = Seq(InternalRow(0.0, 1.0), InternalRow(1.0, 1.0))
    val schema = StructType(
      Seq(
        StructField("a", DataTypes.DoubleType),
        StructField("b", DataTypes.DoubleType)
      )
    )

    dlSession.createDatasetFromSeq(rows, schema).createOrReplaceTable("R")

    val ds1 = dlSession.table("R")

    println(ds1.logical)

  }

}
