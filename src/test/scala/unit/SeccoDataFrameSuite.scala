package unit

import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.types.{DataTypes, StructField, StructType}
import org.apache.spark.secco.{DataFrame, SeccoSession}
import org.apache.spark.secco.util.misc.SparkSingle
import util.SeccoFunSuite

class DataFrameSuite extends SeccoFunSuite {

  test("create_dataset") {
    val sc = SparkSingle.getSparkContext()
    val seq = Seq(InternalRow(0.0, 1.0), InternalRow(1.0, 1.0))
    val rdd = sc.parallelize(seq)
    val schema = StructType(
      Seq(
        StructField("a", DataTypes.DoubleType),
        StructField("b", DataTypes.DoubleType)
      )
    )

    val ds1 = DataFrame.fromSeq(seq, schema)
    val ds2 = DataFrame.fromRDD(rdd, schema)
    val ds3 = DataFrame.empty(schema)

    println(ds1.queryExecution.logical)
    println(ds2.queryExecution.logical)
    println(ds3.queryExecution.logical)
  }

  test("dataset_operations") {}

}
