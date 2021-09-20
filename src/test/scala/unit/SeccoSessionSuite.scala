package unit

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.util.misc.SparkSingle
import util.SeccoFunSuite

class SeccoSessionSuite extends SeccoFunSuite {

  test("create_dataset") {
    val sc = SparkSingle.getSparkContext()
    val dlSession = SeccoSession.currentSession
    val seq = Seq(Array(0.0, 1.0), Array(1.0, 1.0))
    val rdd = sc.parallelize(seq)

    val ds1 = dlSession.createDatasetFromSeq(seq)
    val ds2 = dlSession.createDatasetFromRDD(rdd)
    val ds3 = dlSession.createEmptyDataset("T3", Seq("A", "B"))

    println(ds1.queryExecution.logical)
    println(ds2.queryExecution.logical)
    println(ds3.queryExecution.logical)

  }

}
