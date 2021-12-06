package unit

import org.apache.spark.secco.{Dataset, SeccoSession}
import org.apache.spark.secco.util.misc.SparkSingle
import util.SeccoFunSuite

class DatasetSuite extends SeccoFunSuite {

  test("create_dataset") {
    val sc = SparkSingle.getSparkContext()
    val dlSession = SeccoSession.currentSession
    val seq = Seq(Array(0.0, 1.0), Array(1.0, 1.0))
    val rdd = sc.parallelize(seq)

    val ds1 = Dataset.fromSeq(seq)
    val ds2 = Dataset.fromRDD(rdd)
    val ds3 = Dataset.empty("T3", Seq("A", "B"))

    println(ds1.queryExecution.logical)
    println(ds2.queryExecution.logical)
    println(ds3.queryExecution.logical)
  }

  test("dataset_operations") {

    val V = Dataset.empty("V", "id" :: "vLabel" :: Nil)
    val E = Dataset.empty("E", "src" :: "dst" :: "eLabel" :: Nil)

    val G = E.toGraph(eLabel = Some("eLabel"))(V, vLabel = Some("vLabel"))
    val res = G.pattern("(a)-[]->(b)-[]->(c)")

    println(res.queryExecution.logical)

  }

}
