package util

import org.apache.spark.secco.{SeccoDataFrame}
import org.apache.spark.secco.util.misc.SparkSingle

object TestData {

  val prefix = "./datasets"

  def loadUndirectedGraphEdge(input: String) = {

    val spark = SparkSingle.getSparkSession()
    val df = spark.read.csv(input, "\\s")
    assert(df.schema.fields.length == 2, "graph should only have two columns.")

    val diGraph = df.toDF("src", "dst")
    val undirectedGraph =
      diGraph.union(df.select("dst", "src")).filter("src != dst").cache()

    val seccoDf = SeccoDataFrame.fromSparkSQL(undirectedGraph)

    seccoDf
  }

  def data(dataName: String) =
    dataName match {
      case "eu"    => loadUndirectedGraphEdge(s"$prefix/eu.txt")
      case "wiki"  => loadUndirectedGraphEdge(s"$prefix/wikiV.txt")
      case "debug" => loadUndirectedGraphEdge(s"$prefix/debug.txt")
    }

}
