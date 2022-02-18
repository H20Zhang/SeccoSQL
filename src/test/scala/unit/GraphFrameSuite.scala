package unit

import org.apache.spark.secco.GraphFrame.{EdgeMetaData, NodeMetaData}
import org.apache.spark.secco.{Dataset, GraphFrame, SeccoSession}
import org.apache.spark.secco.util.misc.SparkSingle
import util.SeccoFunSuite

class GraphFrameSuite extends SeccoFunSuite {

  test("create_graph") {

    clearSession()

    val V = Dataset.empty("V", "id" :: "vLabel" :: Nil)
    val E = Dataset.empty("E", "src" :: "dst" :: "eLabel" :: Nil)

    val graph = GraphFrame(
      V,
      NodeMetaData("id", Some("vLabel")),
      E,
      EdgeMetaData("src", "dst", Some("eLabel"))
    )
  }

  test("dataset_operations") {

    clearSession()

    val V = Dataset.empty("V", "id" :: "vLabel" :: Nil)
    val E = Dataset.empty("E", "src" :: "dst" :: "eLabel" :: Nil)

    val graph = GraphFrame(
      V,
      NodeMetaData("id", Some("vLabel")),
      E,
      EdgeMetaData("src", "dst", Some("eLabel"))
    )

    val patternDS = graph.pattern("(a)-[]->(b)-[]->(c)")
    println(patternDS.queryExecution.logical)

    val graphAnalyticDS =
      graph.messagePassing("state", "sum(state)", "newState")

    println(graphAnalyticDS.queryExecution.logical)

  }
}
