package unit

import org.apache.spark.secco.SeccoGraphFrame.{EdgeMetaData, NodeMetaData}
import org.apache.spark.secco.types.{DataTypes, StructField, StructType}
import org.apache.spark.secco.{
  SeccoDataFrame,
  SeccoGraphFrame,
  SeccoSession,
  types
}
import org.apache.spark.secco.util.misc.SparkSingle
import util.SeccoFunSuite

class SeccoGraphFrameSuite extends SeccoFunSuite {

  test("create_graph") {

    clearSession()

    val nodeSchema = StructType(
      Seq(
        StructField("id", DataTypes.LongType),
        StructField("vLabel", DataTypes.IntegerType)
      )
    )

    val edgeSchema = StructType(
      Seq(
        StructField("src", DataTypes.LongType),
        StructField("dst", DataTypes.LongType),
        StructField("eLabel", DataTypes.IntegerType)
      )
    )

    val V = SeccoDataFrame.empty(nodeSchema).alias("V")
    val E = SeccoDataFrame.empty(edgeSchema).alias("E")

    val graph = SeccoGraphFrame(
      V,
      NodeMetaData("id", Some("vLabel")),
      E,
      EdgeMetaData("src", "dst", Some("eLabel"))
    )
  }

  test("dataset_operations") {

    clearSession()

    val nodeSchema = StructType(
      Seq(
        StructField("id", DataTypes.LongType),
        StructField("vLabel", DataTypes.IntegerType)
      )
    )

    val edgeSchema = StructType(
      Seq(
        StructField("src", DataTypes.LongType),
        StructField("dst", DataTypes.LongType),
        StructField("eLabel", DataTypes.IntegerType)
      )
    )

    val V = SeccoDataFrame.empty(nodeSchema).alias("V")
    val E = SeccoDataFrame.empty(edgeSchema).alias("E")

    val graph = SeccoGraphFrame(
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
