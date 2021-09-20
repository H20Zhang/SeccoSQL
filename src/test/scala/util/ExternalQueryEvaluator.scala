package util

import org.apache.spark.secco.execution.OldInternalDataType
import org.apache.spark.secco.util.misc.SparkSingle

object ExternalQueryEvaluator {

  val ExternalEvaluatorName = "SparkSQL"

  def validateNaturalJoinQuery(
      contents: Seq[Array[Array[OldInternalDataType]]],
      schemas: Seq[Seq[String]]
  ): Long = {

    val spark = SparkSingle.getSparkSession()
    val sc = SparkSingle.getSparkContext()

    def SparkSQLResult(): Long = {
      import spark.implicits._

      val schemasWithName = schemas.zipWithIndex.map(f => (f._1, s"R${f._2}"))
      schemasWithName.zip(contents).foreach {
        case ((schema, schemaName), content) =>
          val rdd = sc.parallelize(content)
          val df = schema.size match {
            case 1 => rdd.map(f => f(0)).toDF(schema: _*)
            case 2 => rdd.map(f => (f(0), f(1))).toDF(schema: _*)
            case 3 => rdd.map(f => (f(0), f(1), f(2))).toDF(schema: _*)
            case 4 => rdd.map(f => (f(0), f(1), f(2), f(3))).toDF(schema: _*)
            case 5 =>
              rdd.map(f => (f(0), f(1), f(2), f(3), f(4))).toDF(schema: _*)
          }

          df.createOrReplaceTempView(schemaName)
      }

      val naturalJoin = " natural join "

      val sqlQuery =
        s"""
           |select *
           |from ${schemasWithName
          .map(f => f._2 + naturalJoin)
          .reduce(_ + _)
          .dropRight(naturalJoin.size)}
           |""".stripMargin

      //    spark.catalog.listTables().show()
      spark.sql(sqlQuery).count()
    }

    SparkSQLResult()
  }

}
