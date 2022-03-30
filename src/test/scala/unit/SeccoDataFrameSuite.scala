package unit

import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  BinaryJoin,
  Distinct,
  Except,
  Filter,
  Intersection,
  Limit,
  LocalRows,
  Project,
  RDDRows,
  SubqueryAlias,
  Union
}
import org.apache.spark.secco.types.{DataTypes, StructField, StructType}
import org.apache.spark.secco.{SeccoDataFrame, SeccoSession}
import org.apache.spark.secco.util.misc.SparkSingle
import util.SeccoFunSuite

class SeccoDataFrameSuite extends SeccoFunSuite {

  test("create") {
    val sc = SparkSingle.getSparkContext()
    val seq = Seq(InternalRow(0.0, 1.0), InternalRow(1.0, 1.0))
    val rdd = sc.parallelize(seq)
    val schema = StructType(
      Seq(
        StructField("a", DataTypes.DoubleType),
        StructField("b", DataTypes.DoubleType)
      )
    )

    val ds1 = SeccoDataFrame.fromSeq(seq, schema)
    val ds2 = SeccoDataFrame.fromRDD(rdd, schema)
    val ds3 = SeccoDataFrame.empty(schema)

    ds1.createOrReplaceTable("R1")
    ds2.createOrReplaceTable("R2")
    ds3.createOrReplaceTable("R3")

    assert(ds1.logical.isInstanceOf[LocalRows])
    assert(ds2.logical.isInstanceOf[RDDRows])
    assert(ds3.logical.isInstanceOf[LocalRows])

    val ds4 = seccoSession.table("R1")
    val ds5 = seccoSession.table("R2")
    val ds6 = seccoSession.table("R3")

    assert(ds4.logical.isInstanceOf[SubqueryAlias])
    assert(ds5.logical.isInstanceOf[SubqueryAlias])
    assert(ds6.logical.isInstanceOf[SubqueryAlias])
  }

  test("sql") {
    val ds1 = seccoSession.table("R1")

    val ds2 = ds1.select("a < b")
    val ds3 = ds2.project("a")
    val ds4 = ds3.join(ds2.alias("R2"), "R2.a = R1.a")
    val ds5 = ds4.aggregate(Seq("sum(b)"), Seq("R1.a"))
    val ds6 = ds1.unionAll(ds2)
    val ds7 = ds1.difference(ds2)
    val ds8 = ds1.intersection(ds2)
    val ds9 = ds7.distinct()
    val ds10 = ds9.limit(10)

    assert(ds2.logical.isInstanceOf[Filter])
    assert(ds3.logical.isInstanceOf[Project])
    assert(ds4.logical.isInstanceOf[BinaryJoin])
    assert(ds5.logical.isInstanceOf[Aggregate])
    assert(ds6.logical.isInstanceOf[Union])
    assert(ds7.logical.isInstanceOf[Except])
    assert(ds8.logical.isInstanceOf[Intersection])
    assert(ds9.logical.isInstanceOf[Distinct])
    assert(ds10.logical.isInstanceOf[Limit])
  }

  //TODO: test the actions in SeccoDataframe.
  test("action") {}

  //TODO: test transformation in SeccoDataframe.
  test("transformation") {}

}
