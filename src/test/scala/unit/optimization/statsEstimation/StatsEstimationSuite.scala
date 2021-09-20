package unit.optimization.statsEstimation

import org.apache.spark.secco.Dataset
import org.apache.spark.secco.optimization.plan.Relation
import util.{SeccoFunSuite, UnitTestTag}

import scala.util.Random

class StatsEstimationSuite extends SeccoFunSuite {

  test("check_computeStats", UnitTestTag) {

    val seq = Array.fill(100)(
      Array(Random.nextInt(100).toDouble, Random.nextInt(100).toDouble)
    )

    //drop existing R1 for cautious
    val catalog = dlSession.sessionState.catalog
    catalog.dropTable("R1")

    val ds1 =
      Dataset.fromSeq(seq, Some("R1"), Some("a" :: "b" :: Nil), dl = dlSession)
    val R1 = ds1.logical.asInstanceOf[Relation]
    val statisticOfR1 = R1.computeStats()

    pprint.pprintln(statisticOfR1)
  }

  test("check_estimateStats", UnitTestTag) {
    val seq1 = Array(
      Array(1, 2, 4),
      Array(2, 3, 4),
      Array(3, 4, 4),
      Array(4, 5, 4),
      Array(5, 6, 4),
      Array(6, 7, 4),
      Array(7, 8, 4)
    ).map(_.map(_.toDouble))

    //drop existing R1 and R2 for cautious.
    val catalog = dlSession.sessionState.catalog
    catalog.dropTable("R1")
    catalog.dropTable("R2")

    val ds1 =
      Dataset.fromSeq(
        seq1,
        Some("R1"),
        Some("a" :: "b" :: "c" :: Nil),
        dl = dlSession
      )
    val ds2 =
      Dataset.fromSeq(
        seq1,
        Some("R2"),
        Some("a" :: "d" :: "e" :: Nil),
        dl = dlSession
      )

    val ds3 = ds1.select("a < b")
    val ds4 = ds1.project("a, b")
    val ds5 = ds1.aggregate("sum(c) by a")
    val ds6 = ds1.naturalJoin(ds2)

    //check the stats of query plan is non-empty
    assert(ds1.queryExecution.logical.stats != null)
    assert(ds3.queryExecution.logical.stats != null)
    assert(ds4.queryExecution.logical.stats != null)
    assert(ds5.queryExecution.logical.stats != null)
    assert(ds6.queryExecution.logical.stats != null)
  }

  test("check_estimation_accuracy", UnitTestTag) {
    val pathPrefix = "./datasets/workload_exp_demo/W1/Low/"

    val catalog = dlSession.sessionState.catalog
    catalog.dropTable("R1")
    catalog.dropTable("R2")
    catalog.dropTable("R3")
    catalog.dropTable("R4")
    catalog.dropTable("R5")

    val R1 = Dataset.fromFile(
      pathPrefix + "R1",
      Some("R1"),
      Some(Seq("A", "B", "C", "D"))
    )
    val R2 =
      Dataset.fromFile(pathPrefix + "R2", Some("R2"), Some(Seq("A", "W1")))
    val R3 =
      Dataset.fromFile(pathPrefix + "R3", Some("R3"), Some(Seq("A", "W2")))
    val R4 =
      Dataset.fromFile(pathPrefix + "R4", Some("R4"), Some(Seq("A", "W3")))
    val R5 =
      Dataset.fromFile(pathPrefix + "R5", Some("R5"), Some(Seq("A", "W4")))

    val res = R1.naturalJoin(R2)

    pprint.pprintln(s"Stats of R1:")
    pprint.pprintln(R1.logical.stats.attributeStats("A"))

    pprint.pprintln(s"Stats of R2:")
    pprint.pprintln(R2.logical.stats.attributeStats("A"))

    pprint.pprintln(s"Stats of Res:")
    println(res.logical.stats)

  }

}
