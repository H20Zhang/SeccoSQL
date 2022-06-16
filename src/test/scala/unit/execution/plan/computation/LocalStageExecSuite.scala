package unit.execution.plan.computation

import org.apache.spark.secco.debug
import util.SeccoFunSuite

class LocalStageExecSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {

    // By setting numbers of core for executing the task and numbers of partition to 1.
    // We can make communication transparent, thus, we can focus on testing local computation.
    conf.setNumCore(1)
    conf.setNumPartition(1)

    createTestRelation(
      Seq(Seq(1, 2), Seq(1, 2)),
      "R1",
      "a",
      "b"
    )()
    createTestRelation(Seq(Seq(2, 3)), "R2", "b", "c")()
    createTestRelation(Seq(Seq(3, 4)), "R3", "c", "d")()
    createTestRelation(Seq(Seq(4, 5)), "R4", "d", "e")(
      "d"
    )
  }

  test("simple") {

    val sql1 =
      s"""
         |select a
         |from R1
         |
         |""".stripMargin

    val sql2 =
      s"""
         |select *
         |from R1
         |where a != 1
         |
         |""".stripMargin

    val sql3 =
      s"""
         |select *
         |from R1, R2
         |where R1.b = R2.b
         |
         |""".stripMargin

    val sql4 =
      s"""
         |select *
         |from R1, R2, R3
         |where R1.b = R2.b and R2.c = R3.c
         |
         |""".stripMargin

    val sql5 =
      s"""
         |select R1.a
         |from R1, R2
         |where R1.b = R2.b
         |
         |""".stripMargin

    val ds1 = seccoSession.sql(sql1)
    val ds2 = seccoSession.sql(sql2)
    val ds3 = seccoSession.sql(sql3)
    val ds4 = seccoSession.sql(sql4)
    val ds5 = seccoSession.sql(sql5)

    ds5.explain()
    ds5.show(100)

  }

}
