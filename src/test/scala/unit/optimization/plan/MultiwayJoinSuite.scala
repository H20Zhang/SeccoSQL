package unit.optimization.plan

import org.apache.spark.secco.optimization.plan.{BinaryJoin, MultiwayJoin}
import util.SeccoFunSuite

class MultiwayJoinSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    createTestEmptyRelation("R1", "a", "b")()
    createTestEmptyRelation("R2", "b", "c", "d")()
    createTestEmptyRelation("R3", "a", "c", "e")()
  }

  test("basic") {
    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")

    val ds = R1.join(R2, "R1.b = R2.b").join(R3, "R2.c = R3.c and R3.a = R1.a")

    val plan = ds.queryExecution.analyzedPlan

    val join2 = plan.asInstanceOf[BinaryJoin]
    val join1 = join2.left.asInstanceOf[BinaryJoin]

    val multiwayJoin = MultiwayJoin(
      join1.children :+ join2.right,
      Seq(join1.condition.get, join2.condition.get)
    )

    println(multiwayJoin)
//    println(multiwayJoin.attributeOrder)
//    println(multiwayJoin.output)
//    println(multiwayJoin.attributeOrder)

  }

}
