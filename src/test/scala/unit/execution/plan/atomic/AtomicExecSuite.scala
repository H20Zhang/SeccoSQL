package unit.execution.plan.atomic

import org.apache.spark.secco.execution.RowBlock
import org.apache.spark.secco.execution.plan.atomic._
import org.apache.spark.secco.execution.plan.io.InMemoryScanExec
import util.{SeccoFunSuite, TestDataGenerator, UnitTestTag}

class AtomicExecSuite extends SeccoFunSuite {

  test("TransformExec", UnitTestTag) {
    val attrs = "A" :: "B" :: Nil
    val cardinality = 10
    val upperBound = 10000
    val scanExec = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
      "R1",
      attrs,
      cardinality,
      upperBound
    )
    val transformExec =
      TransformExec(scanExec, Seq("A", "0.85*B+0.001"), Seq("A", "weight"))

    val scanSeq = scanExec.collectSeq()
    val transformSeq = transformExec.collectSeq()

    val isAllEqual = scanSeq.zip(transformSeq).forall {
      case (t1, t2) =>
        (t1(0) == t2(0)) && (0.85 * t1(1) + 0.001 == t2(1))
    }

    assert(isAllEqual)
  }

  test("CacheExec", UnitTestTag) {
    val attrs = "A" :: "B" :: Nil
    val cardinality = 1000
    val upperBound = 10000
    val scanExec = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
      "R1",
      attrs,
      cardinality,
      upperBound
    )
    val cacheExec = CacheExec(scanExec)

    for (i <- 0 until 2) {
      pprint.pprintln(cacheExec.execute().count())
    }

  }

  test("AssignExec", UnitTestTag) {
    val attrs = "A" :: "B" :: Nil
    val cardinality = 1000
    val upperBound = 10000
    val scanExec = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
      "R1",
      attrs,
      cardinality,
      upperBound
    )
    val assignExec = AssignExec(scanExec, "R2")
    assignExec.execute()
    val scanExec2 = InMemoryScanExec("R2", attrs)
    pprint.pprintln(scanExec2.execute().count())

  }

  test("RenameExec", UnitTestTag) {
    val attrs = "A" :: "B" :: Nil
    val cardinality = 1000
    val upperBound = 10000
    val scanExec = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
      "R1",
      attrs,
      cardinality,
      upperBound
    )
    val renameExec = RenameExec(scanExec, Map("B" -> "A", "A" -> "B"))
    pprint.pprintln(renameExec.outputOld)
  }

  test("UpdateExec", UnitTestTag) {
    val attrs = "A" :: "B" :: Nil
    val cardinality = 10
    val upperBound = 20
    val scanExec = TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
      "R1",
      attrs,
      cardinality,
      upperBound
    )

    val updateExec = UpdateExec(scanExec, "Out", "Delta", Seq("A"))
    val deltaScanExec = InMemoryScanExec("Delta", attrs)

    updateExec.execute()
    for (i <- 0 until 3) {

      //gen new tuples for relation R1
      TestDataGenerator.genInMemoryScanExecWithRandomInternalRow(
        "R1",
        attrs,
        2,
        upperBound
      )

      pprint.pprintln(s"new input")
      pprint.pprintln(
        scanExec
          .execute()
          .flatMap(f => f.asInstanceOf[RowBlock].blockContent.content)
          .collect()
      )

      updateExec.execute()

      pprint.pprintln(s"Delta")
      pprint.pprintln(
        deltaScanExec
          .execute()
          .flatMap(f => f.asInstanceOf[RowBlock].blockContent.content)
          .collect()
      )
    }

    //gen final table
    updateExec.genFinalTable()

    val outScanExec = InMemoryScanExec("Out", attrs)

    pprint.pprintln(s"Out")
    pprint.pprintln(
      outScanExec
        .execute()
        .flatMap(f => f.asInstanceOf[RowBlock].blockContent.content)
        .collect()
    )

  }

}
