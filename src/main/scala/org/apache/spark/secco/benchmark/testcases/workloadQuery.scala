package org.apache.spark.secco.benchmark.testcases

import org.apache.spark.secco.Dataset
import org.apache.spark.secco.benchmark.{SQLBenchmark}
import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.plan.{MultiwayNaturalJoin, JoinType}
import org.apache.spark.secco.optimization.statsEstimation.exact.ExactLogicalPlanEstimation

abstract class ControlledCardinalityEstimationSQLBenchmark
    extends SQLBenchmark {

  override protected def preprocess(
      inputData: Map[String, Dataset]
  ): Map[String, Dataset] = {

    val conf = dlSession.sessionState.conf

    if (conf.estimator == "Exact") {
      configureCardinalityEstimation(inputData)
      inputData
    } else {
      super.preprocess(inputData)
    }
  }

  /** Configure the exact cardinality estimation.
    * Note that it is to be override by concrete subclass.
    */
  protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit

}

/* == Workload Experiment Queries == */
/** Star schema workload. */
object W1 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(
          CatalogColumn("A"),
          CatalogColumn("B"),
          CatalogColumn("C"),
          CatalogColumn("D")
        )
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("A"), CatalogColumn("W1"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("B"), CatalogColumn("W2"))
      ) -> "R3",
      CatalogTable(
        "R4",
        Seq(CatalogColumn("C"), CatalogColumn("W3"))
      ) -> "R4",
      CatalogTable(
        "R5",
        Seq(CatalogColumn("D"), CatalogColumn("W4"))
      ) -> "R5"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")
    val R4 = inputData("R4")
    val R5 = inputData("R5")

    val res = R1.naturalJoin(R2).naturalJoin(R3).naturalJoin(R4).naturalJoin(R5)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R4", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R5", math.pow(2, 24).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 22).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 26).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 30).toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 20).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 16).toLong
        )
      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 28).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 32).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 36).toLong
        )
    }
  }

}

/** Snowflake join schema. */
object W2 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(CatalogColumn("A"), CatalogColumn("B"), CatalogColumn("C"))
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("A"), CatalogColumn("W1"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("B"), CatalogColumn("W2"))
      ) -> "R3",
      CatalogTable(
        "R4",
        Seq(CatalogColumn("C"), CatalogColumn("K3"))
      ) -> "R4",
      CatalogTable(
        "R5",
        Seq(CatalogColumn("K3"), CatalogColumn("W3"))
      ) -> "R5"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")
    val R4 = inputData("R4")
    val R5 = inputData("R5")

    val res = R1.naturalJoin(R2).naturalJoin(R3).naturalJoin(R4).naturalJoin(R5)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R4", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R5", math.pow(2, 24).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 22).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 26).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 30).toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 20).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 16).toLong
        )
      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 28).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 32).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 36).toLong
        )
    }
  }
}

/** line join schema. */
object W3 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(CatalogColumn("A"), CatalogColumn("B"))
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("B"), CatalogColumn("C"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("C"), CatalogColumn("D"))
      ) -> "R3",
      CatalogTable(
        "R4",
        Seq(CatalogColumn("D"), CatalogColumn("E"))
      ) -> "R4",
      CatalogTable(
        "R5",
        Seq(CatalogColumn("E"), CatalogColumn("F"))
      ) -> "R5"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")
    val R4 = inputData("R4")
    val R5 = inputData("R5")

    val res = R1.naturalJoin(R2).naturalJoin(R3).naturalJoin(R4).naturalJoin(R5)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R4", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R5", math.pow(2, 24).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 22).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 26).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 30).toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 20).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 16).toLong
        )
      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 28).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 32).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 36).toLong
        )
    }
  }
}

/** tree join schema. */
object W4 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(CatalogColumn("A"), CatalogColumn("B"))
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("A"), CatalogColumn("C"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("A"), CatalogColumn("D"))
      ) -> "R3",
      CatalogTable(
        "R4",
        Seq(CatalogColumn("B"), CatalogColumn("E"))
      ) -> "R4",
      CatalogTable(
        "R5",
        Seq(CatalogColumn("C"), CatalogColumn("F"))
      ) -> "R5"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")
    val R4 = inputData("R4")
    val R5 = inputData("R5")

    val res = R1.naturalJoin(R2).naturalJoin(R3).naturalJoin(R4).naturalJoin(R5)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R4", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R5", math.pow(2, 24).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 22).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 26).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 30).toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 20).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 16).toLong
        )
      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 28).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 32).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 36).toLong
        )
    }
  }
}

/** cycle join schema. */
object W5 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(CatalogColumn("A"), CatalogColumn("B"))
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("B"), CatalogColumn("C"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("C"), CatalogColumn("D"))
      ) -> "R3",
      CatalogTable(
        "R4",
        Seq(CatalogColumn("D"), CatalogColumn("E"))
      ) -> "R4",
      CatalogTable(
        "R5",
        Seq(CatalogColumn("E"), CatalogColumn("A"))
      ) -> "R5"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")
    val R4 = inputData("R4")
    val R5 = inputData("R5")

    val res = R1.naturalJoin(R2).naturalJoin(R3).naturalJoin(R4).naturalJoin(R5)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R4", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R5", math.pow(2, 24).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 22).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 26).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 30).toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 20).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 16).toLong
        )
      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 28).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 32).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(⋈(R1,R2),R3),R4)",
          math.pow(2, 36).toLong
        )
    }
  }
}

/** theta-join schema. */
object W6 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(CatalogColumn("A"), CatalogColumn("B"))
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("C"), CatalogColumn("D"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("E"), CatalogColumn("F"))
      ) -> "R3"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")

    val res = R1.thetaJoin("B < C", R2).thetaJoin("D < E", R3)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 18).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 18).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 18).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(R1,R2)",
          math.pow(2, 36).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[B<C](⨉(R1,R2))",
          math.pow(2, 6).toLong * 120L * math.pow(2, 3).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(\uD835\uDF0E[B<C](⨉(R1,R2)),R3)",
          math.pow(2, 6).toLong * 120L * math.pow(2, 3).toLong * math
            .pow(2, 18)
            .toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[D<E](⨉(\uD835\uDF0E[B<C](⨉(R1,R2)),R3))",
          math.pow(2, 6).toLong * 120L * 28L * math.pow(2, 6).toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(R1,R2)",
          math.pow(2, 36).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[B<C](⨉(R1,R2))",
          math.pow(2, 6).toLong * 120L * math.pow(2, 3).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(\uD835\uDF0E[B<C](⨉(R1,R2)),R3)",
          math.pow(2, 6).toLong * 120L * math.pow(2, 3).toLong * math
            .pow(2, 18)
            .toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[D<E](⨉(\uD835\uDF0E[B<C](⨉(R1,R2)),R3))",
          math.pow(2, 6).toLong * 120L * 1L * math.pow(2, 1).toLong
        )
      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(R1,R2)",
          math.pow(2, 36).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[B<C](⨉(R1,R2))",
          math.pow(2, 6).toLong * 120L * math.pow(2, 13).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(\uD835\uDF0E[B<C](⨉(R1,R2)),R3)",
          math.pow(2, 6).toLong * 120L * math.pow(2, 13).toLong * math
            .pow(2, 18)
            .toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[D<E](⨉(\uD835\uDF0E[B<C](⨉(R1,R2)),R3))",
          math.pow(2, 6).toLong * 120L * 120L * math.pow(2, 14).toLong
        )
    }
  }
}

/** theta-join + project schema. */
object W7 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(CatalogColumn("A"), CatalogColumn("B"))
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("C"), CatalogColumn("D"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("E"), CatalogColumn("F"))
      ) -> "R3"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")

    val res = R1.thetaJoin("B < C", R2).project("A, D").thetaJoin("D < E", R3)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 18).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 18).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 18).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(R1,R2)",
          math.pow(2, 36).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[B<C](⨉(R1,R2))",
          math.pow(2, 6).toLong * 6 * math.pow(2, 7).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "∏[A,D](\uD835\uDF0E[B<C](⨉(R1,R2)))",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(∏[A,D](\uD835\uDF0E[B<C](⨉(R1,R2))),R3)",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong * math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[D<E](⨉(∏[A,D](\uD835\uDF0E[B<C](⨉(R1,R2))),R3))",
          math.pow(2, 6).toLong * 120 * math
            .pow(2, 8)
            .toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(R1,R2)",
          math.pow(2, 36).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[B<C](⨉(R1,R2))",
          math.pow(2, 6).toLong * 6 * math.pow(2, 7).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "∏[A,D](\uD835\uDF0E[B<C](⨉(R1,R2)))",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(∏[A,D](\uD835\uDF0E[B<C](⨉(R1,R2))),R3)",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong * math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[D<E](⨉(∏[A,D](\uD835\uDF0E[B<C](⨉(R1,R2))),R3))",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong * 28 * math
            .pow(2, 1)
            .toLong
        )

      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(R1,R2)",
          math.pow(2, 36).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[B<C](⨉(R1,R2))",
          math.pow(2, 9).toLong * 6 * math.pow(2, 14).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "∏[A,D](\uD835\uDF0E[B<C](⨉(R1,R2)))",
          math.pow(2, 9).toLong * math.pow(2, 14).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(∏[A,D](\uD835\uDF0E[B<C](⨉(R1,R2))),R3)",
          math.pow(2, 9).toLong * math.pow(2, 14).toLong * math
            .pow(2, 18)
            .toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[D<E](⨉(∏[A,D](\uD835\uDF0E[B<C](⨉(R1,R2))),R3))",
          math.pow(2, 9).toLong * 2016 * math
            .pow(2, 11)
            .toLong
        )
    }
  }
}

/** theta-join + aggregate schema. */
object W8 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(CatalogColumn("A"), CatalogColumn("B"))
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("C"), CatalogColumn("D"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("E"), CatalogColumn("F"))
      ) -> "R3"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")

    val res = R1
      .thetaJoin("B < C", R2)
      .aggregate("count(*) by A,D")
      .thetaJoin("D < E", R3)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 18).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 18).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 18).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(R1,R2)",
          math.pow(2, 36).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[B<C](⨉(R1,R2))",
          math.pow(2, 6).toLong * 6 * math.pow(2, 7).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "[A,D]\uD835\uDF6A[count(*)](\uD835\uDF0E[B<C](⨉(R1,R2)))",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⨉([A,D]\uD835\uDF6A[count(*)](\uD835\uDF0E[B<C](⨉(R1,R2))),R3)",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong * math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[D<E](⨉([A,D]\uD835\uDF6A[count(*)](\uD835\uDF0E[B<C](⨉(R1,R2))),R3))",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong * 120 * math
            .pow(2, 8)
            .toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(R1,R2)",
          math.pow(2, 36).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[B<C](⨉(R1,R2))",
          math.pow(2, 6).toLong * 6 * math.pow(2, 7).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "[A,D]\uD835\uDF6A[count(*)](\uD835\uDF0E[B<C](⨉(R1,R2)))",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⨉([A,D]\uD835\uDF6A[count(*)](\uD835\uDF0E[B<C](⨉(R1,R2))),R3)",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong * math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[D<E](⨉([A,D]\uD835\uDF6A[count(*)](\uD835\uDF0E[B<C](⨉(R1,R2))),R3))",
          math.pow(2, 6).toLong * math.pow(2, 7).toLong * 120 * math
            .pow(2, 8)
            .toLong
        )

      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⨉(R1,R2)",
          math.pow(2, 36).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[B<C](⨉(R1,R2))",
          math.pow(2, 9).toLong * 6 * math.pow(2, 14).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "[A,D]\uD835\uDF6A[count(*)](\uD835\uDF0E[B<C](⨉(R1,R2)))",
          math.pow(2, 9).toLong * math.pow(2, 14).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⨉([A,D]\uD835\uDF6A[count(*)](\uD835\uDF0E[B<C](⨉(R1,R2))),R3)",
          math.pow(2, 9).toLong * math.pow(2, 14).toLong * math
            .pow(2, 18)
            .toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "\uD835\uDF0E[D<E](⨉([A,D]\uD835\uDF6A[count(*)](\uD835\uDF0E[B<C](⨉(R1,R2))),R3))",
          math.pow(2, 9).toLong * math.pow(2, 14).toLong * 2016 * math
            .pow(2, 11)
            .toLong
        )
    }
  }
}

/** line join + project schema. */
object W9 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(CatalogColumn("A"), CatalogColumn("B"))
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("B"), CatalogColumn("C"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("C"), CatalogColumn("D"))
      ) -> "R3",
      CatalogTable(
        "R4",
        Seq(CatalogColumn("D"), CatalogColumn("E"))
      ) -> "R4",
      CatalogTable(
        "R5",
        Seq(CatalogColumn("E"), CatalogColumn("F"))
      ) -> "R5"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")
    val R4 = inputData("R4")
    val R5 = inputData("R5")

    val res = R1
      .naturalJoin(R2)
      .naturalJoin(R3)
      .project("A, D")
      .naturalJoin(R4)
      .naturalJoin(R5)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R4", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R5", math.pow(2, 24).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 22).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 26).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "∏[A,D](⋈(⋈(R1,R2),R3))",
          math.pow(2, 24).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(∏[A,D](⋈(⋈(R1,R2),R3)),R4)",
          math.pow(2, 28).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(∏[A,D](⋈(⋈(R1,R2),R3)),R4),R5)",
          math.pow(2, 32).toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 22).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 20).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "∏[A,D](⋈(⋈(R1,R2),R3))",
          math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(∏[A,D](⋈(⋈(R1,R2),R3)),R4)",
          math.pow(2, 16).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(∏[A,D](⋈(⋈(R1,R2),R3)),R4),R5)",
          math.pow(2, 14).toLong
        )
      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 28).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 32).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "∏[A,D](⋈(⋈(R1,R2),R3))",
          math.pow(2, 30).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(∏[A,D](⋈(⋈(R1,R2),R3)),R4)",
          math.pow(2, 34).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(∏[A,D](⋈(⋈(R1,R2),R3)),R4),R5)",
          math.pow(2, 38).toLong
        )
    }
  }
}

/** line join + aggregate schema. */
object W10 extends ControlledCardinalityEstimationSQLBenchmark {

  override def relationsWithAddress: Map[CatalogTable, String] =
    Map(
      CatalogTable(
        "R1",
        Seq(CatalogColumn("A"), CatalogColumn("B"))
      ) -> "R1",
      CatalogTable(
        "R2",
        Seq(CatalogColumn("B"), CatalogColumn("C"))
      ) -> "R2",
      CatalogTable(
        "R3",
        Seq(CatalogColumn("C"), CatalogColumn("D"))
      ) -> "R3",
      CatalogTable(
        "R4",
        Seq(CatalogColumn("D"), CatalogColumn("E"))
      ) -> "R4",
      CatalogTable(
        "R5",
        Seq(CatalogColumn("E"), CatalogColumn("F"))
      ) -> "R5"
    )

  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
    val R1 = inputData("R1")
    val R2 = inputData("R2")
    val R3 = inputData("R3")
    val R4 = inputData("R4")
    val R5 = inputData("R5")

    val res = R1
      .naturalJoin(R2)
      .naturalJoin(R3)
      .aggregate("count(*) by A, D")
      .naturalJoin(R4)
      .naturalJoin(R5)
    val plan = res.logical transform { case j: MultiwayNaturalJoin =>
      j.copy(joinType = JoinType.GHDFKFK)
    }

    Dataset(res.seccoSession, plan)
  }

  override protected def configureCardinalityEstimation(
      inputData: Map[String, Dataset]
  ): Unit = {

    ExactLogicalPlanEstimation.setCardinality("R1", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R2", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R3", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R4", math.pow(2, 24).toLong)
    ExactLogicalPlanEstimation.setCardinality("R5", math.pow(2, 24).toLong)

    _inputPath.get match {
      case x: String if x.contains("LowHigh") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 22).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 26).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "[A,D]\uD835\uDF6A[count(*)](⋈(⋈(R1,R2),R3))",
          math.pow(2, 24).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈([A,D]\uD835\uDF6A[count(*)](⋈(⋈(R1,R2),R3)),R4)",
          math.pow(2, 28).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈([A,D]\uD835\uDF6A[count(*)](⋈(⋈(R1,R2),R3)),R4),R5)",
          math.pow(2, 32).toLong
        )

      case x: String if x.contains("Low") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 22).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 20).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "[A,D]\uD835\uDF6A[count(*)](⋈(⋈(R1,R2),R3))",
          math.pow(2, 18).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈([A,D]\uD835\uDF6A[count(*)](⋈(⋈(R1,R2),R3)),R4)",
          math.pow(2, 16).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈([A,D]\uD835\uDF6A[count(*)](⋈(⋈(R1,R2),R3)),R4),R5)",
          math.pow(2, 14).toLong
        )
      case x: String if x.contains("High") =>
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(R1,R2)",
          math.pow(2, 28).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈(R1,R2),R3)",
          math.pow(2, 32).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "[A,D]\uD835\uDF6A[count(*)](⋈(⋈(R1,R2),R3))",
          math.pow(2, 30).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈([A,D]\uD835\uDF6A[count(*)](⋈(⋈(R1,R2),R3)),R4)",
          math.pow(2, 34).toLong
        )
        ExactLogicalPlanEstimation.setCardinality(
          "⋈(⋈([A,D]\uD835\uDF6A[count(*)](⋈(⋈(R1,R2),R3)),R4),R5)",
          math.pow(2, 38).toLong
        )
    }
  }
}
