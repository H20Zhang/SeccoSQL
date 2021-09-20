package org.apache.spark.secco.benchmark.dataset

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import org.apache.spark.secco.execution.OldInternalRow
import org.apache.spark.secco.util.misc.LogAble

import scala.collection.mutable

/** The base class for dataset that is generated synthetically. */
abstract class SyntheticDatasets {}

object SyntheticDatasets {
  def workloadExpDatasets: WorkloadExpDatasets = new WorkloadExpDatasets
}

/** The generated dataset for workload experiment.
  *
  * In this datasets, we assume each dataset(table) has 2^24 = 16777216 tuples, and each dataset is generated through
  * [[MatchingDatasetGenerator]]
  */
class WorkloadExpDatasets extends SyntheticDatasets with LogAble {

  lazy val generator = SyntheticDatasetGenerator.matchingDatasetGenerator()

  /** Generate the dataset based on feature vector. */
  private def genDatasetWithFeatureVector(
      featureVec: Array[Int],
      cardinality: Long,
      startOfInvalidValue: Int,
      isInvalidValueIncrement: Boolean
  ): Array[Array[Int]] = {
    val domainRanges = featureVec.map(f => math.pow(2, f).toInt)
    generator.generateMatchingDatasetWithDomainRanges(
      domainRanges,
      cardinality,
      startOfInvalidValue,
      isInvalidValueIncrement: Boolean
    )
  }

  /** Get the feature vector for each relation with name workdloadName/workdloadMode/relationName`, e.g., "W1/High/R1".
    *
    * Each feature vector is the radix of 2 for `domainRange`, which is used in
    * [[MatchingDatasetGenerator]].generateMatchingDatasetWithDomainRanges().
    *
    * For example, for feature vector (2,3,4), it corresponds to domainRange (2^2, 2^3 , 2^4)
    */
  private def getFeatureVector(
      workloadName: String,
      workloadMode: String,
      relationName: String
  ): Array[Int] = {
    val identifier = s"${workloadName}/${workloadMode}/${relationName}"
    identifier match {
      //W1
      case "W1/High/R1"    => Array(6, 6, 6, 6)
      case "W1/High/R2"    => Array(6, 4)
      case "W1/High/R3"    => Array(6, 4)
      case "W1/High/R4"    => Array(6, 4)
      case "W1/High/R5"    => Array(6, 4)
      case "W1/Low/R1"     => Array(6, 6, 6, 6)
      case "W1/Low/R2"     => Array(4, 0)
      case "W1/Low/R3"     => Array(4, 0)
      case "W1/Low/R4"     => Array(4, 0)
      case "W1/Low/R5"     => Array(4, 0)
      case "W1/LowHigh/R1" => Array(6, 6, 6, 6)
      case "W1/LowHigh/R2" => Array(4, 0)
      case "W1/LowHigh/R3" => Array(6, 4)
      case "W1/LowHigh/R4" => Array(6, 4)
      case "W1/LowHigh/R5" => Array(6, 4)

      //W2
      case "W2/High/R1"    => Array(8, 8, 8)
      case "W2/High/R2"    => Array(8, 4)
      case "W2/High/R3"    => Array(8, 4)
      case "W2/High/R4"    => Array(8, 4)
      case "W2/High/R5"    => Array(8, 4)
      case "W2/Low/R1"     => Array(8, 8, 8)
      case "W2/Low/R2"     => Array(6, 0)
      case "W2/Low/R3"     => Array(6, 0)
      case "W2/Low/R4"     => Array(2, 4)
      case "W2/Low/R5"     => Array(2, 0)
      case "W2/LowHigh/R1" => Array(8, 8, 8)
      case "W2/LowHigh/R2" => Array(6, 0)
      case "W2/LowHigh/R3" => Array(8, 4)
      case "W2/LowHigh/R4" => Array(8, 4)
      case "W2/LowHigh/R5" => Array(8, 4)

      //W3
      case "W3/High/R1"    => Array(12, 12)
      case "W3/High/R2"    => Array(12, 4)
      case "W3/High/R3"    => Array(4, 4)
      case "W3/High/R4"    => Array(4, 4)
      case "W3/High/R5"    => Array(4, 4)
      case "W3/Low/R1"     => Array(8, 16)
      case "W3/Low/R2"     => Array(2, 12)
      case "W3/Low/R3"     => Array(2, 8)
      case "W3/Low/R4"     => Array(2, 4)
      case "W3/Low/R5"     => Array(2, 0)
      case "W3/LowHigh/R1" => Array(12, 12)
      case "W3/LowHigh/R2" => Array(2, 8)
      case "W3/LowHigh/R3" => Array(8, 4)
      case "W3/LowHigh/R4" => Array(4, 4)
      case "W3/LowHigh/R5" => Array(4, 4)

      //W4
      case "W4/High/R1"    => Array(12, 12)
      case "W4/High/R2"    => Array(12, 4)
      case "W4/High/R3"    => Array(12, 4)
      case "W4/High/R4"    => Array(12, 4)
      case "W4/High/R5"    => Array(4, 4)
      case "W4/Low/R1"     => Array(12, 12)
      case "W4/Low/R2"     => Array(2, 8)
      case "W4/Low/R3"     => Array(0, 0)
      case "W4/Low/R4"     => Array(2, 8)
      case "W4/Low/R5"     => Array(2, 4)
      case "W4/LowHigh/R1" => Array(12, 12)
      case "W4/LowHigh/R2" => Array(2, 8)
      case "W4/LowHigh/R3" => Array(2, 4)
      case "W4/LowHigh/R4" => Array(12, 4)
      case "W4/LowHigh/R5" => Array(8, 4)

      //W5
      case "W5/High/R1"    => Array(12, 12)
      case "W5/High/R2"    => Array(12, 4)
      case "W5/High/R3"    => Array(4, 4)
      case "W5/High/R4"    => Array(4, 4)
      case "W5/High/R5"    => Array(4, 12)
      case "W5/Low/R1"     => Array(12, 12)
      case "W5/Low/R2"     => Array(2, 8)
      case "W5/Low/R3"     => Array(2, 4)
      case "W5/Low/R4"     => Array(2, 4)
      case "W5/Low/R5"     => Array(4, 12)
      case "W5/LowHigh/R1" => Array(12, 12)
      case "W5/LowHigh/R2" => Array(2, 8)
      case "W5/LowHigh/R3" => Array(8, 4)
      case "W5/LowHigh/R4" => Array(4, 4)
      case "W5/LowHigh/R5" => Array(4, 12)

      //W6
      case "W6/High/R1"    => Array(6, 12)
      case "W6/High/R2"    => Array(4, 13)
      case "W6/High/R3"    => Array(4, 14)
      case "W6/Low/R1"     => Array(6, 12)
      case "W6/Low/R2"     => Array(4, 3)
      case "W6/Low/R3"     => Array(1, 1)
      case "W6/LowHigh/R1" => Array(6, 12)
      case "W6/LowHigh/R2" => Array(4, 3)
      case "W6/LowHigh/R3" => Array(3, 6)

      //W7
      case "W7/High/R1"    => Array(9, 9)
      case "W7/High/R2"    => Array(2, 14)
      case "W7/High/R3"    => Array(6, 11)
      case "W7/Low/R1"     => Array(6, 12)
      case "W7/Low/R2"     => Array(2, 7)
      case "W7/Low/R3"     => Array(3, 0)
      case "W7/LowHigh/R1" => Array(6, 12)
      case "W7/LowHigh/R2" => Array(2, 7)
      case "W7/LowHigh/R3" => Array(4, 8)

      //W8
      case "W8/High/R1"    => Array(9, 9)
      case "W8/High/R2"    => Array(2, 14)
      case "W8/High/R3"    => Array(6, 11)
      case "W8/Low/R1"     => Array(6, 12)
      case "W8/Low/R2"     => Array(2, 7)
      case "W8/Low/R3"     => Array(3, 0)
      case "W8/LowHigh/R1" => Array(6, 12)
      case "W8/LowHigh/R2" => Array(2, 7)
      case "W8/LowHigh/R3" => Array(4, 8)

      //W9
      case "W9/High/R1"    => Array(22, 2)
      case "W9/High/R2"    => Array(2, 6)
      case "W9/High/R3"    => Array(0, 4)
      case "W9/High/R4"    => Array(4, 4)
      case "W9/High/R5"    => Array(4, 4)
      case "W9/Low/R1"     => Array(14, 2)
      case "W9/Low/R2"     => Array(2, 6)
      case "W9/Low/R3"     => Array(0, 4)
      case "W9/Low/R4"     => Array(0, 2)
      case "W9/Low/R5"     => Array(0, 0)
      case "W9/LowHigh/R1" => Array(14, 2)
      case "W9/LowHigh/R2" => Array(2, 6)
      case "W9/LowHigh/R3" => Array(0, 10)
      case "W9/LowHigh/R4" => Array(10, 4)
      case "W9/LowHigh/R5" => Array(4, 4)

      //W10
      case "W10/High/R1"    => Array(22, 2)
      case "W10/High/R2"    => Array(2, 6)
      case "W10/High/R3"    => Array(0, 4)
      case "W10/High/R4"    => Array(4, 4)
      case "W10/High/R5"    => Array(4, 4)
      case "W10/Low/R1"     => Array(14, 2)
      case "W10/Low/R2"     => Array(2, 6)
      case "W10/Low/R3"     => Array(0, 4)
      case "W10/Low/R4"     => Array(0, 2)
      case "W10/Low/R5"     => Array(0, 0)
      case "W10/LowHigh/R1" => Array(14, 2)
      case "W10/LowHigh/R2" => Array(2, 6)
      case "W10/LowHigh/R3" => Array(0, 10)
      case "W10/LowHigh/R4" => Array(10, 4)
      case "W10/LowHigh/R5" => Array(4, 4)

    }
  }

  /** Get IsInvalidValueIncrement for each test-case with name workdloadName/workdloadMode`, e.g., "W1/High". */
  private def getIsInvalidValueIncrement(
      workloadName: String
  ): Boolean = {
    val identifier = s"${workloadName}"
    identifier match {
      case "W1"  => true
      case "W2"  => true
      case "W3"  => true
      case "W4"  => true
      case "W5"  => true
      case "W6"  => false
      case "W7"  => false
      case "W8"  => false
      case "W9"  => true
      case "W10" => true
    }
  }

  /** Generate the datasets. */
  def genDatasets(
      isDemo: Boolean = false,
      workloadNames: Seq[String] =
        Seq("W1", "W2", "W3", "W4", "W5", "W6", "W7", "W8", "W9", "W10"),
      workloadModes: Seq[String] = Seq("High", "Low", "LowHigh"),
      outputPath: Option[String] = None
  ): Unit = {

    val getRelationOfWorkload = Map(
      "W1" -> Seq("R1", "R2", "R3", "R4", "R5"),
      "W2" -> Seq("R1", "R2", "R3", "R4", "R5"),
      "W3" -> Seq("R1", "R2", "R3", "R4", "R5"),
      "W4" -> Seq("R1", "R2", "R3", "R4", "R5"),
      "W5" -> Seq("R1", "R2", "R3", "R4", "R5"),
      "W6" -> Seq("R1", "R2", "R3"),
      "W7" -> Seq("R1", "R2", "R3"),
      "W8" -> Seq("R1", "R2", "R3"),
      "W9" -> Seq("R1", "R2", "R3", "R4", "R5"),
      "W10" -> Seq("R1", "R2", "R3", "R4", "R5")
    )

    val getCardinalityOfWorkload = Map(
      "W1" -> math.pow(2, 24).toLong,
      "W2" -> math.pow(2, 24).toLong,
      "W3" -> math.pow(2, 24).toLong,
      "W4" -> math.pow(2, 24).toLong,
      "W5" -> math.pow(2, 24).toLong,
      "W6" -> math.pow(2, 18).toLong,
      "W7" -> math.pow(2, 18).toLong,
      "W8" -> math.pow(2, 18).toLong,
      "W9" -> math.pow(2, 24).toLong,
      "W10" -> math.pow(2, 24).toLong
    )

    for (workloadName <- workloadNames) {
      val cardinality = if (isDemo) {
        math.pow(2, 18).toLong
      } else {
        getCardinalityOfWorkload(workloadName)
      }
      for (workloadMode <- workloadModes) {
        val relationNames = getRelationOfWorkload(workloadName)
        val isInvalidValueIncrement =
          getIsInvalidValueIncrement(workloadName)

        var startOfInvalidValue = if (isInvalidValueIncrement) {
          (cardinality + 1).toInt
        } else {
          (-(cardinality + 1)).toInt
        }

        for (relationName <- relationNames) {
          //increment start of the invalid to ensure invalid value of each relation is different and won't match in join.
          startOfInvalidValue = if (isInvalidValueIncrement) {
            startOfInvalidValue + cardinality.toInt
          } else {
            startOfInvalidValue - cardinality.toInt
          }

          val identifier = s"${workloadName}/${workloadMode}/${relationName}"
          val vec = getFeatureVector(workloadName, workloadMode, relationName)

          // For demonstration purpose, we will set the 0-th pos of R1 to "1" to reduce the evaluation cost.
          // This is for demonstration and debug purpose only.
          if (isDemo && relationName == "R1") {
            vec(0) = 0
            logInfo(s"generating demo relation of $identifier")
          } else {
            logInfo(s"generating $identifier")
          }

          val dataset =
            genDatasetWithFeatureVector(
              vec,
              cardinality,
              startOfInvalidValue,
              isInvalidValueIncrement
            )

          //write the generated dataset out.
          outputPath match {
            case Some(path) =>
              val outputPath = s"$path/$identifier"

              val file = new File(outputPath)
              val writer = new BufferedWriter(new FileWriter(file))

              try {
                dataset.foreach { row =>
                  writer.write(row.mkString("\t"))
                  writer.write("\n")
                }

              } finally {
                writer.close()
              }
            case None =>
          }

        }
      }
    }

  }

}
