package org.apache.spark.secco.benchmark.dataset

import scala.collection.mutable.ArrayBuffer

/**
  * An base class of data generator that generate synthetic datasets.
  */
abstract class SyntheticDatasetGenerator {}

object SyntheticDatasetGenerator {

  /** Return a generator for matching dataset. */
  def matchingDatasetGenerator(): MatchingDatasetGenerator =
    new MatchingDatasetGenerator()

}

/**
  * An matching dataset generator.
  *
  * An matching dataset `R1` is of form:
  *
  *   A B C
  *   1 1 1
  *   1 1 2
  *   1 2 1
  *   1 2 2
  *   2 1 1
  *   2 1 2
  *   2 2 1
  *   2 2 2
  */
class MatchingDatasetGenerator extends SyntheticDatasetGenerator {

  /**
    * Generate the matching dataset
    * @param domains domains of each column.
    * @param cardinality the cardinality of the matching dataset
    * @param startOfInvalidValue start of the invalidValue to filling into matching dataset to make it reaches `cardinality`
    * @param isInvalidValueIncrement set true if invalid values increments from `startOfInvalidValue`, and set false if otherwise
    * @return a matching dataset
    */
  def generateMatchingDatasetWithDomain(
      domains: Seq[Seq[Int]],
      cardinality: Long,
      startOfInvalidValue: Int,
      isInvalidValueIncrement: Boolean
  ): Array[Array[Int]] = {

    //compute some information about the dataset to generate
    val validTupleCardinality = domains.map(_.size.toLong).reduce(_ * _)
    val invalidTupleCardinality = cardinality - validTupleCardinality
    val arity = domains.size
    var invalidValue = startOfInvalidValue
    val invalidValueIncrementAmount = isInvalidValueIncrement match {
      case true  => 1
      case false => -1
    }

    assert(
      invalidTupleCardinality >= 0,
      s"invalidTupleCardinality:${invalidTupleCardinality}<0, domains:${domains}"
    )
    assert(arity > 0)

    //generate invalid tuples
    val buffer = ArrayBuffer[Array[Int]]()

    var i = 0L
    while (i < invalidTupleCardinality) {
      invalidValue += invalidValueIncrementAmount
      buffer += Array.fill(arity)(invalidValue)
      i += 1
    }

    //generate valid tuples
    import org.apache.spark.secco.util.`extension`.ArrayExtension.catersianProduct

    val firstDomainTuples = domains(0).map(Seq(_))

    domains
      .drop(1)
      .foldLeft(firstDomainTuples)((lastDomainTuples, curDomain) =>
        catersianProduct(lastDomainTuples, curDomain.map(Seq(_)))
      )
      .foreach(f => buffer += f.toArray)

    //return the matching dataset
    buffer.toArray
  }

  /**
    * Generate the matching dataset
    * @param domainRanges ranges of domain of each column, e.g., Seq(2, 3, 4) represents domain ([2], [3], [4])
    * @param cardinality the cardinality of the matching dataset
    * @param startOfInvalidValue start of the invalidValue to filling into matching dataset to make it reaches `cardinality`
    * @return a matching dataset
    */
  def generateMatchingDatasetWithDomainRanges(
      domainRanges: Seq[Int],
      cardinality: Long,
      startOfInvalidValue: Int,
      isInvalidValueIncrement: Boolean
  ): Array[Array[Int]] = {
    val domains = domainRanges.map { f =>
      Range(0, f)
    }

    generateMatchingDatasetWithDomain(
      domains,
      cardinality,
      startOfInvalidValue,
      isInvalidValueIncrement
    )
  }

}
