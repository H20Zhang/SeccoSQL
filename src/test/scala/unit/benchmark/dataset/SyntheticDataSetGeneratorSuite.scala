package unit.benchmark.dataset

import org.apache.spark.secco.benchmark.dataset.SyntheticDatasetGenerator
import util.SeccoFunSuite

class SyntheticDataSetGeneratorSuite extends SeccoFunSuite {

  test("MatchingDatasetGenerator") {

    val generator = SyntheticDatasetGenerator.matchingDatasetGenerator()

    val domainRange = Seq(2, 2, 3)
    val cardinality = 2 * 2 * 3 * 2
    val invalidValue = 100

    val ds = generator.generateMatchingDatasetWithDomainRanges(
      domainRange,
      cardinality,
      invalidValue,
      true
    )

    //check if size of invalid tuples matches
    assert(
      ds.filter(_(0) > invalidValue).size == cardinality - domainRange
        .reduce(_ * _)
    )

    //check if correctly generate the matching dataset
    val matchingDataset = Seq(
      Seq(0, 0, 0),
      Seq(0, 0, 1),
      Seq(0, 0, 2),
      Seq(0, 1, 0),
      Seq(0, 1, 1),
      Seq(0, 1, 2),
      Seq(1, 0, 0),
      Seq(1, 0, 1),
      Seq(1, 0, 2),
      Seq(1, 1, 0),
      Seq(1, 1, 1),
      Seq(1, 1, 2)
    )

    assert(
      ds.filter(_(0) < invalidValue).map(_.toSeq).toSeq == matchingDataset
    )

  }

  test("check_negative_invalid_value_increment") {

    val generator = SyntheticDatasetGenerator.matchingDatasetGenerator()

    val domainRange = Seq(2, 2, 3)
    val cardinality = 2 * 2 * 3 * 2
    val invalidValue = 0

    val ds = generator.generateMatchingDatasetWithDomainRanges(
      domainRange,
      cardinality,
      invalidValue,
      false
    )

    //check if size of invalid tuples matches
    assert(
      ds.filter(_(0) < invalidValue).size == cardinality - domainRange
        .reduce(_ * _)
    )

    pprint.pprintln(ds.filter(_(0) < invalidValue))

    val invalidMatchingDataset = Seq(
      Seq(-1, -1, -1),
      Seq(-2, -2, -2),
      Seq(-3, -3, -3),
      Seq(-4, -4, -4),
      Seq(-5, -5, -5),
      Seq(-6, -6, -6),
      Seq(-7, -7, -7),
      Seq(-8, -8, -8),
      Seq(-9, -9, -9),
      Seq(-10, -10, -10),
      Seq(-11, -11, -11),
      Seq(-12, -12, -12)
    )

    assert(
      ds.filter(_(0) < invalidValue)
        .map(_.toSeq)
        .toSeq == invalidMatchingDataset
    )

  }

}
