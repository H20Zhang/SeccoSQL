package util

import scala.util.Random

object TestQueryGenerator {

  //generate random schemas, which can be used to test correctness of natural join algorithm.
  def genRandomSchemas(
      numRelation: Int,
      arityLimit: Int
  ): Seq[Seq[String]] = {
    Range(0, numRelation).map { _ =>
      val attributes =
        Range(0, Math.max(Random.nextInt(arityLimit + 1), 1)).map { _ =>
          Random.nextInt(arityLimit + 1).toString
        }
      attributes
    }
  }

}
