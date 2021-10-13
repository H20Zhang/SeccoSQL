package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.expression.Attribute

import java.util.Locale

object JoinType {
  def apply(typ: String): JoinType =
    typ.toLowerCase(Locale.ROOT).replace("_", "") match {
      case "inner"                        => Inner
      case "outer" | "full" | "fullouter" => FullOuter
      case "leftouter" | "left"           => LeftOuter
      case "rightouter" | "right"         => RightOuter
      case "leftsemi"                     => LeftSemi
      case "leftanti"                     => LeftAnti
      case "cross"                        => Cross
      case _ =>
        val supported = Seq(
          "inner",
          "outer",
          "full",
          "fullouter",
          "full_outer",
          "leftouter",
          "left",
          "left_outer",
          "rightouter",
          "right",
          "right_outer",
          "leftsemi",
          "left_semi",
          "leftanti",
          "left_anti",
          "cross"
        )

        throw new IllegalArgumentException(
          s"Unsupported join type '$typ'. " +
            "Supported join types include: " + supported.mkString(
              "'",
              "', '",
              "'"
            ) + "."
        )
    }
}

sealed abstract class JoinType {
  def sql: String
}

/** The explicitCartesian flag indicates if the inner join was constructed with a CROSS join
  * indicating a cartesian product has been explicitly requested.
  */
sealed abstract class InnerLike extends JoinType {
  def explicitCartesian: Boolean
}

case object Inner extends InnerLike {
  override def explicitCartesian: Boolean = false
  override def sql: String = "INNER"
}

case object Cross extends InnerLike {
  override def explicitCartesian: Boolean = true
  override def sql: String = "CROSS"
}

case object LeftOuter extends JoinType {
  override def sql: String = "LEFT OUTER"
}

case object RightOuter extends JoinType {
  override def sql: String = "RIGHT OUTER"
}

case object FullOuter extends JoinType {
  override def sql: String = "FULL OUTER"
}

case object LeftSemi extends JoinType {
  override def sql: String = "LEFT SEMI"
}

case object LeftAnti extends JoinType {
  override def sql: String = "LEFT ANTI"
}

case class ExistenceJoin(exists: Attribute) extends JoinType {
  override def sql: String = {
    // This join type is only used in the end of optimizer and physical plans, we will not
    // generate SQL for this join type
    throw new UnsupportedOperationException
  }
}

case class NaturalJoin(tpe: JoinType) extends JoinType {
  require(
    Seq(Inner, LeftOuter, RightOuter, FullOuter).contains(tpe),
    "Unsupported natural join type " + tpe
  )
  override def sql: String = "NATURAL " + tpe.sql
}

case class UsingJoin(tpe: JoinType, usingColumns: Seq[String])
    extends JoinType {
  require(
    Seq(Inner, LeftOuter, LeftSemi, RightOuter, FullOuter, LeftAnti).contains(
      tpe
    ),
    "Unsupported using join type " + tpe
  )
  override def sql: String = "USING " + tpe.sql
}

object LeftExistence {
  def unapply(joinType: JoinType): Option[JoinType] = joinType match {
    case LeftSemi | LeftAnti => Some(joinType)
    case j: ExistenceJoin    => Some(joinType)
    case _                   => None
  }
}
