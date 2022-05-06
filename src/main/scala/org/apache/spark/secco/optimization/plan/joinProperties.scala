package org.apache.spark.secco.optimization.plan

import java.util.Locale

object JoinProperty {
  def apply(typ: String): JoinProperty =
    typ.toLowerCase(Locale.ROOT).replace("_", "") match {
      case "cyclic"   => CyclicJoinProperty
      case "acyclic"  => AcyclicJoinProperty
      case "equi"     => EquiJoinProperty
      case "theta"    => ThetaJoinProperty
      case "gequi"    => GeneralizedEquiJoinProperty
      case "pkfk"     => PrimaryKeyForeignKeyJoinConstraintProperty
      case "fkfk"     => ForeignKeyForeignKeyJoinConstraintProperty
      case "optimize" => JoinOptimizableProperty
      case _ =>
        throw new IllegalArgumentException(s"Unsupported join properties $typ")
    }
}

/** Base class for defining additional property of the join, beside JoinType. */
sealed abstract class JoinProperty {
  def sql: String

  override def toString: String = sql
}

/** A property that is used to indicate if join is part of the cyclic join. */
sealed abstract class JoinCyclicityProperty extends JoinProperty {
  def isCyclic: Boolean
}

case object JoinOptimizableProperty extends JoinProperty {
  def sql: String = "OPT"
}

case object CyclicJoinProperty extends JoinCyclicityProperty {
  override def isCyclic: Boolean = true

  override def sql: String = "CYCLIC"
}

case object AcyclicJoinProperty extends JoinCyclicityProperty {
  override def isCyclic: Boolean = false

  override def sql: String = "ACYCLIC"
}

/** A property that is used to distinguish join predicates type. */
sealed abstract class JoinPredicatesProperty extends JoinProperty {}

case object ThetaJoinProperty extends JoinPredicatesProperty {
  override def sql: String = "THETA-JOIN"
}

case object EquiJoinProperty extends JoinPredicatesProperty {
  override def sql: String = "EQUI-JOIN"
}

case object GeneralizedEquiJoinProperty extends JoinPredicatesProperty {
  override def sql: String = "GEQUI-JOIN"
}

/** A property that is used to distinguish join's integrity constraint. */
sealed abstract class JoinIntegrityConstraintProperty extends JoinProperty {}

case object PrimaryKeyForeignKeyJoinConstraintProperty
    extends JoinIntegrityConstraintProperty {
  override def sql: String = "PKFK"
}

case object ForeignKeyForeignKeyJoinConstraintProperty
    extends JoinIntegrityConstraintProperty {
  override def sql: String = "FKFK"
}
