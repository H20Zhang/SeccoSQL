package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.expression.{
  AttributeReference,
  EqualTo,
  PredicateHelper
}
import org.apache.spark.secco.optimization.{LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan.{
  AcyclicJoinProperty,
  BinaryJoin,
  CyclicJoinProperty,
  EquiJoinProperty,
  ForeignKeyForeignKeyJoinConstraintProperty,
  GeneralizedEquiJoinProperty,
  InnerLike,
  JoinCyclicityProperty,
  JoinIntegrityConstraintProperty,
  JoinPredicatesProperty,
  JoinProperty,
  MultiwayJoin,
  PrimaryKeyForeignKeyJoinConstraintProperty,
  ThetaJoinProperty
}
import org.apache.spark.secco.optimization.rules.MarkJoinIntegrityConstraintProperty.splitConjunctivePredicates
import org.apache.spark.secco.optimization.rules.MarkJoinPredicateProperty.splitConjunctivePredicates
import org.apache.spark.secco.optimization.support.ExtractRequiredProjectJoins

/** A rule that marks predicate properties of the join. A join can be equi-join or theta-join. */
object MarkJoinPredicateProperty
    extends Rule[LogicalPlan]
    with PredicateHelper {

  private def identifyJoinPredicateProperties(
      j: BinaryJoin
  ): Option[JoinPredicatesProperty] = {
    j.condition match {
      case None => None
      case Some(condition) =>
        if (
          splitConjunctivePredicates(condition).forall(e =>
            e match {
              case EqualTo(a1: AttributeReference, a2: AttributeReference) =>
                true
              case _ => false
            }
          )
        ) {
          Some(EquiJoinProperty)
        } else {
          Some(ThetaJoinProperty)
        }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // only marks join predicate property for the ones that haven't been marked.
    case j @ BinaryJoin(_, _, _, _, property, mode)
        if property
          .intersect(
            Set(
              ThetaJoinProperty,
              EquiJoinProperty,
              GeneralizedEquiJoinProperty
            )
          )
          .isEmpty =>
      j.copy(property =
        property ++ identifyJoinPredicateProperties(j)
          .map(property => Set(property))
          .getOrElse(Set())
      )
  }
}

/** A rule that marks integrity constraint property of the join.
  *
  * A join can be Primary Key Foreign Key join or Foreign Key Foreign Key join.
  *
  * Note that this rule should be called after [[MarkJoinPredicateProperty]], as it relies on [[EquiJoinProperty]]
  * marked by [[MarkJoinIntegrityConstraintProperty]].
  */
object MarkJoinIntegrityConstraintProperty
    extends Rule[LogicalPlan]
    with PredicateHelper {

  private def identifyJoinIntegrityProperties(
      j: BinaryJoin
  ): Option[JoinIntegrityConstraintProperty] = {
    j.condition match {
      case None => None
      case Some(condition) =>
        val joinAttributePairs = splitConjunctivePredicates(condition).map {
          f =>
            f match {
              case EqualTo(a: AttributeReference, b: AttributeReference) =>
                (a, b)
              case _ =>
                throw new Exception(
                  s"identifyJoinIntegrityProperties can be only called on " +
                    s"${EquiJoinProperty}.sql join "
                )
            }
        }

        if (
          joinAttributePairs.exists { case (a, b) =>
            j.left.primaryKeySet
              .contains(a) || j.right.primaryKeySet.contains(b)
          }
        ) {
          Some(PrimaryKeyForeignKeyJoinConstraintProperty)
        } else {
          Some(ForeignKeyForeignKeyJoinConstraintProperty)
        }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // only marks join integrity property for the ones that haven't been marked.
    case j @ BinaryJoin(_, _, _, _, property, mode)
        if property
          .intersect(
            Set(
              PrimaryKeyForeignKeyJoinConstraintProperty,
              ForeignKeyForeignKeyJoinConstraintProperty
            )
          )
          .isEmpty && property.contains(EquiJoinProperty) =>
      j.copy(property =
        property ++ identifyJoinIntegrityProperties(j)
          .map(property => Set(property))
          .getOrElse(Set())
      )
  }
}

/** A rule that marks cyclicity property of the join.
  *
  * Note that this rule only marks cyclicity between FK-FK inner Join. Also, this rule depends on the execution of
  * [[MarkJoinIntegrityConstraintProperty]].
  */
object MarkJoinCyclicityProperty
    extends Rule[LogicalPlan]
    with PredicateHelper {

  private def identifyJoinCyclicityProperties(
      j: BinaryJoin
  ): Option[JoinCyclicityProperty] = {

    //set behavior of the pattern extractor
    ExtractRequiredProjectJoins.resetRequirement()
    ExtractRequiredProjectJoins.setRequiredJoinProperties(
      ForeignKeyForeignKeyJoinConstraintProperty :: EquiJoinProperty :: Nil
    )

    j match {
      case ExtractRequiredProjectJoins(
            inputs,
            conditions,
            projectionList,
            mode
          ) =>
        val multiwayJoin = MultiwayJoin(
          inputs,
          conditions,
          Set(ForeignKeyForeignKeyJoinConstraintProperty, EquiJoinProperty),
          mode
        )

        if (multiwayJoin.isCyclic()) {
          Some(CyclicJoinProperty)
        } else {
          Some(AcyclicJoinProperty)
        }

      case _ => None
    }

  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // only marks join cyclicity property for the ones that haven't been marked.
    case j @ BinaryJoin(_, _, joinType: InnerLike, _, property, mode)
        if property
          .intersect(
            Set(
              CyclicJoinProperty,
              AcyclicJoinProperty
            )
          )
          .isEmpty && Set(
          EquiJoinProperty,
          ForeignKeyForeignKeyJoinConstraintProperty
        ).asInstanceOf[Set[JoinProperty]].subsetOf(property) =>
      j.copy(property =
        property ++ identifyJoinCyclicityProperties(j)
          .map(property => Set(property))
          .getOrElse(Set())
      )
  }
}

/** A rule that marks an join as optimizable.
  *
  * Note that some of the rules only optimize [[BinaryJoin]] with Optimizable property.
  */
object MarkJoinOptimizableProperty extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case b: BinaryJoin =>
      b.copy(property = b.property ++ Seq(JoinProperty("optimize")))
  }
}

/** A rule that marks an join as optimizable.
  *
  * Note that some of the rules only optimize [[BinaryJoin]] with Optimizable property.
  */
object ClearJoinOptimizableProperty extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case b: BinaryJoin =>
      b.copy(property = b.property -- Seq(JoinProperty("optimize")))
  }
}
