package org.apache.spark.secco.optimization.util.joingraph

import org.apache.spark.secco.expression.{
  AttributeReference,
  EqualTo,
  Expression
}
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.optimization.plan.{
  ForeignKeyForeignKeyJoinConstraintProperty,
  JoinIntegrityConstraintProperty,
  PrimaryKeyForeignKeyJoinConstraintProperty
}

// construct a join graph where each node is a input relation, and there exists an edge between two node iff there is
// a join condition between them. We assume the join graph is a directed graph. For PK-FK join, the direction is
// from input relation with foreign key to relation with primary key. For FK-FK join, we randomly gives the direction.
@deprecated
object JoinGraphConstructor {

  def constructJoinGraph(
      inputs: Seq[LogicalPlan],
      conditions: Seq[Expression]
  ): (
      Seq[LogicalPlan],
      Seq[
        (LogicalPlan, LogicalPlan, Expression, JoinIntegrityConstraintProperty)
      ]
  ) = {
    val nodes = inputs
    val edges = conditions.map { cond =>
      cond match {
        case EqualTo(a: AttributeReference, b: AttributeReference) =>
          val srcNode = inputs.filter(f => f.outputSet.contains(a)).head
          val dstNode = inputs.filter(f => f.outputSet.contains(b)).head

          if (srcNode.primaryKeySet.contains(a)) {
            (dstNode, srcNode, cond, PrimaryKeyForeignKeyJoinConstraintProperty)
          } else if (dstNode.primaryKeySet.contains(b)) {
            (srcNode, dstNode, cond, PrimaryKeyForeignKeyJoinConstraintProperty)
          } else {
            (srcNode, dstNode, cond, ForeignKeyForeignKeyJoinConstraintProperty)
          }

        case cond: Expression =>
          throw new Exception(
            s"join graph cannot be constructed for ${cond}, it can only be constructed for equi-join conditions"
          )
      }
    }

    (nodes, edges)
  }

}
