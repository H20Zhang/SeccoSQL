package org.apache.spark.secco.execution.plan.computation.localExec

import org.apache.spark.secco.execution.SeccoPlan
import org.apache.spark.secco.execution.plan.computation.{
  LocalProcessingExec,
  LocalStageExec
}
import org.apache.spark.secco.execution.plan.computation.newIter.{
  CartesianProductIterator,
  SeccoIterator,
  UnionIterator
}
import org.apache.spark.secco.expression.Attribute

/** An operator that performs union. */
case class LocalUnionExec(left: LocalProcessingExec, right: LocalProcessingExec)
    extends LocalProcessingExec {

  override def localStage: LocalStageExec = {
    assert(left.localStage.equals(right.localStage))
    left.localStage
  }

  override def isSorted: Boolean = left.isSorted && right.isSorted

  override def iterator(): SeccoIterator =
    UnionIterator(left.iterator(), right.iterator())

  override def output: Seq[Attribute] = {
    assert(left.output.size == right.output.size)
    assert(left.output.zip(right.output).forall { case (a1, a2) =>
      a1.dataType == a2.dataType
    })

    left.output
  }

  override def children: Seq[SeccoPlan] = Seq(left, right)
}

/** An operator that performs CartesianProduct */
case class LocalCartesianProductExec(
    lop: LocalProcessingExec,
    rop: LocalProcessingExec
) extends LocalProcessingExec {

  override def relationalSymbol: String = s"⨉"

  override def localStage: LocalStageExec = {
    assert(lop.localStage.equals(rop.localStage))
    lop.localStage
  }

  override def isSorted: Boolean = false

  override def iterator(): SeccoIterator =
    CartesianProductIterator(lop.iterator(), rop.iterator())

  override def output: Seq[Attribute] = lop.output ++ rop.output

  override def children: Seq[SeccoPlan] = Seq(lop, rop)
}
