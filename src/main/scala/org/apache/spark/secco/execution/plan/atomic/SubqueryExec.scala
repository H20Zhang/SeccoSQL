package org.apache.spark.secco.execution.plan.atomic

import org.apache.spark.rdd.RDD
import org.apache.spark.secco.execution.SeccoPlan
import org.apache.spark.secco.execution.storage.{
  ColumnarBlockPartition,
  GenericBlockPartition,
  HashMapPartition,
  InternalPartition,
  PairedPartition,
  SetPartition,
  TrieIndexedPartition,
  UnsafeBlockPartition
}
import org.apache.spark.secco.expression.Attribute

/** An operator that execute the sub-query. */
case class SubqueryExec(
    subquery: SeccoPlan,
    aliasName: String,
    output: Seq[Attribute]
) extends SeccoPlan {

  override protected def doExecute(): RDD[InternalPartition] = {
    subquery.execute().map { partition =>
      partition match {
        case p: HashMapPartition       => p.copy(output = output)
        case p: TrieIndexedPartition   => p.copy(output = output)
        case p: UnsafeBlockPartition   => p.copy(output = output)
        case p: GenericBlockPartition  => p.copy(output = output)
        case p: SetPartition           => p.copy(output = output)
        case p: PairedPartition        => p.copy(output = output)
        case p: ColumnarBlockPartition => p.copy(output = output)
      }
    }
  }

  override def children: Seq[SeccoPlan] = Seq(subquery)
}
