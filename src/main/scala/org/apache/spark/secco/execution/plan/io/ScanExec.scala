package org.apache.spark.secco.execution.plan.io

import org.apache.spark.secco.execution.SeccoPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.execution.plan.communication.Coordinate
import org.apache.spark.secco.execution.storage.{
  InternalPartition,
  UnsafeBlockPartition
}
import org.apache.spark.secco.execution.storage.block.UnsafeInternalRowBlockBuilder
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.types.StructType

/** The base trait for physical plan that scans the data. */
trait ScanExec extends SeccoPlan {
  override final def children: Seq[SeccoPlan] = Nil
}

/** The physical plan that scans rows from external [[RDD]] that stores [[InternalRow]]. */
case class ExternalRDDScanExec(
    inputRDD: RDD[InternalRow],
    output: Seq[Attribute]
) extends ScanExec {

  override protected def doExecute(): RDD[InternalPartition] = {

    val partitioner = inputRDD.partitioner

    inputRDD.mapPartitionsWithIndex { (index, partition) =>
      // by default, we use UnsafeInternalBlock to store rows.
      val schema = StructType.fromAttributes(output)
      val builder = new UnsafeInternalRowBlockBuilder(schema)
      partition.foreach { builder.add }

      Iterator(
        UnsafeBlockPartition(
          output,
          Seq(builder.build()),
          None,
          partitioner
        )
      )
    }
  }
}

/** The physical plan that scans local [[InternalRow]]. */
case class LocalRowScanExec(seq: Seq[InternalRow], output: Seq[Attribute])
    extends ScanExec {

  override protected def doExecute(): RDD[InternalPartition] = {

    if (seq.nonEmpty) {
      // parallelize local rows.
      val rdd = sc.parallelize(seq)

      // delegate to ExternalRDDScanExec
      ExternalRDDScanExec(rdd, output).execute()
    } else {
      sc.emptyRDD
    }
  }

}

/** The physical plan that scans [[RDD]] that stores partitioned [[InternalPartition]] */
case class PartitionedRDDScanExec(
    partitions: RDD[InternalPartition],
    output: Seq[Attribute]
) extends ScanExec {
  override protected def doExecute(): RDD[InternalPartition] = partitions
}
