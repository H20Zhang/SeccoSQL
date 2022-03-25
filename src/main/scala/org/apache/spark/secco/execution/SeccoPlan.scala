package org.apache.spark.secco.execution

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.TempViewManager
import org.apache.spark.secco.execution.plan.computation.utils.Alg
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper
import org.apache.spark.secco.trees.QueryPlan
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.execution.plan.communication.PairPartitioner
import org.apache.spark.secco.execution.storage.{
  InternalPartition,
  PairedPartition
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.storage.StorageLevel

/** The base class for physical operators.
  *
  * The naming convention is that physical operators end with "Exec" suffix, e.g. [[ProjectExec]].
  */
abstract class SeccoPlan
    extends QueryPlan[SeccoPlan]
    with Logging
    with Serializable {

  @transient lazy val statisticKeeper = StatisticKeeper(this)
  @transient var cachedExecuteResult: Option[RDD[InternalPartition]] = None
  @transient val dataManager =
    SeccoSession.currentSession.sessionState.tempViewManager

  def taskPartitioner(): PairPartitioner = {
    throw new Exception(s"taskPartition not avaiable for ${this.getClass}")
  }

  def cacheOutput(): Unit = {
    if (cachedExecuteResult.isEmpty) {
      val result =
        execute().persist(seccoSession.sessionState.conf.rddCacheLevel)
      val time1 = System.currentTimeMillis()
      result.count()
      val time2 = System.currentTimeMillis()
      logInfo(
        s"cached ${this.verboseString} in ${time2 - time1}ms"
      )
      cachedExecuteResult = Some(result)
    }
  }

  protected def sparkContext = sc

  override def verboseString: String =
    simpleString + s"-> (${output.mkString(",")})"

  /** Collect the results as [[Seq[InternalRow]]] */
  def collectSeq(): Seq[InternalRow] = {
    val partitionRDD = execute()

    val time1 = System.currentTimeMillis()
    val rows = partitionRDD
      .flatMap { partition =>
        if (partition.isInstanceOf[PairedPartition]) {
          throw new Exception(
            s"PairedPartition cannot be serialized into Seq[InternalRow]."
          )
        } else {
          partition.data.head.iterator
        }
      }
      .collect()
    val time2 = System.currentTimeMillis()
    logInfo(
      s"execute `collectSeq` of ${this.verboseString} in ${time2 - time1}ms"
    )

    rows
  }

  /** Returns the numbers of tuples */
  def count(): Long = {
    val _rdd = rdd()

    val time1 = System.currentTimeMillis()
    val numRows = _rdd
      .mapPartitions { rowPartitionIt =>
        var res = 0L
        while (rowPartitionIt.hasNext) {
          res += 1
          rowPartitionIt.next()
        }
        Iterator(res)
      }
      .sum()
      .toLong
    val time2 = System.currentTimeMillis()
    logInfo(s"execute `count` of ${this.verboseString} in ${time2 - time1}ms")

    numRows
  }

  /** Returns the result of this query as an RDD[InternalBlock] by delegating to `doExecute` after
    * preparations.
    *
    * Concrete implementations of SparkPlan should override `doExecute`.
    */
  final def execute(): RDD[InternalPartition] = {

    //by pass execution if cachedExecuteResults exists
    cachedExecuteResult match {
      case Some(rdd) => rdd
      case None      =>
        //prepare
        doPrepare()
        //execute
        val time1 = System.currentTimeMillis()
        val rdd = doExecute()
        val time2 = System.currentTimeMillis()
        logInfo(s"execute ${this.verboseString}")
        //cleanup
        doCleanUp()
        rdd
    }
  }

  /** Returns the result of this query as an RDD[InternalBlock] by delegating to `doRDD` after
    * preparations.
    *
    * Concrete implementations of SparkPlan should override `doRDD`.
    */
  final def rdd(): RDD[InternalRow] = {

    //by pass execution if cachedExecuteResults exists
    cachedExecuteResult match {
      case Some(rdd) =>
        rdd.flatMap {
          case t: PairedPartition =>
            throw new Exception(s"${t.getClass} not supported.")
          case partition: InternalPartition => partition.data.head.iterator
        }
      case None =>
        //prepare
        doPrepare()
        //execute
        val rdd = doRDD()
        logInfo(s"generate iterator for ${this.verboseString}")
        //cleanup
        doCleanUp()
        rdd
    }
  }

  /** Prepare this secco plan for execution
    */
  def prepare(): Unit = {
    doPrepare()
  }

  /** Overridden by concrete implementations of [[SeccoPlan]]. It is guaranteed to run before any
    * `execute` of [[SeccoPlan]]. This is helpful if we want to set up some state or optimize the
    * parameter using up to date statistic before executing the
    * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
    */
  protected def doPrepare(): Unit = {}

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  protected def doExecute(): RDD[InternalPartition]

  /** Perform the computation for computing the result of the query as an `RDD[InternalRow]`,
    * which allows very large result to be output in iterator form
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  protected def doRDD(): RDD[InternalRow] = {
    doExecute().flatMap {
      case b: PairedPartition =>
        throw new Exception(s"${b.getClass} not supported.")
      case partition: InternalPartition => partition.data.head.iterator
    }
  }

  /** Overridden by concrete implementations of DolpinPlan. It is guaranteed to run after any
    * `execute` of SparkPlan.
    *
    * This is helpful if we want to clean up cache result during `doExecute`
    * or `doPrepare`
    *
    * By default, it clears the temporary cachedExecuteResults from the
    * * cache of Spark.
    */
  protected def doCleanUp(): Unit = {
//    children.foreach { child =>
//      child.foreach { dolpinPlan =>
//        dolpinPlan.cachedExecuteResult match {
//          case Some(rdd) => rdd.unpersist(false)
//          case None      => {}
//        }
//        dolpinPlan.cachedExecuteResult = None
//      }
//    }
  }

  /** The relational symbol of the operator */
  def relationalSymbol = nodeName

  /** The Relational Algebra representation for this operator and its children */
  def relationalString: String = {
    this match {
      case plan: SeccoPlan if plan.children.isEmpty => s"$relationalSymbol"
      case _ =>
        s"$relationalSymbol(${children.map(_.relationalString).mkString(",")})"
    }
  }

}

object SeccoPlan {}

trait LeafExecNode extends SeccoPlan {
  override def children: Seq[SeccoPlan] = Nil
}

trait BinaryExecNode extends SeccoPlan {
  def left: SeccoPlan
  def right: SeccoPlan

  override final def children: Seq[SeccoPlan] = Seq(left, right)
}

trait UnaryExecNode extends SeccoPlan {
  def child: SeccoPlan

  override final def children: Seq[SeccoPlan] = child :: Nil
}

object UnaryExecNode {
  def unapply(a: Any): Option[(SeccoPlan, SeccoPlan)] =
    a match {
      case s: SeccoPlan if s.children.size == 1 => Some((s, s.children.head))
      case _                                    => None
    }
}
