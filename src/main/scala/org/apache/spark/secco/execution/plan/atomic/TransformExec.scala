package org.apache.spark.secco.execution.plan.atomic

import org.apache.spark.secco.execution.plan.support.FuncGenSupport
import org.apache.spark.secco.execution.{
  SeccoPlan,
  OldInternalBlock,
  OldInternalDataType,
  OldInternalRow,
  RowBlockOld,
  RowBlockContent
}
import org.apache.spark.rdd.RDD

case class TransformExec(
    child: SeccoPlan,
    f: Seq[String],
    outputOld: Seq[String]
) extends SeccoPlan
    with FuncGenSupport {

  lazy val transformFuncs: Array[OldInternalRow => OldInternalDataType] =
    f.map(expr => genTransformFunc(expr, child.outputOld)).toArray

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[OldInternalBlock] = {
    child.execute().map {
      case RowBlockOld(_, blockContent) =>
        val content = blockContent.content
        val contentNum = content.length
        val funcNum = transformFuncs.length
        val newContent = new Array[OldInternalRow](contentNum)
        var i = 0
        while (i < contentNum) {
          val arr = new Array[OldInternalDataType](funcNum)
          var j = 0
          while (j < funcNum) {
            arr(j) = transformFuncs(j)(content(i))
            j += 1
          }
          newContent(i) = arr
          i += 1
        }

        RowBlockOld(outputOld, RowBlockContent(newContent))
      case _ =>
        throw new Exception(
          s"block must be of type `RowIndexedBlock` or `RowBlock`"
        )
    }
  }

  override def children: Seq[SeccoPlan] = Seq(child)
}
