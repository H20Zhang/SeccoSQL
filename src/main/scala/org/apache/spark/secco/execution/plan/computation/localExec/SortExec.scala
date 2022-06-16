package org.apache.spark.secco.execution.plan.computation.localExec

import org.apache.spark.secco.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.secco.execution.UnaryExecNode
import org.apache.spark.secco.execution.plan.computation.{
  LocalProcessingExec,
  LocalStageExec,
  PushBasedCodegen
}
import org.apache.spark.secco.execution.plan.computation.newIter.{
  SeccoIterator,
  SortIterator
}
import org.apache.spark.secco.execution.storage.Utils.InternalRowComparator
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.types.StructType

import java.util.Comparator

/** An operator that performs sort.
  *
  * Note: sortOrder must include all attributes.
  * TODO: change sortOrder to include only part of the attributes.
  */
case class LocalSortExec(
    child: LocalProcessingExec,
    sortOrder: Seq[Attribute],
    sortDirection: Seq[Boolean]
) extends LocalProcessingExec
    with PushBasedCodegen
    with UnaryExecNode {

  override def localStage: LocalStageExec = child.localStage

  override def isSorted: Boolean = true

  override def iterator(): SeccoIterator =
    SortIterator(child.iterator(), sortOrder.toArray, sortDirection.toArray)

  override def output: Seq[Attribute] = child.output

  override def usedInputs: AttributeSet = AttributeSet(Seq.empty)

  // Name of sorter variable used in codegen.
  private var arrayToBeSorted: String = _
  private var arraySorted: String = _

  def getComparator: Comparator[InternalRow] =
    new InternalRowComparator(
      StructType.fromAttributes(child.output),
      sortDirection.toArray
    )

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToSort =
      ctx.addMutableState(
        CodeGenerator.JAVA_BOOLEAN,
        "needToSort",
        v => s"$v = true;"
      )

    // Initialize the class member variables.
    val thisPlan = ctx.addReferenceObj("plan", this)
    val sortComparator = ctx.addMutableState(
      classOf[Comparator[InternalRow]].getName,
      "sortComparator",
      v => s"$v = $thisPlan.getComparator();",
      forceInline = true
    )
    arrayToBeSorted = ctx.addMutableState(
      classOf[java.util.ArrayList[InternalRow]].getName,
      "arrayToBeSorted",
      v => s"$v = new java.util.ArrayList<InternalRow>();",
      forceInline = true
    )
    arraySorted = ctx.addMutableState(
      classOf[Array[InternalRow]].getName,
      "sortedArray",
      v => s"$v = new InternalRow[]{};",
      forceInline = true
    )

    val addToArray = ctx.freshName("addToArray")
    val addToArrayFuncName = ctx.addNewFunction(
      addToArray,
      s"""
         | private void $addToArray() throws java.io.IOException {
         |   ${child.asInstanceOf[PushBasedCodegen].produce(ctx, this)}
         | }
      """.stripMargin.trim
    )

    val outputRow = ctx.freshName("outputRow")
    s"""
       | if ($needToSort) {
       |   $addToArrayFuncName();
       |   InternalRow[] $arraySorted = $arrayToBeSorted.toArray(new InternalRow[]{})
       |   java.util.Arrays.sort($arraySorted, $sortComparator)
       |   $needToSort = false;
       | }
       |
       | for (InternalRow row : $arraySorted) {
       |   UnsafeInternalRow $outputRow = (UnsafeInternalRow)row;
       |   ${consume(ctx, null, outputRow)}
       |   if (shouldStop()) return;
       | }
     """.stripMargin.trim
  }

  override def doConsume(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      row: ExprCode
  ): String = {
    s"""
       |${row.code}
       |$arrayToBeSorted.add(${row.value});
     """.stripMargin
  }

  protected def withNewChildInternal(
      newChild: LocalProcessingExec
  ): LocalSortExec =
    copy(child = newChild)

  //  override def inputRowIterator(): Iterator[InternalRow] = child.asInstanceOf[PushBasedCodegen].inputRowIterator()
  override def inputRowIterators(): Seq[Iterator[InternalRow]] =
    child.asInstanceOf[PushBasedCodegen].inputRowIterators()

}
