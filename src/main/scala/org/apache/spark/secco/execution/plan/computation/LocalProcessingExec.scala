package org.apache.spark.secco.execution.plan.computation

import org.apache.spark.secco.optimization.plan.JoinType._
import org.apache.spark.secco.execution.{SeccoPlan, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.execution.plan.computation.newIter.{
  AggregateIterator,
  BuildHashMap,
  BuildSet,
  BuildTrie,
  CartesianProductIterator,
  DistinctIterator,
  HashJoinIterator,
  LeapFrogJoinIterator,
  ProjectIterator,
  SeccoIterator,
  SelectIterator,
  SortIterator,
  TableIterator,
  UnionIterator
}
import org.apache.spark.secco.execution.storage.InternalPartition
import org.apache.spark.secco.execution.storage.block.{
  HashMapInternalBlock,
  InternalBlock
}
import org.apache.spark.secco.expression.aggregate.AggregateFunction
import org.apache.spark.secco.expression.codegen.GeneratePredicate
import org.apache.spark.secco.expression.{
  Attribute,
  BindReferences,
  Expression,
  NamedExpression
}
import org.apache.spark.secco.optimization.plan.JoinType

import scala.collection.mutable

/** A local computation physical operator.
  */
abstract class LocalProcessingExec extends SeccoPlan {

  /** The local computation stage in which this local computation is executed. */
  def localStage: LocalStageExec

  /** Whether the output is sorted. */
  def isSorted: Boolean

  /** The output iterator */
  def iterator(): SeccoIterator

  /** The materialized result of [[iterator()]] */
  def result(): InternalBlock = iterator().results()

  /** Generate the code for local computation. */
  //TODO: implement this.
  def codegen(): Unit = {
    throw new NotImplementedError()
  }

  /** LocalExec cannot doExecute, as it is a global operation */
  override protected def doExecute(): RDD[InternalPartition] = {
    throw new Exception(
      "LocalExec does not support doExecute, which is a global operation, and Local Exec " +
        "is performed per InternalBlock. Try use result() instead."
    )
  }
}

/** An operator that loads block at specific position */
case class LocalInputExec(
    localStage: LocalStageExec,
    pos: Int,
    output: Seq[Attribute]
) extends LocalProcessingExec {

  private var localBlock: Option[InternalPartition] = None

  private lazy val block = result()

  def setBlock(partition: InternalPartition) = {
    this.localBlock = Some(partition)
  }

  //TODO: add `isSorted` field to InternalBlock.
  override def isSorted: Boolean = false

  //TODO: add `isSorted` field to InternalBlock.
  override def iterator(): SeccoIterator =
    TableIterator(block, output.toArray, false)

  override def result(): InternalBlock = {
    assert(
      localBlock.nonEmpty,
      "LocalInputExec has not been initialized by setting local block."
    )

    block
  }

  override def children: Seq[SeccoPlan] = Nil

  override def relationalSymbol: String = s"R${pos.toString}"
}

/** An operator that filters row using [[condition]] */
case class LocalSelectExec(
    child: LocalProcessingExec,
    condition: Expression
) extends LocalProcessingExec {

  override def iterator(): SeccoIterator =
    SelectIterator(child.iterator(), condition)

  override def children: Seq[SeccoPlan] = Seq(child)

  override def relationalSymbol: String =
    s"𝜎[${condition.sql}]"

  override def localStage: LocalStageExec = child.localStage

  override def isSorted: Boolean = child.isSorted

  override def output: Seq[Attribute] = child.output
}

/** An operator that performs projection. */
case class LocalProjectExec(
    child: LocalProcessingExec,
    projectionList: Seq[NamedExpression]
) extends LocalProcessingExec {

  override def iterator(): SeccoIterator =
    ProjectIterator(child.iterator(), projectionList.toArray)

  override def children: Seq[SeccoPlan] = Seq(child)

  override def relationalSymbol: String =
    s"∏[${projectionList.map(_.sql).mkString(",")}]"

  override def localStage: LocalStageExec = child.localStage

  override def isSorted: Boolean = child.isSorted

  override def output: Seq[Attribute] = projectionList.map(_.toAttribute)
}

/** An operator that performs distinct */
case class LocalDistinctExec(child: LocalProcessingExec)
    extends LocalProcessingExec {

  override def localStage: LocalStageExec = child.localStage

  override def isSorted: Boolean = child.isSorted

  override def iterator(): SeccoIterator = DistinctIterator(child.iterator())

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SeccoPlan] = Seq(child)
}

/** An operator that performs sort.
  *
  * Note: sortOrder must include all attributes.
  * TODO: change sortOrder to include only part of the attributes.
  */
case class LocalSortExec(
    child: LocalProcessingExec,
    sortOrder: Seq[Attribute],
    sortDirection: Seq[Boolean]
) extends LocalProcessingExec {

  override def localStage: LocalStageExec = child.localStage

  override def isSorted: Boolean = true

  override def iterator(): SeccoIterator =
    SortIterator(child.iterator(), sortOrder.toArray, sortDirection.toArray)

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SeccoPlan] = Seq(child)
}

trait IndexType
case object TrieIndex extends IndexType
case object HashMapIndex extends IndexType
case object HashSetIndex extends IndexType

/** An operator that build index
  * @param child child local operator
  * @param indexType the index to build, which is selected from [[TrieIndex]], [[HashMapIndex]], [[HashSetIndex]]
  * @param indexedAttributes the attributes to be indexed
  */
case class LocalBuildIndexExec(
    child: LocalProcessingExec,
    indexType: IndexType,
    indexedAttributes: Seq[Attribute]
) extends LocalProcessingExec {

  override def localStage: LocalStageExec = child.localStage

  override def isSorted: Boolean = child.isSorted

  override def iterator(): SeccoIterator = indexType match {
    case TrieIndex => BuildTrie(child.iterator(), indexedAttributes.toArray)
    case HashMapIndex =>
      BuildHashMap(child.iterator(), indexedAttributes.toArray)
    case HashSetIndex => BuildSet(child.iterator(), indexedAttributes.toArray)
  }

  override def output: Seq[Attribute] = child.output

  override def children: Seq[SeccoPlan] = Seq(child)
}

/** An operator that perform aggregation
  * @param child child local operator
  * @param groupingExpressions expressions to group tuples
  * @param aggregateExpressions expressions to perform aggregation per group
  */
case class LocalAggregateExec(
    child: LocalProcessingExec,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateFunction]
) extends LocalProcessingExec {

  override def localStage: LocalStageExec = child.localStage

  override def isSorted: Boolean = false

  override def iterator(): SeccoIterator = AggregateIterator(
    child.iterator(),
    groupingExpressions.toArray,
    aggregateExpressions.toArray
  )

  override def output: Seq[Attribute] = groupingExpressions.map(
    _.toAttribute
  ) ++ aggregateExpressions.flatMap(_.aggBufferAttributes)

  override def children: Seq[SeccoPlan] = Seq(child)

  override def relationalSymbol: String =
    s"Agg[${groupingExpressions.map(_.sql).mkString(",")}, ${aggregateExpressions.map(_.sql).mkString(",")}]"
}

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

/** An operator that performs BinaryJoin */
//TODO: TBD, some problem in BuildHashMap, and HashJoinIterator
case class LocalHashJoinExec(
    left: LocalProcessingExec,
    right: LocalBuildIndexExec,
    leftKeys: Seq[Expression],
    joinType: JoinType,
    joinCondition: Expression
) extends LocalProcessingExec {

  override def relationalSymbol: String = s"⋈"

  override def localStage: LocalStageExec = {
    assert(left.localStage.eq(right.localStage))

    left.localStage
  }

  override def isSorted: Boolean = false

  override def iterator(): SeccoIterator = ???
//  {
//    HashJoinIterator(
//      left.iterator(), right, leftKeys, joinCondition
//    )
//  }

  /** The output attributes */
  override def output: Seq[Attribute] = left.output ++ right.output

  override def children: Seq[SeccoPlan] = Seq(left, right)
}

/** An operator that performs LeapFrog join. */
case class LocalLeapFrogJoinExec(
    children: Seq[LocalProcessingExec],
    attributeOrder: Seq[Attribute]
) extends LocalProcessingExec {

  override def relationalSymbol: String = s"⋈"

  override def localStage: LocalStageExec = {
    assert(children.nonEmpty)
    val headLocalStage = children.head.localStage;
    assert(children.forall(_.localStage == headLocalStage))

    headLocalStage
  }

  override def isSorted: Boolean = true

  override def iterator(): SeccoIterator =
    LeapFrogJoinIterator(children.map(_.iterator()), attributeOrder.toArray)

  override def output: Seq[Attribute] = attributeOrder

}
