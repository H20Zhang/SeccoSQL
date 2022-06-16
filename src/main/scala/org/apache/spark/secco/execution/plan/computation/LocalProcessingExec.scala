package org.apache.spark.secco.execution.plan.computation

import org.apache.spark.secco.optimization.plan.JoinType._
import org.apache.spark.secco.execution.{SeccoPlan, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen.{
  CodeGenerator,
  CodegenContext,
  ExprCode,
  FalseLiteralValue,
  JavaCode
}
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
import org.apache.spark.secco.execution.storage.Utils.InternalRowComparator
import org.apache.spark.secco.execution.storage.{
  InternalPartition,
  PairedPartition
}
import org.apache.spark.secco.execution.storage.block.{
  HashMapInternalBlock,
  HashMapInternalBlockBuilder,
  InternalBlock,
  TrieInternalBlock,
  TrieInternalBlockBuilder
}
import org.apache.spark.secco.execution.storage.row.{
  InternalRow,
  UnsafeInternalRow
}
import org.apache.spark.secco.expression.BindReferences.bindReferences
import org.apache.spark.secco.expression.aggregate.{
  AggregateFunction,
  DeclarativeAggregate,
  ImperativeAggregate
}
import org.apache.spark.secco.expression.codegen.{
  GeneratePredicate,
  GenerateUnsafeProjection
}
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.expression.{
  Attribute,
  AttributeReference,
  AttributeSeq,
  BindReferences,
  BoundReference,
  EmptyRow,
  ExprId,
  Expression,
  IsNotNull,
  NamedExpression,
  PredicateHelper
}
import org.apache.spark.secco.optimization.plan.JoinType
import org.apache.spark.secco.optimization.util.{AttributeOrder, EquiAttributes}
import org.apache.spark.secco.types.StructType

import java.util
import java.util.Comparator
import scala.collection.mutable

/** A local computation physical operator.
  */
abstract class LocalProcessingExec extends SeccoPlan {

  /** The local computation stage in which this local computation is executed. */
  def localStage: LocalStageExec

  /** Whether the output is sorted.
    *
    * By default, isSorted is false.
    */
  def isSorted: Boolean = false

  /** The output iterator */
  def iterator(): SeccoIterator

  /** The materialized result of [[iterator()]] */
  def iteratorResults(): InternalBlock = iterator().results()

  /** LocalExec cannot doExecute, as it is a global operation */
  override protected def doExecute(): RDD[InternalPartition] = {
    throw new Exception(
      "LocalExec does not support doExecute, which is a global operation, and Local Exec " +
        "is performed per InternalBlock. Try use result() instead."
    )
  }
}

//trait IndexType
//case object TrieIndex extends IndexType
//case object HashMapIndex extends IndexType
//case object HashSetIndex extends IndexType
//
//@deprecated
///** An operator that build index
//  * @param child child local operator
//  * @param indexType the index to build, which is selected from [[TrieIndex]], [[HashMapIndex]], [[HashSetIndex]]
//  * @param indexedAttributes the attributes to be indexed
//  */
//case class LocalBuildIndexExec(
//    child: LocalProcessingExec,
//    indexType: IndexType,
//    indexedAttributes: Seq[Attribute]
//) extends LocalProcessingExec {
//
//  override def localStage: LocalStageExec = child.localStage
//
//  override def isSorted: Boolean = child.isSorted
//
//  override def iterator(): SeccoIterator = indexType match {
//    case TrieIndex => BuildTrie(child.iterator(), indexedAttributes.toArray)
//    case HashMapIndex =>
//      BuildHashMap(child.iterator(), indexedAttributes.toArray)
//    case HashSetIndex => BuildSet(child.iterator(), indexedAttributes.toArray)
//  }
//
//  override def output: Seq[Attribute] = child.output
//
//  override def children: Seq[SeccoPlan] = Seq(child)
//
//  override def relationalSymbol: String = s"${indexType.toString}"
//}
