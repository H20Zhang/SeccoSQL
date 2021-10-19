package org.apache.spark.secco.execution.plan.computation

import org.apache.spark.secco.execution.{SeccoPlan, _}
import org.apache.spark.secco.execution.plan.computation.utils.{
  Alg,
  ArrayTrie,
  ConsecutiveRowArray,
  InternalRowHashMap,
  LexicalOrderComparator
}
import org.apache.spark.secco.execution.plan.communication.utils.PairPartitioner
import org.apache.spark.secco.execution.statsComputation.{StatisticKeeper}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/** A local preprocessing operator.
  * @param child child of this operator
  * @param sharedAttributeOrder shared attribute orders
  */
case class LocalPreparationExec(
    child: SeccoPlan,
    sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]],
    sharedPreparationTask: SharedParameter[mutable.ArrayBuffer[PreparationTask]]
) extends SeccoPlan {

  override lazy val statisticKeeper: StatisticKeeper = child.statisticKeeper

  def attributeOrder: mutable.ArrayBuffer[String] = sharedAttributeOrder.res

  override def taskPartitioner(): PairPartitioner = child.taskPartitioner()

  def preparationTask: PreparationTask = sharedPreparationTask.res.head

  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[OldInternalBlock] = {
    child.execute().map { block =>
      preparationTask match {
        case PreparationTask.ConstructTrie =>
          block match {

            case RowIndexedBlockOld(output, shareVector, blockContent) =>
              val content = blockContent.content
              val schema = output

              if (
                attributeOrder
                  .filter(schema.contains) != schema
              ) {
                Alg.reorder(attributeOrder, schema, content)
              }

              val trie = ArrayTrie(content, schema.size)

              TrieIndexedBlockOld(
                attributeOrder.filter(output.contains),
                shareVector,
                TrieBlockContent(trie)
              )
            case RowBlockOld(output, blockContent) =>
              val content = blockContent.content
              val schema = output

//              Alg.reorder(attributeOrder, schema, content)
              if (
                attributeOrder
                  .filter(schema.contains) != schema
              ) {
                Alg.reorder(attributeOrder, schema, content)
              }

              val trie = ArrayTrie(content, schema.size)

              TrieBlockOld(
                attributeOrder.filter(output.contains),
                TrieBlockContent(trie)
              )
            case _ =>
              throw new Exception(
                s"block must be of type `IndexedRowBlock` or `RowBlock`"
              )
          }
        case PreparationTask.Sort =>
          block match {
            case RowIndexedBlockOld(output, shareVector, blockContent) =>
              val content = blockContent.content
              val schema = output

              //reorder and sort the relation in lexical order
              Alg.reorder(attributeOrder, schema, content)
              val comparator = new LexicalOrderComparator(schema.size)
              java.util.Arrays.sort(content, comparator)

              ConsecutiveRowIndexedBlockOld(
                attributeOrder.filter(output.contains),
                shareVector,
                ConsecitiveRowBlockContent(
                  ConsecutiveRowArray(schema.size, content)
                )
              )

            case RowBlockOld(output, blockContent) =>
              val content = blockContent.content
              val schema = output

              println(s"attributeOrder:${attributeOrder}, schema:${schema}")
              //reorder and sort the relation in lexical order
              Alg.reorder(attributeOrder, schema, content)
              val comparator = new LexicalOrderComparator(schema.size)
              java.util.Arrays.sort(content, comparator)

              ConsecutiveRowBlockOld(
                attributeOrder.filter(output.contains),
                ConsecitiveRowBlockContent(
                  ConsecutiveRowArray(schema.size, content)
                )
              )
            case _ =>
              throw new Exception(
                s"block must be of type `IndexedRowBlock` or `RowBlock`"
              )
          }
        case PreparationTask.ConstructHashMap(keyAttr) =>
          block match {
            case RowIndexedBlockOld(output, shareVector, blockContent) =>
              val content = blockContent.content
              val schema = output
              val localAttributeOrder =
                attributeOrder.filter(schema.contains).toArray
              val localKeyOrder =
                attributeOrder.filter(keyAttr.contains).toArray

              //reorder and sort the relation in lexical order
              Alg.reorder(attributeOrder, schema, content)
              val comparator = new LexicalOrderComparator(schema.size)
              java.util.Arrays.sort(content, comparator)

              HashMapIndexedBlockOld(
                attributeOrder.filter(output.contains),
                shareVector,
                HashMapBlockContent(
                  InternalRowHashMap(
                    localKeyOrder,
                    localAttributeOrder,
                    content
                  )
                )
              )

            case RowBlockOld(output, blockContent) =>
              val content = blockContent.content
              val schema = output
              val localAttributeOrder =
                attributeOrder.filter(schema.contains).toArray
              val localKeyOrder =
                attributeOrder.filter(keyAttr.contains).toArray

              //reorder and sort the relation in lexical order
              Alg.reorder(attributeOrder, schema, content)
              val comparator = new LexicalOrderComparator(schema.size)
              java.util.Arrays.sort(content, comparator)

              HashMapBlockOld(
                attributeOrder.filter(output.contains),
                HashMapBlockContent(
                  InternalRowHashMap(
                    localKeyOrder,
                    localAttributeOrder,
                    content
                  )
                )
              )
            case _ =>
              throw new Exception(
                s"block must be of type `IndexedRowBlock` or `RowBlock`"
              )
          }
      }

    }
  }

  //fixme: figure out why in PullPairExchange, we should use partitionOutput instead of output (localAttributeOrder)
  /** The output attributes */
  override def outputOld: Seq[String] = {
    //DEBUG
//    println(child.output)
//    attributeOrder.filter(child.output.contains)
    child.outputOld
  }

//  def partitionOutput = child.output

  override def children: Seq[SeccoPlan] = Seq(child)
}
