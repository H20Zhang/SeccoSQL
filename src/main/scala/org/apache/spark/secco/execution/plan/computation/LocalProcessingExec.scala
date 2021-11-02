package org.apache.spark.secco.execution.plan.computation

import org.apache.spark.secco.optimization.plan.JoinType._
import org.apache.spark.secco.execution.{SeccoPlan, _}
import org.apache.spark.secco.execution.plan.computation.iter.{
  SeccoIterator,
  IteratorFactory
}
import org.apache.spark.secco.execution.plan.support.FuncGenSupport
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/** A local computation physical operator.
  */
abstract class LocalProcessingExec extends SeccoPlan {}
//
//  /** shared parameter--- attribute order */
//  def sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]]
//
//  /** The global attribute order */
//  def attributeOrder: mutable.ArrayBuffer[String] = sharedAttributeOrder.res
//
//  /** The local attribute order */
//  def localAttributeOrder: Seq[String] =
//    attributeOrder.filter(outputOld.contains)
//
//  /** The output iterator */
//  def iterator(): SeccoIterator
//
//  /** The materialized result of [[iterator()]] */
//  def result(): InternalBlock = {
//
//    val result = iterator().result()
//    RowBlock(localAttributeOrder, RowBlockContent(result))
//
//  }
//
//  /** LocalExec cannot doExecute, as it is a global operation */
//  override protected def doExecute(): RDD[InternalBlock] = {
//    throw new Exception(
//      "LocalExec does not support doExecute, which is a global operation, and Local Exec " +
//        "is performed per InternalBlock. Try use result() instead."
//    )
//  }
//}
//
///** An operator that filters row using [[selectionExprs]] */
//case class LocalSelectExec(
//    child: LocalProcessingExec,
//    selectionExprs: Seq[(String, String, String)],
//    sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]]
//) extends LocalProcessingExec {
//
//  override def outputOld: Seq[String] = child.outputOld
//
//  /** The output iterator */
//  override def iterator(): SeccoIterator =
//    IteratorFactory.makeSelectIter(
//      child.iterator(),
//      selectionExprs,
//      localAttributeOrder.toArray
//    )
//
//  override def children: Seq[SeccoPlan] = Seq(child)
//
//  override def relationalSymbol: String =
//    s"𝜎[${selectionExprs.map(f => s"${f._1}${f._2}${f._3}").mkString("&&")}]"
//}
//
//case class LocalProjectExec(
//    child: LocalProcessingExec,
//    projectionList: Seq[String],
//    sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]]
//) extends LocalProcessingExec
//    with FuncGenSupport {
//
//  /** The output iterator */
//  override def iterator(): SeccoIterator =
//    IteratorFactory.makeProjectIter(
//      child.iterator(),
//      projectionList,
//      localAttributeOrder.toArray
//    )
//
//  override def children: Seq[SeccoPlan] = Seq(child)
//
//  /** The output attributes */
//  override def outputOld: Seq[String] = {
//    if (attributeOrder.nonEmpty) {
//      attributeOrder.filter(projectionList.contains)
//    } else {
//      projectionList
//    }
//  }
//
//  override def relationalSymbol: String =
//    s"∏[${projectionList.mkString(",")}]"
//}
//
//case class LocalSemiringAggregateExec(
//    child: LocalProcessingExec,
//    groupingList: Seq[String],
//    producedAttribute: String,
//    semiringList: (String, String),
//    sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]]
//) extends LocalProcessingExec {
//
//  /** The output iterator */
//  override def iterator(): SeccoIterator = {
//    IteratorFactory.makeAggregateIter(
//      child.iterator(),
//      groupingList,
//      semiringList,
//      localAttributeOrder.toArray
//    )
//  }
//
//  override def children: Seq[SeccoPlan] = Seq(child)
//
//  override def outputOld: Seq[String] = {
//    val attributes = groupingList :+ producedAttribute
//    if (attributeOrder.nonEmpty) {
//      attributeOrder.filter(attributes.contains)
//    } else {
//      attributes
//    }
//  }
//
//  override def relationalSymbol: String =
//    s"[${groupingList.mkString(",")}]𝝪[${s"${semiringList._1}(${semiringList._2})"}]"
//
//}
//
///** An operator that loads block at specific position */
//case class LocalPlaceHolderExec(
//    pos: Int,
//    outputOld: Seq[String],
//    sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]]
//) extends LocalProcessingExec {
//
//  private var optionBlock: Option[InternalBlock] = None
//
//  private lazy val block = result()
//
//  def setInternalBlock(block: InternalBlock) = {
//    this.optionBlock = Some(block)
//  }
//
//  /** The output iterator */
//  override def iterator(): SeccoIterator = {
//    block match {
//      case r: RowBlock =>
//        IteratorFactory.makeArrayTableIter(
//          r.blockContent.content,
//          localAttributeOrder.toArray
//        )
//      case r: RowIndexedBlock =>
//        IteratorFactory.makeArrayTableIter(
//          r.blockContent.content,
//          localAttributeOrder.toArray
//        )
//      case r: ConsecutiveRowBlock =>
//        IteratorFactory.makeConsecutiveRowArrayTableIter(
//          r.blockContent.content,
//          localAttributeOrder.toArray
//        )
//      case r: ConsecutiveRowIndexedBlock =>
//        IteratorFactory.makeConsecutiveRowArrayTableIter(
//          r.blockContent.content,
//          localAttributeOrder.toArray
//        )
//      case t: TrieIndexedBlock =>
//        IteratorFactory.makeTrieTableIter(
//          t.blockContent.content,
//          localAttributeOrder.toArray
//        )
//      case t: TrieBlock =>
//        IteratorFactory.makeTrieTableIter(
//          t.blockContent.content,
//          localAttributeOrder.toArray
//        )
//      case h: HashMapBlock =>
//        IteratorFactory.makeHashMapTableIter(
//          h.blockContent.content,
//          localAttributeOrder.toArray
//        )
//      case h: HashMapIndexedBlock =>
//        IteratorFactory.makeHashMapTableIter(
//          h.blockContent.content,
//          localAttributeOrder.toArray
//        )
//      case b: InternalBlock =>
//        throw new Exception(s"${b.getClass} not support iterator()")
//    }
//
//  }
//
//  /** The materialized result of [[iterator()]] */
//  override def result(): InternalBlock = {
//    optionBlock match {
//      case Some(internalBlock) =>
//        internalBlock match {
//          case MultiTableIndexedBlock(output, shareVector, indexedBlocks) =>
//            indexedBlocks(pos)
//          case rb @ RowBlock(output, blockContent) =>
//            assert(pos == 0, s"pos:${pos} out of range")
//            rb
//          case rhb @ RowIndexedBlock(output, shareVector, blockContent) =>
//            assert(pos == 0, s"pos:${pos} out of range")
//            rhb
//          case cb @ ConsecutiveRowBlock(output, blockContent) =>
//            assert(pos == 0, s"pos:${pos} out of range")
//            cb
//          case crb @ ConsecutiveRowIndexedBlock(
//                output,
//                shareVector,
//                blockContent
//              ) =>
//            assert(pos == 0, s"pos:${pos} out of range")
//            crb
//          case thb @ TrieBlock(output, blockContent) =>
//            assert(pos == 0, s"pos:${pos} out of range")
//            thb
//          case hhb @ HashMapBlock(output, blockContent) =>
//            assert(pos == 0, s"pos:${pos} out of range")
//            hhb
//          case _ =>
//            throw new Exception(
//              s"${internalBlock.getClass} is not supported by ${getClass}"
//            )
//        }
//      case None =>
//        throw new Exception("optionBlock must be set")
//    }
//  }
//
//  override def children: Seq[SeccoPlan] = Nil
//
//  override def relationalSymbol: String = s"[${pos.toString}]"
//}
//
//case class LocalCartesianProductExec(
//    children: Seq[LocalProcessingExec],
//    sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]]
//) extends LocalProcessingExec {
//
////  lazy val baseOp = {
////    val l = children(0)
////    val r = children(1)
////    if (
////      localAttributeOrder.containsSlice(
////        l.localAttributeOrder
////      ) && localAttributeOrder(0) == l.localAttributeOrder(0)
////    ) {
////      l
////    } else {
////      r
////    }
////  }
////
////  lazy val indexOp = {
////    children.diff(Seq(baseOp)).head
////  }
//
//  lazy val baseOp = children(0)
//  lazy val indexOp = children(1)
//
//  /** The output iterator */
//  override def iterator(): SeccoIterator =
//    IteratorFactory.makeCartesianProductIter(
//      baseOp.iterator(),
//      indexOp.iterator(),
//      localAttributeOrder.toArray
//    )
//
//  /** The output attributes */
//  override def outputOld: Seq[String] = {
//    val attributes = children.flatMap(_.outputOld).distinct
//
//    if (attributeOrder.nonEmpty) {
//      attributeOrder.filter(attributes.contains)
//    } else {
//      attributes
//    }
//
//  }
//
//  override def relationalSymbol: String = s"⨉"
//}
//
//case class LocalJoinExec(
//    children: Seq[LocalProcessingExec],
//    joinType: JoinType,
//    sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]]
//) extends LocalProcessingExec {
//
////  lazy val baseOp = {
////    val l = children(0)
////    val r = children(1)
////    if (
////      localAttributeOrder.containsSlice(
////        l.localAttributeOrder
////      ) && localAttributeOrder(0) == l.localAttributeOrder(0)
////    ) {
////      l
////    } else {
////      r
////    }
////  }
////
////  lazy val indexOp = {
////    children.diff(Seq(baseOp)).head
////  }
//
//  lazy val baseOp = children(0)
//  lazy val indexOp = children(1)
//
//  /** The output iterator */
//  override def iterator(): SeccoIterator = {
//    joinType match {
//      case GHD     => genGHDJoinIter()
//      case FKFK    => genBinaryJoinIter()
//      case GHDFKFK => genBinaryJoinIter()
//      case PKFK    => genBinaryJoinIter()
//      case _       => throw new Exception("not supported join type")
//    }
//  }
//
//  private def genGHDJoinIter() = {
//    val tries = children
//      .map(_.result())
//      .map { block =>
//        block match {
//          case TrieIndexedBlock(output, shareVector, blockContent) =>
//            blockContent.content
//          case _ =>
//            throw new Exception(
//              s"result() of children of ${getClass} should be of ${TrieIndexedBlock.getClass}, current class is ${block.getClass}"
//            )
//        }
//      }
//      .toArray
//
//    IteratorFactory.makeLeapFrogJoinIter(
//      tries,
//      children.map(_.localAttributeOrder),
//      localAttributeOrder.toArray
//    )
//  }
//
//  private def genBinaryJoinIter() = {
//    IteratorFactory.makeBinaryJoinIter(
//      baseOp.iterator(),
//      indexOp.iterator(),
//      localAttributeOrder.toArray
//    )
//  }
//
//  /** The output attributes */
//  override def outputOld: Seq[String] = {
//    val attributes = children.flatMap(_.outputOld).distinct
//
//    if (attributeOrder.nonEmpty) {
//      attributeOrder.filter(attributes.contains)
//    } else {
//      attributes
//    }
//  }
//
//  override def relationalSymbol: String = s"⋈"
//}
//
/////** An operator that rename the output attributes */
////case class LocalRenameExec(
////                            child: LocalExec,
////                            isRoot: Boolean,
////                            sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]],
////                            attrRenameMap: Map[String, String],
////                            preferredOrder: Seq[String] = Nil
////                          ) extends LocalExec {
////
////  /** The output iterator */
////  override def iterator(): Iterator[InternalRow] = child.iterator()
////
////  /** The output iterator with prefix given */
////  override def indexIterator(prefix: InternalRow): Iterator[InternalRow] =
////    child.indexIterator(prefix)
////
////  /** The materialized result of [[iterator()]] */
////  override def result(): InternalBlock = child.result()
////
////  override def children: Seq[SeccoPlan] = Seq(child)
////
////  /** The actual attribute order of this class */
////  override def localAttributeOrder: Seq[String] =
////    child.localAttributeOrder.map(attrRenameMap)
////
////  override def output: Seq[String] = child.output.map(attrRenameMap)
////}
//
////case class LocalDiffExec(
////                          left: SeccoPlan,
////                          right: SeccoPlan,
////                          isRoot: Boolean,
////                          sharedAttributeOrder: SharedParameter[mutable.ArrayBuffer[String]],
////                          preferredOrder: Seq[String] = Nil
////                        ) extends LocalExec {
////
////  /** The output iterator */
////  override def iterator(): Iterator[InternalRow] = ???
////
////  /** The output iterator with prefix given */
////  override def indexIterator(prefix: InternalRow): Iterator[InternalRow] = ???
////
////  /** The materialized result of [[iterator()]] */
////  override def result(): InternalBlock = ???
////
////  override def children: Seq[SeccoPlan] = Seq(left, right)
////
////  /** The output attributes */
////  override def output: Seq[String] = left.output
////}
