package org.apache.spark.secco.execution.plan.computation.newIter

import org.apache.spark.secco.execution.storage.block.{
  InternalBlock,
  InternalBlockBuilder,
  TrieInternalBlock,
  TrieInternalBlockBuilder
}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.expression.codegen.{
  BaseUnaryIteratorProducer,
  GenerateLeapFrogJoinIterator,
  GenerateUnaryIterator
}
import org.apache.spark.secco.types.StructType

/** The base class for performing leapfrog join via iterator */
sealed abstract class BaseLeapFrogJoinIterator extends SeccoIterator {

  /** The tries of the relations */
  def tries: Array[TrieInternalBlock]
}

/** The iterator that performs leapfrog join
  * @param children the children which is IndexableTableIterator with TrieInternalRow as block
  * @param localAttributeOrder the attribute order to perform leapfrog join
  *
  * Note: the output of leapfrog join is always sorted w.r.t localAttributeOrder.
  */
case class LeapFrogJoinIterator(
    children: Seq[SeccoIterator],
    localAttributeOrder: Array[Attribute]
//      children: Seq[IndexableTableIterator],
) extends BaseLeapFrogJoinIterator {

  private val childrenTries = getChildrenTries

  private def getChildrenTries: Array[TrieInternalBlock] = {
    children.map { child =>
      TrieInternalBlock(
        child.results().toArray(),
        StructType.fromAttributes(child.localAttributeOrder())
      )
    }.toArray
  }

  private val rowSchema = StructType.fromAttributes(localAttributeOrder)
  private val childrenSchemas = children.map(_.localAttributeOrder().toSeq)

  private val producer = GenerateLeapFrogJoinIterator.generate(
    (localAttributeOrder, childrenSchemas)
  )
  private val iterator = producer.getIterator(childrenTries)

  override def tries: Array[TrieInternalBlock] = childrenTries

  override def isSorted(): Boolean = true

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = {
    val builder = new TrieInternalBlockBuilder(rowSchema)
    val childrenTriesInner = getChildrenTries
    val iterInner = producer.getIterator(childrenTriesInner)
    while (iterInner.hasNext) {
      builder.add(iterInner.next().copy())
    }
    builder.build()
  }

  override def hasNext: Boolean = iterator.hasNext

  override def next(): InternalRow = iterator.next()
}

/** The iterator that performs leapfrog join with index-like operations
  * @param children the children which is IndexableTableIterator with TrieInternalRow as block
  * @param localAttributeOrder the attribute order to perform leapfrog join
  *                            @param keyAttributes the key attributes
  *
  * Note: the output of leapfrog join is always sorted w.r.t localAttributeOrder.
  * The keyAttributes should be match the prefix of localAttributeOrder.
  */
case class IndexableLeapFrogIterator(
    children: Seq[IndexableTableIterator],
    localAttributeOrder: Array[Attribute],
    keyAttributes: Array[Attribute]
) extends BaseLeapFrogJoinIterator
    with IndexableSeccoIterator {

  override def tries: Array[TrieInternalBlock] = ???

  override def setKey(key: InternalRow): Boolean = ???

  override def getOneRow(key: InternalRow): Option[InternalRow] = ???

  override def unsafeGetOneRow(key: InternalRow): InternalRow = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}

// lgh code fragments

// 1.
//// LeapFrogJoinIterator implemented using GenerateUnaryIterator
///** The iterator that performs leapfrog join
//  * @param children the children which is IndexableTableIterator with TrieInternalRow as block
//  * @param localAttributeOrder the attribute order to perform leapfrog join
//  *
//  * Note: the output of leapfrog join is always sorted w.r.t localAttributeOrder.
//  */
//case class LeapFrogJoinIterator(
//                                 children: Seq[IndexableTableIterator],
//                                 localAttributeOrder: Array[Attribute]
//                               ) extends BaseLeapFrogJoinIterator {
//
//  private val childrenTries = children.map {
//    child =>
//      TrieInternalBlock(child.results().toArray(), StructType.fromAttributes(child.localAttributeOrder))
//  }.toArray
//
//  private val arity: Int = localAttributeOrder.length
//  private val rowSchema = StructType.fromAttributes(localAttributeOrder)
//  private val producers: Seq[BaseUnaryIteratorProducer] = (1 to arity).map {
//    curArity =>
//      GenerateUnaryIterator.generate((localAttributeOrder.slice(0, curArity),
//        children.map(_.localAttributeOrder.toSeq)))
//  }
//
//  var iterators: Array[java.util.Iterator[AnyRef]] = new Array[java.util.Iterator[AnyRef]](arity)
//  //  private var rowCache: InternalRow = InternalRow(new Array[Any](arity))
//  private val arrayCache: Array[Any] = new Array[Any](arity)
//  private var hasNextCache: Boolean = _
//  private var hasNextCacheValid = false
//
//  init()
//
//  private def init(): Unit = {
//    var i = 0
//    while (!hasNextCacheValid && i < arity) {
//      val curIter = producers(i).getIterator(InternalRow(arrayCache.slice(0, i)), childrenTries)
//      iterators(i) = curIter
//      if (curIter.hasNext) {
//        arrayCache(i) = curIter.next()
//      }
//      else {
//        hasNextCache = false
//        hasNextCacheValid = true
//      }
//      i += 1
//    }
//  }
//
//  override def tries: Array[TrieInternalBlock] = childrenTries
//
//  override def isSorted(): Boolean = true
//
//  override def isBreakPoint(): Boolean = false
//
//  override def results(): InternalBlock = {
//    val builder = new TrieInternalBlockBuilder(rowSchema)
//    while(hasNext){
//      builder.add(next())
//    }
//    builder.build()
//  }
//
//  override def hasNext: Boolean = {
//    if (hasNextCacheValid) return hasNextCache
//    var i = arity - 1
//    while(!hasNextCacheValid){
//      val curIter = iterators(i)
//      if (curIter.hasNext) {
//        arrayCache(i) = curIter.next()
//        if(i == arity - 1){
//          hasNextCache = true
//          hasNextCacheValid = true
//        }
//        else{
//          i += 1
//          iterators(i) = producers(i).getIterator(InternalRow(arrayCache.slice(0, i)), childrenTries)
//        }
//      }
//      else {
//        if(i == 0)
//        {
//          hasNextCache = false
//          hasNextCacheValid = true
//        }
//        else{
//          i -= 1
//        }
//      }
//    }
//    return hasNextCache
//  }
//
//  override def next(): InternalRow = {
//    if(isBreakPoint())  throw new NoSuchMethodException()
//    if(!hasNext) throw new NoSuchElementException("next on empty iterator")
//    else
//    {
//      hasNextCacheValid = false
//      InternalRow(arrayCache)
//    }
//  }
//}
