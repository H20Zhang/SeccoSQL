package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.Utils
import org.apache.spark.secco.execution.storage.block._
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.codegen.{BaseProjectionFunc, GenerateSafeProjection}
import org.apache.spark.secco.expression._
import org.apache.spark.secco.types._

import scala.collection.mutable.ArrayBuffer

/** The base class for performing project via iterator. */
sealed abstract class BaseProjectIterator extends SeccoIterator {

  def projectionList: Array[NamedExpression]

  def projectionFunction: BaseProjectionFunc

}

/** The iterator that performs projection */
case class ProjectIterator(
    childIter: SeccoIterator,
    projectionList: Array[NamedExpression]
) extends BaseProjectIterator {

//  override def projectionFunctions: Array[BaseProjectionFunc] = {
//    val result = projectionList.map(expr => GenerateSafeProjection.generate(Seq(expr)).asInstanceOf[BaseProjectionFunc])
//    result
//  }
  override lazy val projectionFunction: BaseProjectionFunc =
    GenerateSafeProjection.generate(projectionList, childIter.localAttributeOrder()).asInstanceOf[BaseProjectionFunc]

  override def localAttributeOrder(): Array[Attribute] = projectionList.map(_.toAttribute)

  override def isSorted(): Boolean = childIter.isSorted()

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = {
    val schema = StructType.fromAttributes(localAttributeOrder())
    val childRows = childIter.results().toArray()
    val resultRows = childRows.map(projectionFunction(_).copy())
    InternalBlock(resultRows, schema)
  }

  override def hasNext: Boolean = childIter.hasNext

  override def next(): InternalRow = {
    if(!hasNext) throw new NoSuchElementException("next on empty iterator")
    else
    {
      val rawRow: InternalRow = childIter.next()
      val resultRow = projectionFunction(rawRow)
      UnsafeInternalRow.fromInternalRow(StructType.fromAttributes(localAttributeOrder()), resultRow)
    }
  }

  override def children: Seq[SeccoIterator] = childIter :: Nil
}

/** The iterator that performs projection and support index-like operation if childIter support index-like operations
  *
  * Note that: not all projectionList is valid in this context, as we need to get the inverse function of projectionList
  * to help us compute the key to query childIter, which may not always exists.
  *
  * Also, currently we only support compute the inverse function of very simple expressions in projectionList, i.e.,
  * simple arithmetic operations (x, x+y, x-y)
  */
case class IndexableProjectIterator(
    childIter: SeccoIterator with IndexableSeccoIterator,
    projectionList: Array[NamedExpression]
) extends BaseProjectIterator
    with IndexableSeccoIterator {

  private val projectFunc: codegen.Projection = GenerateSafeProjection.generate(projectionList)

  override def projectionFunction: BaseProjectionFunc =
    GenerateSafeProjection.generate(projectionList).asInstanceOf[BaseProjectionFunc]

  override def keyAttributes(): Array[Attribute] = ???

  override def setKey(key: InternalRow): Boolean = ???

  override def getOneRow(key: InternalRow): Option[InternalRow] = ???

  override def unsafeGetOneRow(key: InternalRow): InternalRow = ???

  override def localAttributeOrder(): Array[Attribute] = projectionList.map(_.toAttribute)

  override def isSorted(): Boolean = childIter.isSorted()

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = ??? // same with above

  override def hasNext: Boolean = childIter.hasNext

  override def next(): InternalRow = ??? // same with above

  override def children: Seq[SeccoIterator] = childIter :: Nil
}


//backup code segment --lgh

//1)
//  var rowCache: InternalRow = _
//  var cacheValid = false
//  var hasNextCache: Boolean = _
//  var rowCacheAssigned: Boolean = false

//2)
//    val result = Array(GenerateSafeProjection.generate(projectionList).asInstanceOf[BaseProjectionFunc])
//    val result = Array(GenerateSafeProjection.generate(projectionList).asInstanceOf[BaseProjectionFunc])

//3)
//  override def hasNext: Boolean = {
//    if(!cacheValid)
//    {
//      hasNextCache = childIter.hasNext
//      if(hasNextCache) {
//        rowCache = childIter.next()
//        rowCacheAssigned = true
//        for (projectFunc <- projectionFunctions) {
//          rowCache = projectFunc(rowCache)
//        }
//      }
//      cacheValid = true
//    }
//    hasNextCache
//  }

//4)
//    if(!hasNext) throw new NoSuchElementException("next on empty iterator")
//    else
//    {
//      cacheValid = false
//      rowCache
//    }

//5)
//      for (projectFunc <- projectionFunctions) {
//        row = projectFunc(row)
//      }

//6)
//    val rowArrayBuffer = ArrayBuffer[InternalRow]()
//    while(hasNext) rowArrayBuffer += next()