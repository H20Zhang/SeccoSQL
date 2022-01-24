package org.apache.spark.secco.execution.plan.computation.newIter

import org.apache.spark.secco.execution.storage.Utils
import org.apache.spark.secco.execution.storage.block.{GenericInternalBlock, InternalBlock}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{Attribute, AttributeReference, Expression}
import org.apache.spark.secco.expression.codegen.{GeneratePredicate, PredicateFunc}
import org.apache.spark.secco.types.{StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/** The base class for performing selection via iterator. */
sealed abstract class BaseSelectIterator extends SeccoIterator {

  /** The condition for filtering the tuples */
  def condition(): Expression

  /** The generated function for filtering the tuples */
  def filterFunc(): PredicateFunc
}

/** The iterator that performs selection operation */
case class SelectIterator(childIter: SeccoIterator, condition: Expression)
    extends BaseSelectIterator {

  private var row: InternalRow = _
  private var hasNextCacheValid = false
  private var hasNextCache: Boolean = _

  override lazy val filterFunc: PredicateFunc = GeneratePredicate.generate(condition, childIter.localAttributeOrder())

  override def localAttributeOrder(): Array[Attribute] = childIter.localAttributeOrder()

  override def isSorted(): Boolean = childIter.isSorted()

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = {
    val rowArrayBuffer = ArrayBuffer[InternalRow]()
//    while(hasNext) rowArrayBuffer += next()
//    val structFieldsArray =
//    {
//      //lgh TODO: consider when localAttributeOrder elements are not instances of AttributeReference
//      localAttributeOrder().map(_.asInstanceOf[AttributeReference]).map(i => StructField(i.name, i.dataType))
//    }
//    val schema = StructType(structFieldsArray)
    for (row <- childIter.results().toArray()){
      if(filterFunc.eval(row)) rowArrayBuffer += row
    }
    val schema = StructType.fromAttributes(localAttributeOrder())
    GenericInternalBlock(rowArrayBuffer.toArray, schema)
  }

//  def row_equal(a: InternalRow, b: InternalRow): Boolean = {
//    localAttributeOrder().map(a.get(0, _.asInstanceOf))
//  }

  override def hasNext: Boolean = {
    if(!hasNextCacheValid)
    {
      var evalResult = false
      while(!evalResult && childIter.hasNext){
        row = childIter.next()
        evalResult = filterFunc.eval(row)
      }
      hasNextCache = evalResult
      hasNextCacheValid = true
    }
    hasNextCache
  }

  override def next(): InternalRow = {
    if(!hasNext) throw new NoSuchElementException("next on empty iterator")
    else
    {
      hasNextCacheValid = false
      row
    }
  }

  override def children: Seq[SeccoIterator] = childIter :: Nil
}

/** The iterator that performs selection operation and also support index-like operations if its
  * chlidIter support index-like operations.
  */
case class IndexableSelectIterator(
    childIter: SeccoIterator with IndexableSeccoIterator,
    condition: Expression,
    keyAttributes: Array[Attribute]
) extends BaseSelectIterator
    with IndexableSeccoIterator {

  private var row: InternalRow = _
  private var hasNextCacheValid = false
  private var hasNextCache: Boolean = _

  override def filterFunc(): PredicateFunc = GeneratePredicate.generate(condition)

  override def setKey(key: InternalRow): Boolean = {
    if(childIter.setKey(key))
      filterFunc().eval(childIter.getOneRow(key).get)
    else
      false
  }

  //lgh TODO: consider when multiple results correspond to one key
  override def getOneRow(key: InternalRow): Option[InternalRow] = {
//    var result: Option[InternalRow] = None
    var evalResult = false
    val childRow = childIter.getOneRow(key)
    if(childRow.isDefined) evalResult = filterFunc().eval(childRow.get)
    if(evalResult) childRow else None
//    while(!evalResult && childRow.isDefined){
//      childRow = childIter.getOneRow(key)
//      if(childRow.isDefined) evalResult = filterFunc().eval(childRow.get)
//    }
  }

  //lgh TODO: consider when multiple results correspond to one key
  override def unsafeGetOneRow(key: InternalRow): InternalRow = {
    var evalResult = false
    val childRow = childIter.getOneRow(key)
    if(childRow.isDefined) evalResult = filterFunc().eval(childRow.get)
    if(evalResult) childRow.get else null
  }

  override def localAttributeOrder(): Array[Attribute] = childIter.localAttributeOrder()

  override def isSorted(): Boolean = childIter.isSorted()

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = {
//    val rowArrayBuffer = ArrayBuffer[InternalRow]()
//    while(hasNext) rowArrayBuffer += next()
//    val structFieldsArray = {
//      //lgh TODO: consider when localAttributeOrder elements are not instances of AttributeReference
//      localAttributeOrder().map(_.asInstanceOf[AttributeReference]).map(i => StructField(i.name, i.dataType))
//    }
//    val schema = StructType(structFieldsArray)
//    GenericInternalBlock(rowArrayBuffer.toArray, schema)
    val schema = StructType.fromAttributes(localAttributeOrder())
    val rowArrayBuffer = ArrayBuffer[InternalRow]()
    for(row <- childIter.results().toArray()){
      if(filterFunc().eval(row)) rowArrayBuffer += row
    }
    InternalBlock(rowArrayBuffer.toArray, schema)
  }

  override def hasNext: Boolean = {
    if(!hasNextCacheValid)
    {
      var evalResult = false
      while(!evalResult && childIter.hasNext){
        row = childIter.next()
        evalResult = filterFunc().eval(row)
      }
      hasNextCache = evalResult
      hasNextCacheValid = true
    }
    hasNextCache
  }

  override def next(): InternalRow = {
    if(!hasNext) throw new NoSuchElementException("next on empty iterator")
    else
    {
      hasNextCacheValid = false
      row
    }
  }

  override def children: Seq[SeccoIterator] = childIter :: Nil
}
