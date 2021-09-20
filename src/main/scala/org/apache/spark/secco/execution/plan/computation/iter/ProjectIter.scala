package org.apache.spark.secco.execution.plan.computation.iter

import org.apache.spark.secco.execution.{InternalDataType, InternalRow}
import org.apache.spark.secco.execution.plan.computation.utils.{
  BufferPool,
  InternalRowBufferPoolImpl,
  LexicalOrderComparator
}
import org.apache.spark.secco.execution.plan.support.FuncGenSupport

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait ProjectIter extends SeccoIterator with FuncGenSupport {
  def childIter: SeccoIterator
  def projectionList: Seq[String]
  lazy val (projectionFunc, projectedRow) =
    genProjectionFunc(localAttributeOrder, childIter.localAttributeOrder)

  override def result(): Array[InternalRow] = {
    val childResult = childIter.result()
    val outputRows = new Array[InternalRow](childResult.length)
    val numOutputRow = outputRows.length
    var i = 0
    while (i < numOutputRow) {
      val row = childResult(i)
      outputRows(i) = projectionFunc(row).clone()
      i += 1
    }

    val lexicalOrderComparator = new LexicalOrderComparator(
      localAttributeOrder.length
    )

    java.util.Arrays
      .sort(outputRows, lexicalOrderComparator)

    outputRows
  }

}

case class MatchedPrefixProjectIter(
    childIter: SeccoIterator,
    projectionList: Seq[String],
    localAttributeOrder: Array[String]
) extends ProjectIter {

  override def reset(prefix: InternalRow): SeccoIterator = {
    outputDefined = false
    isInitialized = false
    childIter.reset(prefix)
    this
  }

  val localAttributeSize: Int = projectionList.size
  var outputDefined = false
  var isInitialized = false
  val outputRow = new Array[InternalDataType](localAttributeSize)

  private def updateOutput(): Unit = {
    var i = 0
    while (i < localAttributeSize) {
      outputRow(i) = projectedRow(i)
      i += 1
    }
    outputDefined = true
  }

  private def checkBoundary() = {
    var isBoundaryEncountered = false
    var i = 0
    while (i < localAttributeSize) {
      if (projectedRow(i) != outputRow(i)) {
        isBoundaryEncountered = true
      }
      i += 1
    }
    isBoundaryEncountered
  }

  override def hasNext: Boolean = {

    if (outputDefined) {
      return true
    }

    while (childIter.hasNext) {
      val row = childIter.next()
      projectionFunc(row)

      if (!isInitialized) {
        updateOutput()
        isInitialized = true
        return outputDefined
      } else {
        val isBoundaryEncountered = checkBoundary()
        if (isBoundaryEncountered) {
          updateOutput()
          return outputDefined
        }
      }
    }

    outputDefined
  }

  override def next(): InternalRow = {
    outputDefined = false
    outputRow
  }

  override def result(): Array[InternalRow] = {
    val buffer = ArrayBuffer[InternalRow]()
    while (hasNext) {
      buffer += next().clone()
    }
    buffer.toArray
  }

}

case class PartiallyMatchedPrefixProjectIter(
    childIter: SeccoIterator,
    projectionList: Seq[String],
    localAttributeOrder: Array[String]
) extends ProjectIter {

  lazy val divergePos: Int = {
    var i = 0
    var hasEnd = false
    val size = projectionList.size
    //where projectionList and child.attributeOrder diverage
    var pos = 0
    while (i < size && !hasEnd) {
      if (projectionList(i) != childIter.localAttributeOrder(i)) {
        pos = i
        hasEnd = true
      }
      i += 1
    }
    pos
  }

  val groupingList: Array[String] = projectionList.slice(0, divergePos).toArray
  val localAttributeSize: Int = localAttributeOrder.length

  private val emptyIt = new EmptyIterator

  override def reset(prefix: InternalRow): SeccoIterator = {
    inputBufferPool.reset()
//    outputBufferPool.reset()
    nextGroupKeyDefined = false
    nextProjectedIter = emptyIt
    nextGroup.clear()
    childIter.reset(prefix)
    this
  }

  val (groupProjectionFunc, groupProjectedRow) = {
    genProjectionFunc(groupingList, childIter.localAttributeOrder)
  }

  val groupArity: Int = groupingList.length
  var nextGroup: ArrayBuffer[InternalRow] = ArrayBuffer()
  val nextGroupKey = new Array[InternalDataType](groupArity)
  var nextGroupKeyDefined = false
  var nextProjectedIter: Iterator[InternalRow] = Iterator.empty
//  val outputBufferPool: BufferPool[mutable.WrappedArray[InternalDataType]] =
//    new WrappedArrayBufferPoolImpl(localAttributeSize)
  val inputBufferPool: BufferPool[InternalRow] =
    new InternalRowBufferPoolImpl(childIter.localAttributeOrder.length)

  //update nextGroupKey based on row
  private def updateNextGroupKey(row: InternalRow): Unit = {
    inputBufferPool.reset()
    //get group projected row
    groupProjectionFunc(row)

    var i = 0
    while (i < groupArity) {
      nextGroupKey(i) = groupProjectedRow(i)
      i += 1
    }

    //add row to group
    nextGroup += inputBufferPool.newInstanceWithArray(row)
    nextGroupKeyDefined = true
  }

  //check if a new group key is encountered.
  private def checkGroupBoundary(): Boolean = {
    var newGroupEncountered = false
    var i = 0
    while (i < groupArity && !newGroupEncountered) {
      if (groupProjectedRow(i) != nextGroupKey(i)) {
        newGroupEncountered = true
      }
      i += 1
    }
    newGroupEncountered
  }

  val lexicalOrderComparator = new LexicalOrderComparator(
    localAttributeSize
  )
  //make nextProjectedIter using nextGroup
  private def makeNextProjectedIter() = {
//    outputBufferPool.reset()
    val projectedNextGroupSet =
      mutable.HashSet[mutable.WrappedArray[InternalDataType]]()
    nextGroup.foreach { row =>
      projectionFunc(row)
      val arr = new Array[InternalDataType](localAttributeSize)
//      val arr = internalRowBufferPool.newInstance()
      val wrappedArr = mutable.WrappedArray.make[InternalDataType](arr)
//      val wrappedArr = outputBufferPool.newInstance()
      var i = 0
      while (i < localAttributeSize) {
        wrappedArr(i) = projectedRow(i)
        i += 1
      }
      projectedNextGroupSet.add(wrappedArr)
    }
//    val projectedNextGroupArrayBuffer = ArrayBuffer[InternalRow]()
//    projectedNextGroupSet.foreach { projectedRow =>
//      projectedNextGroupArrayBuffer += projectedRow.array
//    }
//
//    val projectedNextGroupArray = projectedNextGroupArrayBuffer.toArray

    val projectedNextGroupArray =
      new Array[InternalRow](projectedNextGroupSet.size)
    var i = 0
    projectedNextGroupSet.foreach { projectedRow =>
      projectedNextGroupArray(i) = projectedRow.array
      i += 1
    }

    java.util.Arrays
      .sort(projectedNextGroupArray, lexicalOrderComparator)

    //clear next group
    nextGroup.clear()

    projectedNextGroupArray.iterator
  }

  override def hasNext: Boolean = {

    if (nextProjectedIter.hasNext) {
      true
    } else {
      while (childIter.hasNext) {
        val row = childIter.next()

//        //DEBUG
//        pprint.pprintln(row)
        if (!nextGroupKeyDefined) {
          updateNextGroupKey(row)
        } else {
          //get group projected row
          groupProjectionFunc(row)

          //check the boundary of current group
          val newGroupEncountered = checkGroupBoundary()

          if (newGroupEncountered) {
            //make nextProjectedIter
            nextProjectedIter = makeNextProjectedIter()

            //update nextGroupKey
            updateNextGroupKey(row)

            return nextProjectedIter.hasNext
          } else {
            nextGroup += inputBufferPool.newInstanceWithArray(row)
          }
        }
      }

      //handle the case of the last group in iterator.
      if (nextGroup.nonEmpty) {
        nextProjectedIter = makeNextProjectedIter()
      }

      nextProjectedIter.hasNext
    }
  }

  override def next(): InternalRow = {
    nextProjectedIter.next()
  }
}

case class NoMatchedPrefixProjectIter(
    childIter: SeccoIterator,
    projectionList: Seq[String],
    localAttributeOrder: Array[String]
) extends ProjectIter {

  override def reset(prefix: InternalRow): SeccoIterator = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???
}
