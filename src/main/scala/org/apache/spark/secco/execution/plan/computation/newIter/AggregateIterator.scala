package org.apache.spark.secco.execution.plan.computation.newIter

import org.apache.avro.reflect.MapEntry
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.expression.aggregate._
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.codegen.{
  BaseProjectionFunc,
  GenerateMutableProjection,
  GenerateSafeProjection,
  MutableProjection,
  Projection
}
import org.apache.spark.secco.types.{AnyDataType, StructType}
import org.apache.spark.secco.util.misc.LogAble
import org.spark_project.dmg.pmml.True

import java.util
import scala.collection.mutable.ArrayBuffer

/** The base class for performing aggregation via iterator */
sealed abstract class BaseAggregateIterator extends SeccoIterator with LogAble {

  /** The group attributes */
  def groupingExpression: Array[NamedExpression]

  /** The aggregate expressions. */
  def aggregateExpressions: Array[NamedExpression]

}

/** The iterator that performs aggregation */
case class AggregateIterator(
    childIter: SeccoIterator,
    groupingExpressions: Array[NamedExpression],
    rawAggregateFunctions: Array[AggregateFunction]
//                              aggregateAttributes:Array[Attribute]
//                              resultExpressions: Array[NamedExpression]
    //    aggregateExpressions: Array[NamedExpression]
) extends SeccoIterator {

  val initialInputBufferOffset = 0

  protected val groupingProjection: Projection =
    GenerateSafeProjection.generate(
      groupingExpressions,
      childIter.localAttributeOrder()
    )
  protected val groupingAttributes: Array[Attribute] =
    groupingExpressions.map(_.toAttribute)

  protected val aggregateFunctions: Array[AggregateFunction] =
    initializeAggregateFunctions(rawAggregateFunctions)

  private val inputAttributes: Seq[Attribute] =
    childIter.localAttributeOrder().toSeq

  protected val processRow: (InternalRow, InternalRow) => Unit =
    generateProcessRow(aggregateFunctions, inputAttributes)

  // The projection used to initialize buffer values for all expression-based aggregates.
  protected[this] val expressionAggInitialProjection: MutableProjection = {
    val initExpressions = rawAggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.initialValues
      // For the positions corresponding to imperative aggregate functions, we'll use special
      // no-op expressions which are ignored during projection code-generation.
      case i: ImperativeAggregate =>
        Seq.fill(i.aggBufferAttributes.length)(NoOp)
    }
    println(s"in val expressionAggInitialProjection")
    GenerateMutableProjection.generate(initExpressions, Nil)
  }

  // Positions of those imperative aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are imperative aggregate functions.
  // ImperativeAggregateFunctionPositions will be [1, 2].
  protected[this] val allImperativeAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < aggregateFunctions.length) {
      aggregateFunctions(i) match {
        case agg: DeclarativeAggregate =>
        case _                         => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // All imperative AggregateFunctions.
  protected[this] val allImperativeAggregateFunctions
      : Array[ImperativeAggregate] =
    allImperativeAggregateFunctionPositions
      .map(aggregateFunctions)
      .map(_.asInstanceOf[ImperativeAggregate])

  protected val generateOutput: (InternalRow, InternalRow) => InternalRow =
    generateResultProjection()

  /** Start processing input rows.
    */
  private[this] val aggBufferIterator
      : java.util.Iterator[java.util.Map.Entry[InternalRow, InternalRow]] =
    processInputs(childIter)

  // Initialize all AggregateFunctions by binding references if necessary,
  // and set inputBufferOffset and mutableBufferOffset.
  protected def initializeAggregateFunctions(
      aggFunctions: Seq[AggregateFunction]
  ): Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    val initializedFunctions = new Array[AggregateFunction](aggFunctions.length)
    var i = 0
    val inputAttributeSeq: AttributeSeq = inputAttributes
    for (func <- aggFunctions) {
      // We need to create BoundReferences if the function is not an
      // expression-based aggregate function (it does not support code-gen) and the mode of
      // this function is Partial or Complete because we will call eval of this
      // function's children in the update method of this aggregate function.
      // Those eval calls require BoundReferences to work.
      val funcWithBoundReferences: AggregateFunction =
        if (func.isInstanceOf[ImperativeAggregate])
          BindReferences.bindReference(func, inputAttributeSeq)
        else func

      val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
        case function: ImperativeAggregate =>
          // Set mutableBufferOffset for this function. It is important that setting
          // mutableBufferOffset happens after all potential bindReference operations
          // because bindReference will create a new instance of the function.
          function.withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      initializedFunctions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    initializedFunctions
  }

  // Initializing the function used to generate the output row.
  protected def generateResultProjection()
      : (InternalRow, InternalRow) => InternalRow = {
    val joinedRow = new JoinedRow
    val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    if (aggregateFunctions.nonEmpty) {
      val evalExpressions = aggregateFunctions.map {
        case ae: DeclarativeAggregate => ae.evaluateExpression
        case agg: AggregateFunction   => NoOp
      }
      val aggregateResult = new GenericInternalRow(
        aggregateFunctions.map(_.dataType).length
      )
      val expressionAggEvalProjection =
        GenerateMutableProjection.generate(evalExpressions, bufferAttributes)
      expressionAggEvalProjection.target(aggregateResult)

      (currentGroupingKey: InternalRow, currentBuffer: InternalRow) => {
        // Generate results for all expression-based aggregate functions.
        expressionAggEvalProjection(currentBuffer)
        // Generate results for all imperative aggregate functions.
        var i = 0
        while (i < allImperativeAggregateFunctions.length) {
          aggregateResult.update(
            allImperativeAggregateFunctionPositions(i),
            allImperativeAggregateFunctions(i).eval(currentBuffer)
          )
          i += 1
        }
        //        val resultRow = new GenericInternalRow(currentGroupingKey.numFields + aggregateResult.numFields)
        joinedRow(currentGroupingKey, aggregateResult)
      }
    } else {
      // Grouping-only: we only output values based on grouping expressions.
      (currentGroupingKey: InternalRow, currentBuffer: InternalRow) =>
        {
          currentGroupingKey
        }
    }
  }

  private def initAggregationBuffer(buffer: InternalRow): Unit = {
    // Initializes declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    // Initializes imperative aggregates' buffer values
    aggregateFunctions
      .collect { case f: ImperativeAggregate => f }
      .foreach(_.initialize(buffer))
  }

  // Creates a new aggregation buffer and initializes buffer values. This function should only be
  // called under two cases:
  //
  //  - when creating aggregation buffer for a new group in the hash map, and
  //  - when creating the re-used buffer for sort-based aggregation
  private def createNewAggregationBuffer(): InternalRow = {
    val bufferFieldTypes =
      aggregateFunctions.flatMap(_.aggBufferAttributes.map(_.dataType))
    val buffer = new GenericInternalRow(bufferFieldTypes.length)
    initAggregationBuffer(buffer)
    buffer
  }

  private def getAggregationBufferByKey(
      hashMap: java.util.Map[InternalRow, InternalRow],
      groupingKey: InternalRow
  ): InternalRow = {
    var aggBuffer = hashMap.get(groupingKey)

    if (aggBuffer == null) {
      aggBuffer = createNewAggregationBuffer()
      hashMap.put(groupingKey.copy(), aggBuffer)
    }

    aggBuffer
  }

  override def localAttributeOrder(): Array[Attribute] =
    groupingAttributes ++ aggregateFunctions.flatMap(_.aggBufferAttributes)

  override def isSorted(): Boolean = childIter.isSorted()

  override def isBreakPoint(): Boolean = true

  override def results(): InternalBlock = {
    val inputRowIter = childIter.results().toArray().iterator
    val seccoIter = new SeccoIterator {
      override def hasNext: Boolean = inputRowIter.hasNext
      override def next(): InternalRow = inputRowIter.next()
      override def localAttributeOrder(): Array[Attribute] = ???
      override def isSorted(): Boolean = ???
      override def isBreakPoint(): Boolean = ???
      override def results(): InternalBlock = ???
      override def children: Seq[SeccoIterator] = ???
      override def productElement(n: Int): Any = ???
      override def productArity: Int = ???
      override def canEqual(that: Any): Boolean = ???
    }
    val tempIter = processInputs(seccoIter)
    val rowArrayBuffer = ArrayBuffer[InternalRow]()
    while (tempIter.hasNext) {
      val curEntry = tempIter.next()
      val curOutputRow = generateOutput(curEntry.getKey, curEntry.getValue)
      rowArrayBuffer.append(curOutputRow)
    }
    val aggrAttributes = aggregateFunctions.map(item =>
      AttributeReference(item.prettyName, item.dataType)()
    )
    InternalBlock(
      rowArrayBuffer.toArray,
      StructType.fromAttributes(groupingAttributes ++ aggrAttributes)
    )
  }

  override def children: Seq[SeccoIterator] = childIter :: Nil

  override def hasNext: Boolean = aggBufferIterator.hasNext

  override def next(): InternalRow = {
    val entry = aggBufferIterator.next()
    generateOutput(entry.getKey, entry.getValue)
  }

  // Initializing functions used to process a row.
  protected def generateProcessRow(
      functions: Seq[AggregateFunction],
      inputAttributes: Seq[Attribute]
  ): (InternalRow, InternalRow) => Unit = {
    val joinedRow = new JoinedRow
    if (functions.nonEmpty) {
      val updateExpressionsDec =
        functions.flatMap {
          case ae: DeclarativeAggregate => ae.updateExpressions
          case agg: AggregateFunction =>
            Seq.fill(agg.aggBufferAttributes.length)(NoOp)
        }
      val updateFunctionsImp = functions.collect {
        case ae: ImperativeAggregate =>
          (
              buffer: InternalRow,
              row: InternalRow
          ) => ae.update(buffer, row) // edited by lgh
//              (buffer: InternalRow, row: InternalRow) => ae.merge(buffer, row)
      }.toArray

      // This projection is used to merge buffer values for all expression-based aggregates.
      val aggregationBufferSchema = functions.flatMap(_.aggBufferAttributes)

      val updateProjectionDec =
        GenerateMutableProjection.generate(
          updateExpressionsDec,
          aggregationBufferSchema ++ inputAttributes
        )

      (currentBuffer: InternalRow, row: InternalRow) => {
        // Process all codeGen aggregate function (for DeclarativeAggregate).
        updateProjectionDec.target(currentBuffer)(joinedRow(currentBuffer, row))
        // Process all imperative aggregate functions (for ImperativeAggregate).
        var i = 0
        while (i < updateFunctionsImp.length) {
          updateFunctionsImp(i)(currentBuffer, row)
          i += 1
        }
      }
    } else {
      // Grouping only.
      (currentBuffer: InternalRow, row: InternalRow) => {}
    }
  }

  // This function is used to read and process input rows. When processing input rows, it first uses
  // hash-based aggregation by putting groups and their buffers in `hashMap`. If `hashMap` grows too
  // large, it sorts the contents, spills them to disk, and creates a new map. At last, all sorted
  // spills are merged together for sort-based aggregation.
  private def processInputs(
      iter: SeccoIterator
  ): java.util.Iterator[java.util.Map.Entry[InternalRow, InternalRow]] = {
    // In-memory map to store aggregation buffer for hash-based aggregation.
    val hashMap = new util.LinkedHashMap[InternalRow, InternalRow]
//
//    // If in-memory map is unable to stores all aggregation buffer, fallback to sort-based
//    // aggregation backed by sorted physical storage.
//    var sortBasedAggregationStore: SortBasedAggregator = null

    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      val groupingKey = groupingProjection.apply(null)
      val buffer: InternalRow = getAggregationBufferByKey(hashMap, groupingKey)
      while (iter.hasNext) {
        processRow(buffer, iter.next())
      }
    } else {
      while (iter.hasNext) {
        val newInput = iter.next()
        val groupingKey = groupingProjection.apply(newInput)
        val buffer: InternalRow =
          getAggregationBufferByKey(hashMap, groupingKey)
        processRow(buffer, newInput)
      }
    }

    hashMap.entrySet().iterator()

  }

}

/** The iterator that performs aggregation and support index-like operations.
  *
  * Note that keyAttributes should be the same as grouping expression after convert named expression to attribute.
  */
case class IndexableAggregateIterator(
    childIter: SeccoIterator,
    keyAttributes: Array[Attribute],
    groupingExpression: Array[NamedExpression],
    aggregateExpressions: Array[NamedExpression]
) extends SeccoIterator
    with IndexableSeccoIterator {

  override def localAttributeOrder(): Array[Attribute] = ???

  override def isSorted(): Boolean = ???

  override def isBreakPoint(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = ???

  override def hasNext: Boolean = ???

  override def next(): InternalRow = ???

  /** Set the key for this iterator
    *
    * @param key the key in [[InternalRow]]
    * @return return true if the key exists, otherwise return false
    */
  override def setKey(key: InternalRow): Boolean = ???

  /** Get one row for the given key
    *
    * @param key the key in [[InternalRow]]
    * @return one of the row under given key
    */
  override def getOneRow(key: InternalRow): Option[InternalRow] = ???

  /** Get one row for the given key
    *
    * @param key the key in [[InternalRow]]
    * @return if the key exists returns one row, otherwise return null
    */
  override def unsafeGetOneRow(key: InternalRow): InternalRow = ???
}
