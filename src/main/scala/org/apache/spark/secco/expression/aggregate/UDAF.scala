package org.apache.spark.secco.expression.aggregate

import org.apache.spark.internal.Logging
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{AttributeReference, Expression, ImplicitCastInputTypes, UserDefinedExpression}
import org.apache.spark.secco.types._
import org.apache.spark.secco.Row
import org.apache.spark.secco.expression.codegen.GenerateMutableProjection
import org.apache.spark.secco.util.CatalystTypeConverters


/**
  * A helper trait used to create specialized setter and getter for types supported by
  * [[org.apache.spark.sql.execution.UnsafeFixedWidthAggregationMap]]'s buffer.
  * (see UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema).
  */
sealed trait BufferSetterGetterUtils {

  def createGetters(schema: StructType): Array[(InternalRow, Int) => Any] = {
    val dataTypes = schema.fields.map(_.dataType)
    val getters = new Array[(InternalRow, Int) => Any](dataTypes.length)

    var i = 0
    while (i < getters.length) {
      getters(i) = dataTypes(i) match {
        case NullType =>
          (row: InternalRow, ordinal: Int) => null

        case BooleanType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getBoolean(ordinal)

        case IntegerType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getInt(ordinal)

        case LongType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getLong(ordinal)

        case FloatType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getFloat(ordinal)

        case DoubleType =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.getDouble(ordinal)

        case other =>
          (row: InternalRow, ordinal: Int) =>
            if (row.isNullAt(ordinal)) null else row.get(ordinal, other)
      }

      i += 1
    }

    getters
  }

  def createSetters(schema: StructType): Array[((InternalRow, Int, Any) => Unit)] = {
    val dataTypes = schema.fields.map(_.dataType)
    val setters = new Array[(InternalRow, Int, Any) => Unit](dataTypes.length)

    var i = 0
    while (i < setters.length) {
      setters(i) = dataTypes(i) match {
        case NullType =>
          (row: InternalRow, ordinal: Int, value: Any) => row.setNullAt(ordinal)

        case b: BooleanType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setBoolean(ordinal, value.asInstanceOf[Boolean])
            } else {
              row.setNullAt(ordinal)
            }

        case IntegerType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setInt(ordinal, value.asInstanceOf[Int])
            } else {
              row.setNullAt(ordinal)
            }

        case LongType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setLong(ordinal, value.asInstanceOf[Long])
            } else {
              row.setNullAt(ordinal)
            }

        case FloatType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setFloat(ordinal, value.asInstanceOf[Float])
            } else {
              row.setNullAt(ordinal)
            }

        case DoubleType =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.setDouble(ordinal, value.asInstanceOf[Double])
            } else {
              row.setNullAt(ordinal)
            }

        case other =>
          (row: InternalRow, ordinal: Int, value: Any) =>
            if (value != null) {
              row.update(ordinal, value)
            } else {
              row.setNullAt(ordinal)
            }
      }

      i += 1
    }

    setters
  }
}

/**
  * A Mutable [[Row]] representing a mutable aggregation buffer.
  */
private[aggregate] class MutableAggregationBufferImpl(
                                                       schema: StructType,
                                                       toCatalystConverters: Array[Any => Any],
                                                       toScalaConverters: Array[Any => Any],
                                                       bufferOffset: Int,
                                                       var underlyingInternalRow: InternalRow)
  extends MutableAggregationBuffer with BufferSetterGetterUtils {

  private[this] val offsets: Array[Int] = {
    val newOffsets = new Array[Int](length)
    var i = 0
    while (i < newOffsets.length) {
      newOffsets(i) = bufferOffset + i
      i += 1
    }
    newOffsets
  }

  private[this] val bufferValueGetters = createGetters(schema)

  private[this] val bufferValueSetters = createSetters(schema)

  override def length: Int = toCatalystConverters.length

  override def get(i: Int): Any = {
    if (i >= length || i < 0) {
      throw new IllegalArgumentException(
        s"Could not access ${i}th value in this buffer because it only has $length values.")
    }

    toScalaConverters(i)(bufferValueGetters(i)(underlyingInternalRow, offsets(i)))
  }

  def update(i: Int, value: Any): Unit = {
    if (i >= length || i < 0) {
      throw new IllegalArgumentException(
        s"Could not update ${i}th value in this buffer because it only has $length values.")
    }

    bufferValueSetters(i)(underlyingInternalRow, offsets(i), toCatalystConverters(i)(value))
  }

  // Because get method call specialized getter based on the schema, we cannot use the
  // default implementation of the isNullAt (which is get(i) == null).
  // We have to override it to call isNullAt of the underlyingBuffer.
  override def isNullAt(i: Int): Boolean = {
    underlyingInternalRow.isNullAt(offsets(i))
  }

  override def copy(): MutableAggregationBufferImpl = {
    new MutableAggregationBufferImpl(
      schema,
      toCatalystConverters,
      toScalaConverters,
      bufferOffset,
      underlyingInternalRow)
  }
}

/**
  * A [[Row]] representing an immutable aggregation buffer.
  */
private[aggregate] class InputAggregationBuffer(
                                                 schema: StructType,
                                                 toCatalystConverters: Array[Any => Any],
                                                 toScalaConverters: Array[Any => Any],
                                                 bufferOffset: Int,
                                                 var underlyingInternalRow: InternalRow)
  extends Row with BufferSetterGetterUtils {

  private[this] val offsets: Array[Int] = {
    val newOffsets = new Array[Int](length)
    var i = 0
    while (i < newOffsets.length) {
      newOffsets(i) = bufferOffset + i
      i += 1
    }
    newOffsets
  }

  private[this] val bufferValueGetters = createGetters(schema)

  def getBufferOffset: Int = bufferOffset

  override def length: Int = toCatalystConverters.length

  override def get(i: Int): Any = {
    if (i >= length || i < 0) {
      throw new IllegalArgumentException(
        s"Could not access ${i}th value in this buffer because it only has $length values.")
    }
    toScalaConverters(i)(bufferValueGetters(i)(underlyingInternalRow, offsets(i)))
  }

  // Because get method call specialized getter based on the schema, we cannot use the
  // default implementation of the isNullAt (which is get(i) == null).
  // We have to override it to call isNullAt of the underlyingInputBuffer.
  override def isNullAt(i: Int): Boolean = {
    underlyingInternalRow.isNullAt(offsets(i))
  }

  override def copy(): InputAggregationBuffer = {
    new InputAggregationBuffer(
      schema,
      toCatalystConverters,
      toScalaConverters,
      bufferOffset,
      underlyingInternalRow)
  }
}

// lgh: deprecated in Spark
/**
  * The base class for implementing user-defined aggregate functions (UDAF).
  *
  * @since 1.5.0
  * @deprecated UserDefinedAggregateFunction is deprecated.
  * Aggregator[IN, BUF, OUT] should now be registered as a UDF via the functions.udaf(agg) method.
  */
abstract class UserDefinedAggregateFunction extends Serializable {

  /**
    * A `StructType` represents data types of input arguments of this aggregate function.
    * For example, if a [[UserDefinedAggregateFunction]] expects two input arguments
    * with type of `DoubleType` and `LongType`, the returned `StructType` will look like
    *
    * ```
    *   new StructType()
    *    .add("doubleInput", DoubleType)
    *    .add("longInput", LongType)
    * ```
    *
    * The name of a field of this `StructType` is only used to identify the corresponding
    * input argument. Users can choose names to identify the input arguments.
    *
    * @since 1.5.0
    */
  def inputSchema: StructType

  /**
    * A `StructType` represents data types of values in the aggregation buffer.
    * For example, if a [[UserDefinedAggregateFunction]]'s buffer has two values
    * (i.e. two intermediate values) with type of `DoubleType` and `LongType`,
    * the returned `StructType` will look like
    *
    * ```
    *   new StructType()
    *    .add("doubleInput", DoubleType)
    *    .add("longInput", LongType)
    * ```
    *
    * The name of a field of this `StructType` is only used to identify the corresponding
    * buffer value. Users can choose names to identify the input arguments.
    *
    * @since 1.5.0
    */
  def bufferSchema: StructType

  /**
    * The `DataType` of the returned value of this [[UserDefinedAggregateFunction]].
    *
    * @since 1.5.0
    */
  def dataType: DataType

  /**
    * Returns true iff this function is deterministic, i.e. given the same input,
    * always return the same output.
    *
    * @since 1.5.0
    */
  def deterministic: Boolean

  /**
    * Initializes the given aggregation buffer, i.e. the zero value of the aggregation buffer.
    *
    * The contract should be that applying the merge function on two initial buffers should just
    * return the initial buffer itself, i.e.
    * `merge(initialBuffer, initialBuffer)` should equal `initialBuffer`.
    *
    * @since 1.5.0
    */
  def initialize(buffer: MutableAggregationBuffer): Unit

  /**
    * Updates the given aggregation buffer `buffer` with new input data from `input`.
    *
    * This is called once per input row.
    *
    * @since 1.5.0
    */
  def update(buffer: MutableAggregationBuffer, input: Row): Unit

  /**
    * Merges two aggregation buffers and stores the updated buffer values back to `buffer1`.
    *
    * This is called when we merge two partially aggregated data together.
    *
    * @since 1.5.0
    */
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit

  /**
    * Calculates the final result of this [[UserDefinedAggregateFunction]] based on the given
    * aggregation buffer.
    *
    * @since 1.5.0
    */
  def evaluate(buffer: Row): Any

}


case class UDAF(
    override val children: Seq[Expression],
    udaf: UserDefinedAggregateFunction,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    udafName: Option[String] = None)
  extends ImperativeAggregate
    with UserDefinedExpression
    with ImplicitCastInputTypes
    with Serializable
    with Logging {

  override def nullable: Boolean = true

  override def dataType: DataType = udaf.dataType

  /**
    * Returns true iff this function is deterministic, i.e. given the same input,
    * always return the same output.
    */
  override def deterministic: Boolean = udaf.deterministic

  override def inputTypes: Seq[DataType] = udaf.inputSchema.map(_.dataType)

  /** The schema of the aggregation buffer. */
  override def aggBufferSchema: StructType = udaf.bufferSchema

  /** Attributes of fields in aggBufferSchema. */
  override def aggBufferAttributes: Seq[AttributeReference] = aggBufferSchema.toAttributes

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  /**
    * Attributes of fields in input aggregation buffers (immutable aggregation buffers that are
    * merged with mutable aggregation buffers in the merge() function or merge expressions).
    * These attributes are created automatically by cloning the [[aggBufferAttributes]].
    */
  override def inputAggBufferAttributes: Seq[AttributeReference] = aggBufferAttributes.map(_.newInstance())

  private[this] lazy val childrenSchema: StructType = {
    val inputFields = children.zipWithIndex.map {
      case (child, index) =>
        StructField(s"input$index", child.dataType, child.nullable)
    }
    StructType(inputFields)
  }

  private lazy val inputProjection = {
    val inputAttributes = childrenSchema.toAttributes
    log.debug(
      s"Creating MutableProj: $children, inputSchema: $inputAttributes.")
    GenerateMutableProjection.generate(children, inputAttributes, useSubexprElimination = false)
//    MutableProjection.create(children, inputAttributes)
  }

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  private[this] lazy val inputToScalaConverters: Any => Any =
    CatalystTypeConverters.createToScalaConverter(childrenSchema)

  private[this] lazy val bufferValuesToCatalystConverters: Array[Any => Any] = {
    aggBufferSchema.fields.map { field =>
      CatalystTypeConverters.createToCatalystConverter(field.dataType)
    }
  }

  private[this] lazy val bufferValuesToScalaConverters: Array[Any => Any] = {
    aggBufferSchema.fields.map { field =>
      CatalystTypeConverters.createToScalaConverter(field.dataType)
    }
  }

  private[this] lazy val outputToCatalystConverter: Any => Any = {
    CatalystTypeConverters.createToCatalystConverter(dataType)
  }

  // This buffer is only used at executor side.
  private[this] lazy val inputAggregateBuffer: InputAggregationBuffer = {
    new InputAggregationBuffer(
      aggBufferSchema,
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      inputAggBufferOffset,
      null)
  }

  // This buffer is only used at executor side.
  private[this] lazy val mutableAggregateBuffer: MutableAggregationBufferImpl = {
    new MutableAggregationBufferImpl(
      aggBufferSchema,
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      mutableAggBufferOffset,
      null)
  }

  // This buffer is only used at executor side.
  private[this] lazy val evalAggregateBuffer: InputAggregationBuffer = {
    new InputAggregationBuffer(
      aggBufferSchema,
      bufferValuesToCatalystConverters,
      bufferValuesToScalaConverters,
      inputAggBufferOffset,
      null)
  }

  /**
    * Initializes the given aggregation buffer, i.e. the zero value of the aggregation buffer.
    *
    * The contract should be that applying the merge function on two initial buffers should just
    * return the initial buffer itself, i.e.
    * `merge(initialBuffer, initialBuffer)` should equal `initialBuffer`.
    */
  override def initialize(buffer: InternalRow): Unit = {
    mutableAggregateBuffer.underlyingInternalRow = buffer
    udaf.initialize(mutableAggregateBuffer)
  }

  /**
    * Updates the given aggregation buffer `buffer` with new input data from `input`.
    *
    * This is called once per input InternalRow.
    */
  override def update(buffer: InternalRow, input: InternalRow): Unit = {
    mutableAggregateBuffer.underlyingInternalRow = buffer

    udaf.update(
      mutableAggregateBuffer,
      inputToScalaConverters(inputProjection(input)).asInstanceOf[Row])
  }

  /**
    * Merges two aggregation buffers and stores the updated buffer values back to `buffer1`.
    *
    * This is called when we merge two partially aggregated data together.
    */
  override def merge(buffer1: InternalRow, buffer2: InternalRow): Unit = {
    mutableAggregateBuffer.underlyingInternalRow = buffer1
    inputAggregateBuffer.underlyingInternalRow = buffer2

    udaf.merge(mutableAggregateBuffer, inputAggregateBuffer)
  }

  /**
    * Calculates the final result of this [[UDAF]] based on the given
    * aggregation buffer.
    */
  override def eval(buffer: InternalRow): Any = {
    evalAggregateBuffer.underlyingInternalRow = buffer

    outputToCatalystConverter(udaf.evaluate(evalAggregateBuffer))
  }

  override def toString: String = {
    s"""$nodeName(${children.mkString(",")})"""
  }
  override def nodeName: String = name

  override def name: String = udafName.getOrElse(udaf.getClass.getSimpleName)

}


/**
  * A `Row` representing a mutable aggregation buffer.
  *
  * This is not meant to be extended outside of Spark.
  */
abstract class MutableAggregationBuffer extends Row {

  /** Update the ith value of this buffer. */
  def update(i: Int, value: Any): Unit
}