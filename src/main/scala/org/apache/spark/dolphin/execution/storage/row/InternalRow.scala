package org.apache.spark.dolphin.execution.storage.row

import org.apache.spark.dolphin.types._

/** This class defines interface for accessing value of InternalRow */
abstract class InternalRow extends SpecializedGetters with Serializable {

  def numFields: Int

  def setNullAt(i: Int): Unit

  /**
    * Updates the value at column `i`. Note that after updating, the given value will be kept in this
    * row, and the caller side should guarantee that this value won't be changed afterwards.
    */
  def update(i: Int, value: Any): Unit

  // default implementation (slow)
  def setBoolean(i: Int, value: Boolean): Unit = update(i, value)
  def setByte(i: Int, value: Byte): Unit = update(i, value)
  def setShort(i: Int, value: Short): Unit = update(i, value)
  def setInt(i: Int, value: Int): Unit = update(i, value)
  def setLong(i: Int, value: Long): Unit = update(i, value)
  def setFloat(i: Int, value: Float): Unit = update(i, value)
  def setDouble(i: Int, value: Double): Unit = update(i, value)
  def setString(i: Int, value: String): Unit = update(i, value)

  /**
    * Make a copy of the current [[InternalRow]] object.
    */
  def copy(): InternalRow

  /** Returns true if there are any NULL values in this row. */
  def anyNull: Boolean = {
    val len = numFields
    var i = 0
    while (i < len) {
      if (isNullAt(i)) { return true }
      i += 1
    }
    false
  }

  /* ---------------------- utility methods for Scala ---------------------- */

  /**
    * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
    */
  def toSeq(fieldTypes: Seq[DataType]): Seq[Any] = {
    val len = numFields
    assert(len == fieldTypes.length)

    val values = new Array[Any](len)
    var i = 0
    while (i < len) {
      values(i) = get(i, fieldTypes(i))
      i += 1
    }
    values
  }

  def toSeq(schema: StructType): Seq[Any] = toSeq(schema.map(_.dataType))
}

object InternalRow {

  /**
    * This method can be used to construct a [[InternalRow]] with the given values.
    */
  def apply(values: Any*): InternalRow = new GenericInternalRow(values.toArray)

  /**
    * This method can be used to construct a [[InternalRow]] from a [[Seq]] of values.
    */
  def fromSeq(values: Seq[Any]): InternalRow =
    new GenericInternalRow(values.toArray)

  /** Returns an empty [[InternalRow]]. */
  val empty = apply()

  /**
    * Returns an accessor for an `InternalRow` with given data type. The returned accessor
    * actually takes a `SpecializedGetters` input because it can be generalized to other classes
    * that implements `SpecializedGetters` (e.g., `ArrayData`) too.
    */
  def getAccessor(dataType: DataType): (SpecializedGetters, Int) => Any =
    dataType match {
      case BooleanType =>
        (input, ordinal) => input.getBoolean(ordinal)
      case IntegerType =>
        (input, ordinal) => input.getInt(ordinal)
      case LongType =>
        (input, ordinal) => input.getLong(ordinal)
      case FloatType =>
        (input, ordinal) => input.getFloat(ordinal)
      case DoubleType =>
        (input, ordinal) => input.getDouble(ordinal)
      case StringType =>
        (input, ordinal) => input.getString(ordinal)
      case _ =>
        (input, ordinal) => input.get(ordinal, dataType)
    }

}
