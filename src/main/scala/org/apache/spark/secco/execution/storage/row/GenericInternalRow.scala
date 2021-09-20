package org.apache.spark.secco.execution.storage.row

import org.apache.spark.secco.types
import org.apache.spark.secco.types._

/**
  * An extended version of [[InternalRow]] that implements all special getters, toString
  * and equals/hashCode by `genericGet`.
  */
trait BaseGenericInternalRow extends InternalRow {

  protected def genericGet(ordinal: Int): Any

  // default implementation (slow)
  private def getAs[T](ordinal: Int): T = genericGet(ordinal).asInstanceOf[T]

  override def isNullAt(ordinal: Int): Boolean = getAs[AnyRef](ordinal) eq null

  override def getBoolean(ordinal: Int): Boolean = getAs[Boolean](ordinal)

  override def getInt(ordinal: Int): Int = getAs[Int](ordinal)

  override def getLong(ordinal: Int): Long = getAs[Long](ordinal)

  override def getFloat(ordinal: Int): Float = getAs[Float](ordinal)

  override def getDouble(ordinal: Int): Double = getAs[Double](ordinal)

  override def getString(ordinal: Int): String = getAs[String](ordinal)

  override def toString: String = {
    if (numFields == 0) {
      "[empty row]"
    } else {
      val sb = new StringBuilder
      sb.append("[")
      sb.append(genericGet(0))
      val len = numFields
      var i = 1
      while (i < len) {
        sb.append(",")
        sb.append(genericGet(i))
        i += 1
      }
      sb.append("]")
      sb.toString()
    }
  }

  override def copy(): InternalRow = {
    val len = numFields
    val newValues = new Array[Any](len)
    var i = 0
    while (i < len) {
      newValues(i) = genericGet(i)
      i += 1
    }
    new GenericInternalRow(newValues)
  }

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[BaseGenericInternalRow]) {
      return false
    }

    val other = o.asInstanceOf[BaseGenericInternalRow]
    if (other eq null) {
      return false // trf: this line seems useless, I will consider deleting it
    }

    val len = numFields
    if (len != other.numFields) {
      return false
    }

    var i = 0
    while (i < len) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = genericGet(i)
        val o2 = other.genericGet(i)
        o1 match {
          case b1: Array[Byte] =>
            if (
              !o2.isInstanceOf[Array[Byte]] ||
              !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])
            ) {
              return false
            }
          case f1: Float if java.lang.Float.isNaN(f1) =>
            if (
              !o2.isInstanceOf[Float] || !java.lang.Float.isNaN(
                o2.asInstanceOf[Float]
              )
            ) {
              return false
            }
          case d1: Double if java.lang.Double.isNaN(d1) =>
            if (
              !o2.isInstanceOf[Double] || !java.lang.Double.isNaN(
                o2.asInstanceOf[Double]
              )
            ) {
              return false
            }
          case _ =>
            if (o1 != o2) {
              return false
            }
        }
      }
      i += 1
    }
    true
  }

  // Custom hashCode function that matches the efficient code generated version.
  override def hashCode: Int = {
    var result: Int = 37
    var i = 0
    val len = numFields
    while (i < len) {
      val update: Int =
        if (isNullAt(i)) {
          0
        } else {
          genericGet(i) match {
            case b: Boolean => if (b) 0 else 1
            case b: Byte    => b.toInt
            case s: Short   => s.toInt
            case i: Int     => i
            case l: Long    => (l ^ (l >>> 32)).toInt
            case f: Float   => java.lang.Float.floatToIntBits(f)
            case d: Double =>
              val b = java.lang.Double.doubleToLongBits(d)
              (b ^ (b >>> 32)).toInt
            case a: Array[Byte] => java.util.Arrays.hashCode(a)
            case other          => other.hashCode()
          }
        }
      result = 37 * result + update
      i += 1
    }
    result
  }
}

/**
  * An internal row implementation that uses an array of objects as the underlying storage.
  * Note that, while the array is not copied, and thus could technically be mutated after creation,
  * this is not allowed.
  */
class GenericInternalRow(val values: Array[Any])
    extends BaseGenericInternalRow {

  /** No-arg constructor for serialization. */
  // below method has not been called by any public method, trf comment it temporarily
  // protected def this() = this(null)

  def this(size: Int) = this(new Array[Any](size))

  override protected def genericGet(ordinal: Int): Any = values(ordinal)

  override def numFields: Int = values.length

  override def setNullAt(i: Int): Unit = {
    values(i) = null
  }

  override def update(i: Int, value: Any): Unit = {
    values(i) = value
  }

  /**
    * Make a copy of the current [[InternalRow]] object.
    */

  override def get(ordinal: Int, dataType: types.DataType): Object =
    dataType match {
      case BooleanType =>
        getBoolean(ordinal).asInstanceOf[Object]
      case IntegerType =>
        getInt(ordinal).asInstanceOf[Object]
      case LongType =>
        getLong(ordinal).asInstanceOf[Object]
      case FloatType =>
        getFloat(ordinal).asInstanceOf[Object]
      case DoubleType =>
        getDouble(ordinal).asInstanceOf[Object]
      case StringType =>
        getString(ordinal).asInstanceOf[Object]
      case _ =>
        genericGet(ordinal).asInstanceOf[Object]
    }
}
