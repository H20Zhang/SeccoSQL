package org.apache.spark.dolphin.types

import org.apache.spark.util.Utils

import scala.math.Numeric.{DoubleAsIfIntegral, FloatAsIfIntegral}
import scala.reflect.runtime.universe.typeTag

/**
  * The data type representing `Boolean` values. Please use the singleton `DataTypes.BooleanType`.
  */
class BooleanType private () extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "BooleanType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  type InternalType = Boolean
  @transient lazy val tag = typeTag[InternalType]
  val ordering = implicitly[Ordering[InternalType]]

  /**
    * The default size of a value of the BooleanType is 1 byte.
    */
  override def defaultSize: Int = 1

  override def asNullable: BooleanType = this
}

case object BooleanType extends BooleanType

/**
  * The data type representing `Int` values. Please use the singleton `DataTypes.IntegerType`.
  */
class IntegerType private () extends IntegralType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "IntegerType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  type InternalType = Int
  @transient lazy val tag = typeTag[InternalType]
  val numeric = implicitly[Numeric[Int]]
  val integral = implicitly[Integral[Int]]
  val ordering = implicitly[Ordering[InternalType]]

  /**
    * The default size of a value of the IntegerType is 4 bytes.
    */
  override def defaultSize: Int = 4

  override def simpleString: String = "int"

  override def asNullable: IntegerType = this
}

case object IntegerType extends IntegerType

/**
  * The data type representing `Long` values. Please use the singleton `DataTypes.LongType`.
  *
  */
class LongType private () extends IntegralType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "LongType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  type InternalType = Long
  @transient lazy val tag = typeTag[InternalType]
  val numeric = implicitly[Numeric[Long]]
  val integral = implicitly[Integral[Long]]
  val ordering = implicitly[Ordering[InternalType]]

  /**
    * The default size of a value of the LongType is 8 bytes.
    */
  override def defaultSize: Int = 8

  override def simpleString: String = "bigint"

  override def asNullable: LongType = this
}

case object LongType extends LongType

/**
  * The data type representing `Float` values. Please use the singleton `DataTypes.FloatType`.
  *
  */
class FloatType private () extends FractionalType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "FloatType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  type InternalType = Float
  @transient lazy val tag = typeTag[InternalType]
  val numeric = implicitly[Numeric[Float]]
  val fractional = implicitly[Fractional[Float]]
  val ordering = new Ordering[Float] {
    override def compare(x: Float, y: Float): Int =
      Utils.nanSafeCompareFloats(x, y)
  }
  val asIntegral = FloatAsIfIntegral

  /**
    * The default size of a value of the FloatType is 4 bytes.
    */
  override def defaultSize: Int = 4

  override def asNullable: FloatType = this
}

case object FloatType extends FloatType

/**
  * The data type representing `Double` values. Please use the singleton `DataTypes.DoubleType`.
  */
class DoubleType private () extends FractionalType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "DoubleType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  type InternalType = Double
  @transient lazy val tag = typeTag[InternalType]
  val numeric = implicitly[Numeric[Double]]
  val fractional = implicitly[Fractional[Double]]
  val ordering = new Ordering[Double] {
    override def compare(x: Double, y: Double): Int =
      Utils.nanSafeCompareDoubles(x, y)
  }
  val asIntegral = DoubleAsIfIntegral

  /**
    * The default size of a value of the DoubleType is 8 bytes.
    */
  override def defaultSize: Int = 8

  override def asNullable: DoubleType = this
}

case object DoubleType extends DoubleType

class NullType private () extends DataType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "NullType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  override def defaultSize: Int = 1

  override def asNullable: NullType = this
}

case object NullType extends NullType
