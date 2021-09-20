package org.apache.spark.dolphin.types

import org.apache.spark.dolphin.expression.Expression

import scala.reflect.runtime.universe.TypeTag

/**
  * A non-concrete data type, reserved for internal uses.
  */
abstract class AbstractDataType {

  /**
    * The default concrete type to use if we want to cast a null literal into this type.
    */
  def defaultConcreteType: DataType

  /**
    * Returns true if `other` is an acceptable input type for a function that expects this,
    * possibly abstract DataType.
    *
    * {{{
    *   // this should return true
    *   DecimalType.acceptsType(DecimalType(10, 2))
    *
    *   // this should return true as well
    *   NumericType.acceptsType(DecimalType(10, 2))
    * }}}
    */
  def acceptsType(other: DataType): Boolean

  /** Readable string representation for the type. */
  def simpleString: String

}

/**
  * A collection of types that can be used to specify type constraints. The sequence also specifies
  * precedence: an earlier type takes precedence over a latter type.
  *
  * {{{
  *   TypeCollection(StringType, BinaryType)
  * }}}
  *
  * This means that we prefer StringType over BinaryType if it is possible to cast to StringType.
  */
class TypeCollection(private val types: Seq[AbstractDataType])
    extends AbstractDataType {

  require(types.nonEmpty, s"TypeCollection ($types) cannot be empty")

  override def defaultConcreteType: DataType = types.head.defaultConcreteType

  override def acceptsType(other: DataType): Boolean =
    types.exists(_.acceptsType(other))

  override def simpleString: String = {
    types.map(_.simpleString).mkString("(", " or ", ")")
  }
}

object TypeCollection {

  /**
    * Types that include numeric types and interval type. They are only used in unary_minus,
    * unary_positive, add and subtract operations.
    */
  val NumericAndInterval = TypeCollection(NumericType)

  def apply(types: AbstractDataType*): TypeCollection =
    new TypeCollection(types)

  def unapply(typ: AbstractDataType): Option[Seq[AbstractDataType]] =
    typ match {
      case typ: TypeCollection => Some(typ.types)
      case _                   => None
    }
}

/**
  * An `AbstractDataType` that matches any concrete data types.
  */
object AnyDataType extends AbstractDataType {

  // Note that since AnyDataType matches any concrete types, defaultConcreteType should never
  // be invoked.
  override def defaultConcreteType: DataType =
    throw new UnsupportedOperationException

  override def simpleString: String = "any"

  override def acceptsType(other: DataType): Boolean = true
}

/**
  * An internal type used to represent everything that is not null, UDTs, arrays, structs, and maps.
  */
abstract class AtomicType extends DataType {
  type InternalType
  val tag: TypeTag[InternalType]
  val ordering: Ordering[InternalType]
}

object AtomicType {

  /**
    * Enables matching against AtomicType for expressions:
    * {{{
    *   case Cast(child @ AtomicType(), StringType) =>
    *     ...
    * }}}
    */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[AtomicType]
}

/**
  * Numeric data types.
  *
  */
abstract class NumericType extends AtomicType {
  // Unfortunately we can't get this implicitly as that breaks Spark Serialization. In order for
  // implicitly[Numeric[JvmType]] to be valid, we have to change JvmType from a type variable to a
  // type parameter and add a numeric annotation (i.e., [JvmType : Numeric]). This gets
  // desugared by the compiler into an argument to the objects constructor. This means there is no
  // longer a no argument constructor and thus the JVM cannot serialize the object anymore.
  val numeric: Numeric[InternalType]
}

object NumericType extends AbstractDataType {

  /**
    * Enables matching against NumericType for expressions:
    * {{{
    *   case Cast(child @ NumericType(), StringType) =>
    *     ...
    * }}}
    */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[NumericType]

  override def defaultConcreteType: DataType = DoubleType

  override def simpleString: String = "numeric"

  override def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[NumericType]
}

object IntegralType extends AbstractDataType {

  /**
    * Enables matching against IntegralType for expressions:
    * {{{
    *   case Cast(child @ IntegralType(), StringType) =>
    *     ...
    * }}}
    */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[IntegralType]

  override def defaultConcreteType: DataType = IntegerType

  override def simpleString: String = "integral"

  override def acceptsType(other: DataType): Boolean =
    other.isInstanceOf[IntegralType]
}

abstract class IntegralType extends NumericType {
  val integral: Integral[InternalType]
}

object FractionalType {

  /**
    * Enables matching against FractionalType for expressions:
    * {{{
    *   case Cast(child @ FractionalType(), StringType) =>
    *     ...
    * }}}
    */
  def unapply(e: Expression): Boolean = e.dataType.isInstanceOf[FractionalType]
}

abstract class FractionalType extends NumericType {
  val fractional: Fractional[InternalType]
  val asIntegral: Integral[InternalType]
}
