package org.apache.spark.dolphin.types

import java.util.Locale

import org.apache.spark.dolphin.expression.Expression

/**
  * The base type of all Secco data types.
  *
  */
abstract class DataType extends AbstractDataType {

  /**
    * Enables matching against DataType for expressions:
    * {{{
    *   case Cast(child @ BinaryType(), StringType) =>
    *     ...
    * }}}
    */
  def unapply(e: Expression): Boolean = e.dataType == this

  /**
    * The default size of a value of this data type, used internally for size estimation.
    */
  def defaultSize: Int

  /** Name of the type used in JSON serialization. */
  def typeName: String = {
    this.getClass.getSimpleName
      .stripSuffix("$")
      .stripSuffix("Type")
      .stripSuffix("UDT")
      .toLowerCase(Locale.ROOT)
  }

  /** Readable string representation for the type. */
  def simpleString: String = typeName

  /** String representation for the type saved in external catalogs. */
  def catalogString: String = simpleString

  /** Readable string representation for the type with truncation */
  def simpleString(maxNumberFields: Int): String = simpleString

  def sql: String = simpleString.toUpperCase(Locale.ROOT)

  /**
    * Check if `this` and `other` are the same data type when ignoring nullability
    * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
    */
  def sameType(other: DataType): Boolean = this == other
  //    if (SQLConf.get.caseSensitiveAnalysis) {
  //      DataType.equalsIgnoreNullability(this, other)
  //    } else {
  //      DataType.equalsIgnoreCaseAndNullability(this, other)
  //    }

  /**
    * Returns the same data type but set all nullability fields are true
    * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
    */
  def asNullable: DataType

  /**
    * Returns true if any `DataType` of this DataType tree satisfies the given function `f`.
    */
  def existsRecursively(f: (DataType) => Boolean): Boolean = f(this)

  override def defaultConcreteType: DataType = this

  override def acceptsType(other: DataType): Boolean = sameType(other)
}
