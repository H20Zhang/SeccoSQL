package org.apache.spark.secco.types

import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.hadoop.classification.InterfaceStability.Stable
import org.apache.spark.secco.expression.Expression

import java.util.Locale
import org.apache.spark.secco.types.StringUtils.StringConcat

/** The base type of all Secco data types.
  */
abstract class DataType extends AbstractDataType {

  /** Enables matching against DataType for expressions:
    * {{{
    *   case Cast(child @ BinaryType(), StringType) =>
    *     ...
    * }}}
    */
  def unapply(e: Expression): Boolean = e.dataType == this

  /** The default size of a value of this data type, used internally for size estimation.
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

  private[secco] def jsonValue: JValue = typeName

  /** Readable string representation for the type. */
  def simpleString: String = typeName

  /** String representation for the type saved in external catalogs. */
  def catalogString: String = simpleString

  /** Readable string representation for the type with truncation */
  def simpleString(maxNumberFields: Int): String = simpleString

  def sql: String = simpleString.toUpperCase(Locale.ROOT)

  /** Check if `this` and `other` are the same data type when ignoring nullability
    * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
    */
  def sameType(other: DataType): Boolean = this == other
  //    if (SQLConf.get.caseSensitiveAnalysis) {
  //      DataType.equalsIgnoreNullability(this, other)
  //    } else {
  //      DataType.equalsIgnoreCaseAndNullability(this, other)
  //    }

  /** Returns the same data type but set all nullability fields are true
    * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
    */
  def asNullable: DataType

  /** Returns true if any `DataType` of this DataType tree satisfies the given function `f`.
    */
  def existsRecursively(f: (DataType) => Boolean): Boolean = f(this)

  override def defaultConcreteType: DataType = this

  override def acceptsType(other: DataType): Boolean = sameType(other)
}

@Stable
object DataType {


  /**
    * Returns true if the two data types share the same "shape", i.e. the types
    * are the same, but the field names don't need to be the same.
    *
    * @param ignoreNullability whether to ignore nullability when comparing the types
    */
  def equalsStructurally(
                          from: DataType,
                          to: DataType,
                          ignoreNullability: Boolean = false): Boolean = {
    (from, to) match {
      case (left: ArrayType, right: ArrayType) =>
        equalsStructurally(left.elementType, right.elementType, ignoreNullability) &&
          (ignoreNullability || left.containsNull == right.containsNull)

      case (left: MapType, right: MapType) =>
        equalsStructurally(left.keyType, right.keyType, ignoreNullability) &&
          equalsStructurally(left.valueType, right.valueType, ignoreNullability) &&
          (ignoreNullability || left.valueContainsNull == right.valueContainsNull)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
          fromFields.zip(toFields)
            .forall { case (l, r) =>
              equalsStructurally(l.dataType, r.dataType, ignoreNullability) &&
                (ignoreNullability || l.nullable == r.nullable)
            }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  protected[types] def buildFormattedString(
                                             dataType: DataType,
                                             prefix: String,
                                             stringConcat: StringConcat,
                                             maxDepth: Int): Unit = {
    dataType match {
      case array: ArrayType =>
        array.buildFormattedString(prefix, stringConcat, maxDepth - 1)
      case struct: ArrayType =>
        struct.buildFormattedString(prefix, stringConcat, maxDepth - 1)
      case map: MapType =>
        map.buildFormattedString(prefix, stringConcat, maxDepth - 1)
      case _ =>
    }
  }
}