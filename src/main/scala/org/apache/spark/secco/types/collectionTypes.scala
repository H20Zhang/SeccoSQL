package org.apache.spark.secco.types

import org.apache.spark.secco.expression.{Attribute, AttributeReference}
import org.apache.spark.secco.types.AtomicType
import org.apache.spark.util.Utils

import scala.reflect.runtime.universe.typeTag

/** The data type representing `String` values. Please use the singleton `DataTypes.StringType`.
  */
class StringType private () extends AtomicType {
  // The companion object and this class is separated so the companion object also subclasses
  // this type. Otherwise, the companion object would be of type "StringType$" in byte code.
  // Defined with a private constructor so the companion object is the only possible instantiation.
  type InternalType = String
  @transient lazy val tag = typeTag[InternalType]
  val ordering = implicitly[Ordering[InternalType]]

  /** The default size of a value of the StringType is 20 bytes.
    */
  override def defaultSize: Int = 20

  override def asNullable: StringType = this
}

case object StringType extends StringType

/** A field inside a StructType.
  * @param name The name of this field.
  * @param dataType The data type of this field.
  * @param nullable Indicates if values of this field can be `null` values.
  */
case class StructField(
    name: String,
    dataType: DataType,
    nullable: Boolean = true
) {

  /** No-arg constructor for kryo. */
  protected def this() = this(null, null)

  // override the default toString to be compatible with legacy parquet files.
  override def toString: String = s"StructField($name,$dataType,$nullable)"

}

/** A [[StructType]] object can be constructed by
  * {{{
  * StructType(fields: Seq[StructField])
  * }}}
  * For a [[StructType]] object, one or multiple [[StructField]]s can be extracted by names.
  * If multiple [[StructField]]s are extracted, a [[StructType]] object will be returned.
  * If a provided name does not have a matching field, it will be ignored. For the case
  * of extracting a single [[StructField]], a `null` will be returned.
  *
  * Scala Example:
  * {{{
  * import org.apache.spark.sql._
  * import org.apache.spark.sql.types._
  *
  * val struct =
  *   StructType(
  *     StructField("a", IntegerType, true) ::
  *     StructField("b", LongType, false) ::
  *     StructField("c", BooleanType, false) :: Nil)
  *
  * // Extract a single StructField.
  * val singleField = struct("b")
  * // singleField: StructField = StructField(b,LongType,false)
  *
  * // If this struct does not have a field called "d", it throws an exception.
  * struct("d")
  * // java.lang.IllegalArgumentException: Field "d" does not exist.
  * //   ...
  *
  * // Extract multiple StructFields. Field names are provided in a set.
  * // A StructType object will be returned.
  * val twoFields = struct(Set("b", "c"))
  * // twoFields: StructType =
  * //   StructType(StructField(b,LongType,false), StructField(c,BooleanType,false))
  *
  * // Any names without matching fields will throw an exception.
  * // For the case shown below, an exception is thrown due to "d".
  * struct(Set("b", "c", "d"))
  * // java.lang.IllegalArgumentException: Field "d" does not exist.
  * //    ...
  * }}}
  */
case class StructType(fields: Array[StructField])
    extends DataType
    with Seq[StructField] {

  /** No-arg constructor for kryo. */
  def this() = this(Array.empty[StructField])

  /** Returns all field names in an array. */
  def fieldNames: Array[String] = fields.map(_.name)

  /** Returns all field names in an array. This is an alias of `fieldNames`.
    *
    * @since 2.4.0
    */
  def names: Array[String] = fieldNames

  private lazy val fieldNamesSet: Set[String] = fieldNames.toSet
  private lazy val nameToField: Map[String, StructField] =
    fields.map(f => f.name -> f).toMap
  private lazy val nameToIndex: Map[String, Int] = fieldNames.zipWithIndex.toMap

  override def equals(that: Any): Boolean = {
    that match {
      case StructType(otherFields) =>
        java.util.Arrays.equals(
          fields.asInstanceOf[Array[AnyRef]],
          otherFields.asInstanceOf[Array[AnyRef]]
        )
      case _ => false
    }
  }

  private lazy val _hashCode: Int =
    java.util.Arrays.hashCode(fields.asInstanceOf[Array[AnyRef]])
  override def hashCode(): Int = _hashCode

  /** Creates a new [[StructType]] by adding a new field.
    * {{{
    * val struct = (new StructType)
    *   .add(StructField("a", IntegerType, true))
    *   .add(StructField("b", LongType, false))
    *   .add(StructField("c", StringType, true))
    * }}}
    */
  def add(field: StructField): StructType = {
    StructType(fields :+ field)
  }

  /** Creates a new [[StructType]] by adding a new nullable field with no metadata.
    *
    * val struct = (new StructType)
    *   .add("a", IntegerType)
    *   .add("b", LongType)
    *   .add("c", StringType)
    */
  def add(name: String, dataType: DataType): StructType = {
    StructType(fields :+ StructField(name, dataType, nullable = true))
  }

  /** Creates a new [[StructType]] by adding a new field with no metadata.
    *
    * val struct = (new StructType)
    *   .add("a", IntegerType, true)
    *   .add("b", LongType, false)
    *   .add("c", StringType, true)
    */
  def add(name: String, dataType: DataType, nullable: Boolean): StructType = {
    StructType(fields :+ StructField(name, dataType, nullable))
  }

  /** Extracts the [[StructField]] with the given name.
    *
    * @throws IllegalArgumentException if a field with the given name does not exist
    */
  def apply(name: String): StructField = {
    nameToField.getOrElse(
      name,
      throw new IllegalArgumentException(s"""Field "$name" does not exist.
                                            |Available fields: ${fieldNames
        .mkString(", ")}""".stripMargin)
    )
  }

  /** Returns a [[StructType]] containing [[StructField]]s of the given names, preserving the
    * original order of fields.
    *
    * @throws IllegalArgumentException if a field cannot be found for any of the given names
    */
  def apply(names: Set[String]): StructType = {
    val nonExistFields = names -- fieldNamesSet
    if (nonExistFields.nonEmpty) {
      throw new IllegalArgumentException(
        s"""Nonexistent field(s): ${nonExistFields.mkString(", ")}.
           |Available fields: ${fieldNames.mkString(", ")}""".stripMargin
      )
    }
    // Preserve the original order of fields.
    StructType(fields.filter(f => names.contains(f.name)))
  }

  /** Returns the index of a given field.
    *
    * @throws IllegalArgumentException if a field with the given name does not exist
    */
  def fieldIndex(name: String): Int = {
    nameToIndex.getOrElse(
      name,
      throw new IllegalArgumentException(s"""Field "$name" does not exist.
                                            |Available fields: ${fieldNames
        .mkString(", ")}""".stripMargin)
    )
  }

  def getFieldIndex(name: String): Option[Int] = {
    nameToIndex.get(name)
  }

  def toAttributes: Seq[AttributeReference] =
    map(f => AttributeReference(f.name, f.dataType, f.nullable)())

  override def apply(fieldIndex: Int): StructField = fields(fieldIndex)

  override def length: Int = fields.length

  override def iterator: Iterator[StructField] = fields.iterator

  /** The default size of a value of the StructType is the total default sizes of all field types.
    */
  override def defaultSize: Int = fields.map(_.dataType.defaultSize).sum

  override def simpleString: String = {
    val fieldTypes =
      fields.map(field => s"${field.name}:${field.dataType.simpleString}")
    Utils.truncatedString(fieldTypes, "struct<", ",", ">")
  }

  override def catalogString: String = {
    // in catalogString, we should not truncate
    val fieldTypes =
      fields.map(field => s"${field.name}:${field.dataType.catalogString}")
    s"struct<${fieldTypes.mkString(",")}>"
  }

  /** Returns the same data type but set all nullability fields are true
    * (`StructField.nullable`, `ArrayType.containsNull`, and `MapType.valueContainsNull`).
    */
  override def asNullable: StructType = {
    val newFields = fields.map { case StructField(name, dataType, nullable) =>
      StructField(name, dataType.asNullable, nullable = true)
    }

    StructType(newFields)
  }
}

object StructType extends AbstractDataType {

  override def defaultConcreteType: DataType = new StructType

  override def acceptsType(other: DataType): Boolean = {
    other.isInstanceOf[StructType]
  }

  override def simpleString: String = "struct"

//  def fromString(raw: String): StructType = {
//    Try(DataType.fromJson(raw)).getOrElse(LegacyTypeStringParser.parse(raw)) match {
//      case t: StructType => t
//      case _ => throw new RuntimeException(s"Failed parsing ${StructType.simpleString}: $raw")
//    }
//  }

//  /**
//   * Creates StructType for a given DDL-formatted string, which is a comma separated list of field
//   * definitions, e.g., a INT, b STRING.
//   *
//   * @since 2.2.0
//   */
//  def fromDDL(ddl: String): StructType = CatalystSqlParser.parseTableSchema(ddl)

  def apply(fields: Seq[StructField]): StructType = StructType(fields.toArray)

  def apply(fields: java.util.List[StructField]): StructType = {
    import scala.collection.JavaConverters._
    StructType(fields.asScala)
  }

  def fromAttributes(attributes: Seq[Attribute]): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable)))

//  private[sql] def removeMetadata(key: String, dt: DataType): DataType =
//    dt match {
//      case StructType(fields) =>
//        val newFields = fields.map { f =>
//          val mb = new MetadataBuilder()
//          f.copy(dataType = removeMetadata(key, f.dataType),
//            metadata = mb.withMetadata(f.metadata).remove(key).build())
//        }
//        StructType(newFields)
//      case _ => dt
//    }

  def fieldsMap(fields: Array[StructField]): Map[String, StructField] = {
    import scala.collection.breakOut
    fields.map(s => (s.name, s))(breakOut)
  }
}
