package org.apache.spark.secco.types

import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.unsafe.types.UTF8String

/** Functions to convert Scala types to Secco types and vice versa.
  */
object TypeConverters {
  // The Predef.Map is scala.collection.immutable.Map.
  // Since the map values can be mutable, we explicitly import scala.collection.Map at here.
  import scala.collection.Map

  private def isPrimitive(dataType: DataType): Boolean = {
    dataType match {
      case BooleanType => true
      case IntegerType => true
      case LongType    => true
      case FloatType   => true
      case DoubleType  => true
      case _           => false
    }
  }

  private def getConverterForType(
      dataType: DataType
  ): TypeConverter[Any, Any, Any] = {
    val converter = dataType match {
//      case udt: UserDefinedType[_] => UDTConverter(udt)
//      case arrayType: ArrayType => ArrayConverter(arrayType.elementType)
//      case mapType: MapType => MapConverter(mapType.keyType, mapType.valueType)
//      case structType: StructType => StructConverter(structType)
      case StringType => StringConverter
//      case DateType => DateConverter
//      case TimestampType => TimestampConverter
//      case dt: DecimalType => new DecimalConverter(dt)
      case BooleanType => BooleanConverter
//      case ByteType => ByteConverter
//      case ShortType => ShortConverter
      case IntegerType        => IntConverter
      case LongType           => LongConverter
      case FloatType          => FloatConverter
      case DoubleType         => DoubleConverter
      case dataType: DataType => IdentityConverter(dataType)
    }
    converter.asInstanceOf[TypeConverter[Any, Any, Any]]
  }

  /** Converts a Scala type to its Secco equivalent (and vice versa).
    *
    * @tparam ScalaInputType The type of Scala values that can be converted to Catalyst.
    * @tparam ScalaOutputType The type of Scala values returned when converting Catalyst to Scala.
    * @tparam SeccoType The internal Secco type used to represent values of this Scala type.
    */
  private abstract class TypeConverter[
      ScalaInputType,
      ScalaOutputType,
      SeccoType
  ] extends Serializable {

    /** Converts a Scala type to its Catalyst equivalent while automatically handling nulls
      * and Options.
      */
    final def toSecco(maybeScalaValue: Any): SeccoType = {
      if (maybeScalaValue == null) {
        null.asInstanceOf[SeccoType]
      } else if (maybeScalaValue.isInstanceOf[Option[ScalaInputType]]) {
        val opt = maybeScalaValue.asInstanceOf[Option[ScalaInputType]]
        if (opt.isDefined) {
          toSeccoImpl(opt.get)
        } else {
          null.asInstanceOf[SeccoType]
        }
      } else {
        toSeccoImpl(maybeScalaValue.asInstanceOf[ScalaInputType])
      }
    }

    /** Given a Catalyst row, convert the value at column `column` to its Scala equivalent.
      */
    final def toScala(row: InternalRow, column: Int): ScalaOutputType = {
      if (row.isNullAt(column)) null.asInstanceOf[ScalaOutputType]
      else toScalaImpl(row, column)
    }

    /** Convert a Secco value to its Scala equivalent.
      */
    def toScala(seccoValue: SeccoType): ScalaOutputType

    /** Converts a Scala value to its Secco equivalent.
      * @param scalaValue the Scala value, guaranteed not to be null.
      * @return the Catalyst value.
      */
    protected def toSeccoImpl(scalaValue: ScalaInputType): SeccoType

    /** Given a Catalyst row, convert the value at column `column` to its Scala equivalent.
      * This method will only be called on non-null columns.
      */
    protected def toScalaImpl(row: InternalRow, column: Int): ScalaOutputType
  }

  private case class IdentityConverter(dataType: DataType)
      extends TypeConverter[Any, Any, Any] {
    override def toSeccoImpl(scalaValue: Any): Any = scalaValue
    override def toScala(seccoValue: Any): Any = seccoValue
    override def toScalaImpl(row: InternalRow, column: Int): Any =
      row.get(column, dataType)
  }

  private object StringConverter extends TypeConverter[Any, String, String] {
    override def toSeccoImpl(scalaValue: Any): String = scalaValue match {
      case str: String      => str
      case utf8: UTF8String => utf8.toString
      case chr: Char        => chr.toString
      case other =>
        throw new IllegalArgumentException(
          s"The value (${other.toString}) of the type (${other.getClass.getCanonicalName}) "
            + s"cannot be converted to the string type"
        )
    }
    override def toScala(seccoValue: String): String =
      if (seccoValue == null) null else seccoValue.toString
    override def toScalaImpl(row: InternalRow, column: Int): String =
      row.getString(column)
  }

  private abstract class PrimitiveConverter[T]
      extends TypeConverter[T, Any, Any] {
    final override def toScala(seccoValue: Any): Any = seccoValue
    final override def toSeccoImpl(scalaValue: T): Any = scalaValue
  }

  private object BooleanConverter extends PrimitiveConverter[Boolean] {
    override def toScalaImpl(row: InternalRow, column: Int): Boolean =
      row.getBoolean(column)
  }

//  private object ByteConverter extends PrimitiveConverter[Byte] {
//    override def toScalaImpl(row: InternalRow, column: Int): Byte = row.getByte(column)
//  }
//
//  private object ShortConverter extends PrimitiveConverter[Short] {
//    override def toScalaImpl(row: InternalRow, column: Int): Short = row.getShort(column)
//  }

  private object IntConverter extends PrimitiveConverter[Int] {
    override def toScalaImpl(row: InternalRow, column: Int): Int =
      row.getInt(column)
  }

  private object LongConverter extends PrimitiveConverter[Long] {
    override def toScalaImpl(row: InternalRow, column: Int): Long =
      row.getLong(column)
  }

  private object FloatConverter extends PrimitiveConverter[Float] {
    override def toScalaImpl(row: InternalRow, column: Int): Float =
      row.getFloat(column)
  }

  private object DoubleConverter extends PrimitiveConverter[Double] {
    override def toScalaImpl(row: InternalRow, column: Int): Double =
      row.getDouble(column)
  }

  /** Creates a converter function that will convert Scala objects to the specified Catalyst type.
    * Typical use case would be converting a collection of rows that have the same schema. You will
    * call this function once to get a converter, and apply it to every row.
    */
  def createToCatalystConverter(dataType: DataType): Any => Any = {
    if (isPrimitive(dataType)) {
      // Although the `else` branch here is capable of handling inbound conversion of primitives,
      // we add some special-case handling for those types here. The motivation for this relates to
      // Java method invocation costs: if we have rows that consist entirely of primitive columns,
      // then returning the same conversion function for all of the columns means that the call site
      // will be monomorphic instead of polymorphic. In microbenchmarks, this actually resulted in
      // a measurable performance impact. Note that this optimization will be unnecessary if we
      // use code generation to construct Scala Row -> Catalyst Row converters.
      def convert(maybeScalaValue: Any): Any = {
        if (maybeScalaValue.isInstanceOf[Option[Any]]) {
          maybeScalaValue.asInstanceOf[Option[Any]].orNull
        } else {
          maybeScalaValue
        }
      }
      convert
    } else {
      getConverterForType(dataType).toSecco
    }
  }

  /** Creates a converter function that will convert Catalyst types to Scala type.
    * Typical use case would be converting a collection of rows that have the same schema. You will
    * call this function once to get a converter, and apply it to every row.
    */
  def createToScalaConverter(dataType: DataType): Any => Any = {
    if (isPrimitive(dataType)) {
      identity
    } else {
      getConverterForType(dataType).toScala
    }
  }

  /**  Converts Scala objects to Catalyst rows / types.
    *
    *  Note: This should be called before do evaluation on Row
    *        (It does not support UDT)
    *  This is used to create an RDD or test results with correct types for Catalyst.
    */
  def convertToSecco(a: Any): Any = a match {
    case s: String => StringConverter.toSecco(s)
//    case d: Date      => DateConverter.toCatalyst(d)
//    case t: Timestamp => TimestampConverter.toCatalyst(t)
//    case d: BigDecimal =>
//      new DecimalConverter(DecimalType(d.precision, d.scale)).toCatalyst(d)
//    case d: JavaBigDecimal =>
//      new DecimalConverter(DecimalType(d.precision, d.scale)).toCatalyst(d)
//    case seq: Seq[Any] =>
//      new GenericArrayData(seq.map(convertToCatalyst).toArray)
//    case r: Row          => InternalRow(r.toSeq.map(convertToCatalyst): _*)
//    case arr: Array[Any] => new GenericArrayData(arr.map(convertToCatalyst))
//    case map: Map[_, _] =>
//      ArrayBasedMapData(
//        map,
//        (key: Any) => convertToCatalyst(key),
//        (value: Any) => convertToCatalyst(value)
//      )
//    case (keys: Array[_], values: Array[_]) =>
//       case for mapdata with duplicate keys
//      new ArrayBasedMapData(
//        new GenericArrayData(keys.map(convertToCatalyst)),
//        new GenericArrayData(values.map(convertToCatalyst))
//      )
    case other => other
  }

  /** Converts Catalyst types used internally in rows to standard Scala types
    * This method is slow, and for batch conversion you should be using converter
    * produced by createToScalaConverter.
    */
  def convertToScala(catalystValue: Any, dataType: DataType): Any = {
    createToScalaConverter(dataType)(catalystValue)
  }
}
