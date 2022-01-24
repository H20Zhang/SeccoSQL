/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.secco.expression

import org.apache.spark.secco.analysis.TypeCheckResult
import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen._
import org.apache.spark.secco.codegen.Block._
import org.apache.spark.secco.execution.storage.row.{GenericInternalRow, InternalRow}
import org.apache.spark.secco.expression.Cast.{forceNullable, resolvableNullability}
import org.apache.spark.secco.expression.utils.StringUtils
import org.apache.spark.secco.trees.TreeNodeTag
import org.apache.spark.secco.types._

import java.util.Locale

object Cast {

  /**
    * A tag to decide if a CAST is specified by user.
    */
  val USER_SPECIFIED_CAST = new TreeNodeTag[Boolean]("user_specified_cast")

  /**
    * We process literals such as 'Infinity', 'Inf', '-Infinity' and 'NaN' etc in case
    * insensitive manner to be compatible with other database systems such as PostgreSQL and DB2.
    */
  def processFloatingPointSpecialLiterals(v: String, isFloat: Boolean): Any = {
    v.trim.toLowerCase(Locale.ROOT) match {
      case "inf" | "+inf" | "infinity" | "+infinity" =>
        if (isFloat) Float.PositiveInfinity else Double.PositiveInfinity
      case "-inf" | "-infinity" =>
        if (isFloat) Float.NegativeInfinity else Double.NegativeInfinity
      case "nan" =>
        if (isFloat) Float.NaN else Double.NaN
      case _ => null
    }
  }

  /**
    * Returns `true` if casting non-nullable values from `from` type to `to` type
    * may return null. Note that the caller side should take care of input nullability
    * first and only call this method if the input is not nullable.
    */
  def forceNullable(from: DataType, to: DataType): Boolean = (from, to) match {
    case (NullType, _) => false // empty array or map case
    case (_, _) if from == to => false

//    case (StringType, BinaryType) => false
    case (StringType, _) => true
    case (_, StringType) => false

//    case (FloatType | DoubleType, TimestampType) => true
//    case (TimestampType, DateType) => false
//    case (_, DateType) => true
//    case (DateType, TimestampType) => false
//    case (DateType, _) => true
//    case (_, CalendarIntervalType) => true

//    case (_, to: DecimalType) if !canNullSafeCastToDecimal(from, to) => true
    case (_: FractionalType, _: IntegralType) => true  // NaN, infinity
    case _ => false
  }

  def resolvableNullability(from: Boolean, to: Boolean): Boolean = !from || to

}


abstract class CastBase extends UnaryExpression {

  override def child: Expression

  def dataType: DataType

//  /**
//    * Returns true iff we can cast `from` type to `to` type.
//    */
//  def canCast(from: DataType, to: DataType): Boolean

//  /**
//    * Returns the error message if casting from one type to another one is invalid.
//    */
//  def typeCheckFailureMessage: String

  override def toString: String = {
    val ansi = if (ansiEnabled) "ansi_" else ""
    s"${ansi}cast($child as ${dataType.simpleString})"
  }

//  override def checkInputDataTypes(): TypeCheckResult = {
//    if (canCast(child.dataType, dataType)) {
//      TypeCheckResult.TypeCheckSuccess
//    } else {
//      TypeCheckResult.TypeCheckFailure(typeCheckFailureMessage)
//    }
//  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)
    val nullSafeCast = nullSafeCastFunction(child.dataType, dataType, ctx)

    ev.copy(code = eval.code +
      castCode(ctx, eval.value, eval.isNull, ev.value, ev.isNull, dataType, nullSafeCast))
  }

  // The function arguments are: `input`, `result` and `resultIsNull`. We don't need `inputIsNull`
  // in parameter list, because the returned code will be put in null safe evaluation region.
  protected[this] type CastFunction = (ExprValue, ExprValue, ExprValue) => Block

  // Since we need to cast input expressions recursively inside ComplexTypes, such as Map's
  // Key and Value, Struct's field, we need to name out all the variable names involved in a cast.
  protected[this] def castCode(ctx: CodegenContext, input: ExprValue, inputIsNull: ExprValue,
                               result: ExprValue, resultIsNull: ExprValue, resultType: DataType, cast: CastFunction): Block = {
    val javaType = JavaCode.javaType(resultType)
    code"""
      boolean $resultIsNull = $inputIsNull;
      $javaType $result = ${CodeGenerator.defaultValue(resultType)};
      if (!$inputIsNull) {
        ${cast(input, result, resultIsNull)}
      }
    """
  }

  private[this] def nullSafeCastFunction(
    from: DataType,
    to: DataType,
    ctx: CodegenContext): CastFunction = to match {

    case _ if from == NullType => (c, evPrim, evNull) => code"$evNull = true;"
    case _ if to == from => (c, evPrim, evNull) => code"$evPrim = $c;"
    case StringType => castToStringCode(from, ctx)
//    case BinaryType => castToBinaryCode(from)
//    case DateType => castToDateCode(from, ctx)
//    case decimal: DecimalType => castToDecimalCode(from, decimal, ctx)
//    case TimestampType => castToTimestampCode(from, ctx)
//    case TimestampNTZType => castToTimestampNTZCode(from, ctx)
//    case CalendarIntervalType => castToIntervalCode(from)
//    case it: DayTimeIntervalType => castToDayTimeIntervalCode(from, it)
//    case it: YearMonthIntervalType => castToYearMonthIntervalCode(from, it)
    case BooleanType => castToBooleanCode(from)
//    case ByteType => castToByteCode(from, ctx)
//    case ShortType => castToShortCode(from, ctx)
    case IntegerType => castToIntCode(from, ctx)
    case FloatType => castToFloatCode(from, ctx)
    case LongType => castToLongCode(from, ctx)
    case DoubleType => castToDoubleCode(from, ctx)

//    case array: ArrayType =>
//      castArrayCode(from.asInstanceOf[ArrayType].elementType, array.elementType, ctx)
//    case map: MapType => castMapCode(from.asInstanceOf[MapType], map, ctx)
    case struct: StructType => castStructCode(from.asInstanceOf[StructType], struct, ctx)
//    case udt: UserDefinedType[_] if udt.acceptsType(from) =>
//      (c, evPrim, evNull) => code"$evPrim = $c;"
//    case _: UserDefinedType[_] =>
//      throw QueryExecutionErrors.cannotCastError(from, to)
  }


  private[this] def castToStringCode(from: DataType, ctx: CodegenContext): CastFunction = {
    from match {
//      case BinaryType =>
//        (c, evPrim, evNull) => code"$evPrim = UTF8String.fromBytes($c);"
//      case DateType =>
//        val df = JavaCode.global(
//          ctx.addReferenceObj("dateFormatter", dateFormatter),
//          dateFormatter.getClass)
//        (c, evPrim, evNull) => code"""$evPrim = UTF8String.fromString(${df}.format($c));"""
//      case TimestampType =>
//        val tf = JavaCode.global(
//          ctx.addReferenceObj("timestampFormatter", timestampFormatter),
//          timestampFormatter.getClass)
//        (c, evPrim, evNull) => code"""$evPrim = UTF8String.fromString($tf.format($c));"""
//      case TimestampNTZType =>
//        val tf = JavaCode.global(
//          ctx.addReferenceObj("timestampNTZFormatter", timestampNTZFormatter),
//          timestampNTZFormatter.getClass)
//        (c, evPrim, evNull) => code"""$evPrim = UTF8String.fromString($tf.format($c));"""
//      case CalendarIntervalType =>
//        (c, evPrim, _) => code"""$evPrim = UTF8String.fromString($c.toString());"""
//      case ArrayType(et, _) =>
//        (c, evPrim, evNull) => {
//          val buffer = ctx.freshVariable("buffer", classOf[UTF8StringBuilder])
//          val bufferClass = JavaCode.javaType(classOf[UTF8StringBuilder])
//          val writeArrayElemCode = writeArrayToStringBuilder(et, c, buffer, ctx)
//          code"""
//                |$bufferClass $buffer = new $bufferClass();
//                |$writeArrayElemCode;
//                |$evPrim = $buffer.build();
//           """.stripMargin
//        }
//      case MapType(kt, vt, _) =>
//        (c, evPrim, evNull) => {
//          val buffer = ctx.freshVariable("buffer", classOf[UTF8StringBuilder])
//          val bufferClass = JavaCode.javaType(classOf[UTF8StringBuilder])
//          val writeMapElemCode = writeMapToStringBuilder(kt, vt, c, buffer, ctx)
//          code"""
//                |$bufferClass $buffer = new $bufferClass();
//                |$writeMapElemCode;
//                |$evPrim = $buffer.build();
//           """.stripMargin
//        }
      case StructType(fields) =>
        (c, evPrim, evNull) => {
          val row = ctx.freshVariable("row", classOf[InternalRow])
          val buffer = ctx.freshVariable("buffer", classOf[StringBuilder]) // changed from UTF8StringBuilder to StringBuilder by lgh
          val bufferClass = JavaCode.javaType(classOf[StringBuilder])
          val writeStructCode = writeStructToStringBuilder(fields.map(_.dataType), row, buffer, ctx)
          code"""
                |InternalRow $row = $c;
                |$bufferClass $buffer = new $bufferClass();
                |$writeStructCode
                |$evPrim = $buffer.build();
           """.stripMargin
        }
//      case pudt: PythonUserDefinedType => castToStringCode(pudt.sqlType, ctx)
//      case udt: UserDefinedType[_] =>
//        val udtRef = JavaCode.global(ctx.addReferenceObj("udt", udt), udt.sqlType)
//        (c, evPrim, evNull) => {
//          code"$evPrim = UTF8String.fromString($udtRef.deserialize($c).toString());"
//        }
//      case i: YearMonthIntervalType =>
//        val iu = IntervalUtils.getClass.getName.stripSuffix("$")
//        val iss = IntervalStringStyles.getClass.getName.stripSuffix("$")
//        val style = s"$iss$$.MODULE$$.ANSI_STYLE()"
//        (c, evPrim, _) =>
//          code"""
//            $evPrim = UTF8String.fromString($iu.toYearMonthIntervalString($c, $style,
//              (byte)${i.startField}, (byte)${i.endField}));
//          """
//      case i: DayTimeIntervalType =>
//        val iu = IntervalUtils.getClass.getName.stripSuffix("$")
//        val iss = IntervalStringStyles.getClass.getName.stripSuffix("$")
//        val style = s"$iss$$.MODULE$$.ANSI_STYLE()"
//        (c, evPrim, _) =>
//          code"""
//            $evPrim = UTF8String.fromString($iu.toDayTimeIntervalString($c, $style,
//              (byte)${i.startField}, (byte)${i.endField}));
//          """
      case _ =>
        (c, evPrim, evNull) => code"$evPrim = String.valueOf($c);"  // edited by lgh
//        (c, evPrim, evNull) => code"$evPrim = UTF8String.fromString(String.valueOf($c));"
      // edited by lgh
    }
  }
  private def writeStructToStringBuilder(
    st: Seq[DataType],
    row: ExprValue,
    buffer: ExprValue,
    ctx: CodegenContext): Block = {
    val structToStringCode = st.zipWithIndex.map { case (ft, i) =>
      val fieldToStringCode = castToStringCode(ft, ctx)
      val field = ctx.freshVariable("field", ft)
      val fieldStr = ctx.freshVariable("fieldStr", StringType)
      val javaType = JavaCode.javaType(ft)
      code"""
            |${if (i != 0) code"""$buffer.append(",");""" else EmptyBlock}
            |if ($row.isNullAt($i)) {
            |  ${appendIfNotLegacyCastToStr(buffer, if (i == 0) "null" else " null")}
            |} else {
            |  ${if (i != 0) code"""$buffer.append(" ");""" else EmptyBlock}
            |
            |  // Append $i field into the string buffer
            |  $javaType $field = ${CodeGenerator.getValue(row, ft, s"$i")};
            |  String $fieldStr = null;
            |  ${fieldToStringCode(field, fieldStr, null /* resultIsNull won't be used */)}
            |  $buffer.append($fieldStr);
            |}
       """.stripMargin
      // edited by lgh
    }

    val writeStructCode = ctx.splitExpressions(
      expressions = structToStringCode.map(_.code),
      funcName = "fieldToString",
      arguments = ("InternalRow", row.code) ::
        (classOf[StringBuilder].getName, buffer.code) :: Nil)

    code"""
          |$buffer.append("$leftBracket");
          |$writeStructCode
          |$buffer.append("$rightBracket");
     """.stripMargin
  }

  // edited by lgh
  private def appendIfNotLegacyCastToStr(buffer: ExprValue, s: String): Block = {
    code"""$buffer.append("$s");"""
    //      if (!legacyCastToStr) code"""$buffer.append("$s");""" else EmptyBlock
  }

  // edited by lgh
  // The brackets that are used in casting structs and maps to strings
  private val (leftBracket, rightBracket) =  ("{", "}")
  //  if (legacyCastToStr) ("[", "]") else ("{", "}")

  protected def ansiEnabled: Boolean

  private[this] def castToBooleanCode(from: DataType): CastFunction = from match {
    case StringType =>
      val stringUtils = inline"${StringUtils.getClass.getName.stripSuffix("$")}"
      (c, evPrim, evNull) =>
        val castFailureCode = if (ansiEnabled) {
          s"throw QueryExecutionErrors.invalidInputSyntaxForBooleanError($c);"
        } else {
          s"$evNull = true;"
        }
        code"""
          if ($stringUtils.isTrueString($c)) {
            $evPrim = true;
          } else if ($stringUtils.isFalseString($c)) {
            $evPrim = false;
          } else {
            $castFailureCode
          }
        """
//    case TimestampType =>
//      (c, evPrim, evNull) => code"$evPrim = $c != 0;"
//    case DateType =>
//      // Hive would return null when cast from date to boolean
//      (c, evPrim, evNull) => code"$evNull = true;"
//    case DecimalType() =>
//      (c, evPrim, evNull) => code"$evPrim = !$c.isZero();"
    case n: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = $c != 0;"
  }


  private[this] def castToIntCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    //Added by lgh
    case StringType =>
      (c, evPrim, evNull) => code"$evPrim = Integer.parseInt($c)"
//    case StringType if ansiEnabled =>
//      val stringUtils = UTF8StringUtils.getClass.getCanonicalName.stripSuffix("$")
//      (c, evPrim, evNull) => code"$evPrim = $stringUtils.toIntExact($c);"
//    case StringType =>
//      val wrapper = ctx.freshVariable("intWrapper", classOf[UTF8String.IntWrapper])
//      (c, evPrim, evNull) =>
//        code"""
//          UTF8String.IntWrapper $wrapper = new UTF8String.IntWrapper();
//          if ($c.toInt($wrapper)) {
//            $evPrim = $wrapper.value;
//          } else {
//            $evNull = true;
//          }
//          $wrapper = null;
//        """
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? 1 : 0;"
//    case DateType =>
//      (c, evPrim, evNull) => code"$evNull = true;"
//    case TimestampType => castTimestampToIntegralTypeCode(ctx, "int", IntegerType.catalogString)
//    case DecimalType() => castDecimalToIntegralTypeCode(ctx, "int", IntegerType.catalogString)
    case LongType if ansiEnabled =>
      castIntegralTypeToIntegralTypeExactCode("int", IntegerType.catalogString)
    case FloatType | DoubleType if ansiEnabled =>
      castFractionToIntegralTypeCode("int", IntegerType.catalogString)
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (int) $c;"
//    case x: DayTimeIntervalType =>
//      castDayTimeIntervalToIntegralTypeCode(x.startField, x.endField, "Int")
//    case x: YearMonthIntervalType =>
//      castYearMonthIntervalToIntegralTypeCode(x.startField, x.endField, "Int")
  }

  private[this] def castIntegralTypeToIntegralTypeExactCode(
    integralType: String,
    catalogType: String): CastFunction = {
    assert(ansiEnabled)
    (c, evPrim, evNull) =>
      code"""
        if ($c == ($integralType) $c) {
          $evPrim = ($integralType) $c;
        } else {
          throw QueryExecutionErrors.castingCauseOverflowError($c, "$catalogType");
        }
      """
  }

  private[this] def castFractionToIntegralTypeCode(
                                                    integralType: String,
                                                    catalogType: String): CastFunction = {
    assert(ansiEnabled)
    val (min, max) = lowerAndUpperBound(integralType)
    val mathClass = classOf[Math].getName
    // When casting floating values to integral types, Spark uses the method `Numeric.toInt`
    // Or `Numeric.toLong` directly. For positive floating values, it is equivalent to `Math.floor`;
    // for negative floating values, it is equivalent to `Math.ceil`.
    // So, we can use the condition `Math.floor(x) <= upperBound && Math.ceil(x) >= lowerBound`
    // to check if the floating value x is in the range of an integral type after rounding.
    (c, evPrim, evNull) =>
      code"""
        if ($mathClass.floor($c) <= $max && $mathClass.ceil($c) >= $min) {
          $evPrim = ($integralType) $c;
        } else {
          throw QueryExecutionErrors.castingCauseOverflowError($c, "$catalogType");
        }
      """
  }

  private[this] def lowerAndUpperBound(integralType: String): (String, String) = {
    val (min, max, typeIndicator) = integralType.toLowerCase(Locale.ROOT) match {
      case "long" => (Long.MinValue, Long.MaxValue, "L")
      case "int" => (Int.MinValue, Int.MaxValue, "")
      case "short" => (Short.MinValue, Short.MaxValue, "")
      case "byte" => (Byte.MinValue, Byte.MaxValue, "")
    }
    (min.toString + typeIndicator, max.toString + typeIndicator)
  }

  private[this] def castToLongCode(from: DataType, ctx: CodegenContext): CastFunction = from match {
    case StringType =>
      (c, evPrim, evNull) => code"$evPrim = Long.parseLong($c);"
//    case StringType if ansiEnabled =>
//        val stringUtils = UTF8StringUtils.getClass.getCanonicalName.stripSuffix("$")
//        (c, evPrim, evNull) => code"$evPrim = $stringUtils.toLongExact($c);"
//    case StringType =>
//      val wrapper = ctx.freshVariable("longWrapper", classOf[UTF8String.LongWrapper])
//      (c, evPrim, evNull) =>
//        code"""
//          UTF8String.LongWrapper $wrapper = new UTF8String.LongWrapper();
//          if ($c.toLong($wrapper)) {
//            $evPrim = $wrapper.value;
//          } else {
//            $evNull = true;
//          }
//          $wrapper = null;
//        """
    case BooleanType =>
      (c, evPrim, evNull) => code"$evPrim = $c ? 1L : 0L;"
//    case DateType =>
//      (c, evPrim, evNull) => code"$evNull = true;"
//    case TimestampType =>
//      (c, evPrim, evNull) => code"$evPrim = (long) ${timestampToLongCode(c)};"
//    case DecimalType() => castDecimalToIntegralTypeCode(ctx, "long", LongType.catalogString)
    case FloatType | DoubleType if ansiEnabled =>
      castFractionToIntegralTypeCode("long", LongType.catalogString)
    case x: NumericType =>
      (c, evPrim, evNull) => code"$evPrim = (long) $c;"
//    case x: DayTimeIntervalType =>
//      castDayTimeIntervalToIntegralTypeCode(x.startField, x.endField, "Long")
//    case x: YearMonthIntervalType =>
//      castYearMonthIntervalToIntegralTypeCode(x.startField, x.endField, "Int")
  }

  private[this] def castToFloatCode(from: DataType, ctx: CodegenContext): CastFunction = {
    from match {
      case StringType =>
        val floatStr = ctx.freshVariable("floatStr", StringType)
        (c, evPrim, evNull) =>
          val handleNull = if (ansiEnabled) {
            s"throw QueryExecutionErrors.invalidInputSyntaxForNumericError($c);"
          } else {
            s"$evNull = true;"
          }
          code"""
          final String $floatStr = $c.toString();
          try {
            $evPrim = Float.valueOf($floatStr);
          } catch (java.lang.NumberFormatException e) {
            final Float f = (Float) Cast.processFloatingPointSpecialLiterals($floatStr, true);
            if (f == null) {
              $handleNull
            } else {
              $evPrim = f.floatValue();
            }
          }
        """
      case BooleanType =>
        (c, evPrim, evNull) => code"$evPrim = $c ? 1.0f : 0.0f;"
//      case DateType =>
//        (c, evPrim, evNull) => code"$evNull = true;"
//      case TimestampType =>
//        (c, evPrim, evNull) => code"$evPrim = (float) (${timestampToDoubleCode(c)});"
//      case DecimalType() =>
//        (c, evPrim, evNull) => code"$evPrim = $c.toFloat();"
      case x: NumericType =>
        (c, evPrim, evNull) => code"$evPrim = (float) $c;"
    }
  }



  private[this] def castToDoubleCode(from: DataType, ctx: CodegenContext): CastFunction = {
    from match {
      case StringType =>
        val doubleStr = ctx.freshVariable("doubleStr", StringType)
        (c, evPrim, evNull) =>
          val handleNull = if (ansiEnabled) {
            s"throw QueryExecutionErrors.invalidInputSyntaxForNumericError($c);"
          } else {
            s"$evNull = true;"
          }
          code"""
          final String $doubleStr = $c.toString();
          try {
            $evPrim = Double.valueOf($doubleStr);
          } catch (java.lang.NumberFormatException e) {
            final Double d = (Double) Cast.processFloatingPointSpecialLiterals($doubleStr, false);
            if (d == null) {
              $handleNull
            } else {
              $evPrim = d.doubleValue();
            }
          }
        """
      case BooleanType =>
        (c, evPrim, evNull) => code"$evPrim = $c ? 1.0d : 0.0d;"
//      case DateType =>
//        (c, evPrim, evNull) => code"$evNull = true;"
//      case TimestampType =>
//        (c, evPrim, evNull) => code"$evPrim = ${timestampToDoubleCode(c)};"
//      case DecimalType() =>
//        (c, evPrim, evNull) => code"$evPrim = $c.toDouble();"
      case x: NumericType =>
        (c, evPrim, evNull) => code"$evPrim = (double) $c;"
    }
  }


  private[this] def castStructCode(
                                    from: StructType, to: StructType, ctx: CodegenContext): CastFunction = {

    val fieldsCasts = from.fields.zip(to.fields).map {
      case (fromField, toField) => nullSafeCastFunction(fromField.dataType, toField.dataType, ctx)
    }
    val tmpResult = ctx.freshVariable("tmpResult", classOf[GenericInternalRow])
    val rowClass = JavaCode.javaType(classOf[GenericInternalRow])
    val tmpInput = ctx.freshVariable("tmpInput", classOf[InternalRow])

    val fieldsEvalCode = fieldsCasts.zipWithIndex.map { case (cast, i) =>
      val fromFieldPrim = ctx.freshVariable("ffp", from.fields(i).dataType)
      val fromFieldNull = ctx.freshVariable("ffn", BooleanType)
      val toFieldPrim = ctx.freshVariable("tfp", to.fields(i).dataType)
      val toFieldNull = ctx.freshVariable("tfn", BooleanType)
      val fromType = JavaCode.javaType(from.fields(i).dataType)
      val setColumn = CodeGenerator.setColumn(tmpResult, to.fields(i).dataType, i, toFieldPrim)
      code"""
        boolean $fromFieldNull = $tmpInput.isNullAt($i);
        if ($fromFieldNull) {
          $tmpResult.setNullAt($i);
        } else {
          $fromType $fromFieldPrim =
            ${CodeGenerator.getValue(tmpInput, from.fields(i).dataType, i.toString)};
          ${castCode(ctx, fromFieldPrim,
        fromFieldNull, toFieldPrim, toFieldNull, to.fields(i).dataType, cast)}
          if ($toFieldNull) {
            $tmpResult.setNullAt($i);
          } else {
            $setColumn;
          }
        }
       """
    }
    val fieldsEvalCodes = ctx.splitExpressions(
      expressions = fieldsEvalCode.map(_.code),
      funcName = "castStruct",
      arguments = ("InternalRow", tmpInput.code) :: (rowClass.code, tmpResult.code) :: Nil)

    (input, result, resultIsNull) =>
      code"""
        final $rowClass $tmpResult = new $rowClass(${fieldsCasts.length});
        final InternalRow $tmpInput = $input;
        $fieldsEvalCodes
        $result = $tmpResult;
      """
  }
}

/**
  * Cast the child expression to the target data type.
  *
  * When cast from/to timezone related types, we need timeZoneId, which will be resolved with
  * session local timezone by an analyzer [[ResolveTimeZone]].
  */
case class Cast(
   child: Expression,
   dataType: DataType,
   override val ansiEnabled: Boolean = true)  // edited by lgh
//   timeZoneId: Option[String] = None)
//   override val ansiEnabled: Boolean = SQLConf.get.ansiEnabled)
  extends CastBase {

//  override def withTimeZone(timeZoneId: String): TimeZoneAwareExpression =
//    copy(timeZoneId = Option(timeZoneId))

//  final override def nodePatternsInternal(): Seq[TreePattern] = Seq(CAST)

//  override def canCast(from: DataType, to: DataType): Boolean = if (ansiEnabled) {
//    AnsiCast.canCast(from, to)
//  } else {
//    Cast.canCast(from, to)
//  }

//  override def typeCheckFailureMessage: String = if (ansiEnabled) {
//    AnsiCast.typeCheckFailureMessage(child.dataType, dataType,
//      Some(SQLConf.ANSI_ENABLED.key), Some("false"))
//  } else {
//    s"cannot cast ${child.dataType.catalogString} to ${dataType.catalogString}"
//  }

//  override protected def withNewChildInternal(newChild: Expression): Cast = copy(child = newChild)
}


object AnsiCast {
  /**
    * As per section 6.13 "cast specification" in "Information technology — Database languages " +
    * "- SQL — Part 2: Foundation (SQL/Foundation)":
    * If the <cast operand> is a <value expression>, then the valid combinations of TD and SD
    * in a <cast specification> are given by the following table. “Y” indicates that the
    * combination is syntactically valid without restriction; “M” indicates that the combination
    * is valid subject to other Syntax Rules in this Sub- clause being satisfied; and “N” indicates
    * that the combination is not valid:
    * SD                   TD
    * EN AN C D T TS YM DT BO UDT B RT CT RW
    * EN  Y  Y  Y N N  N  M  M  N   M N  M  N N
    * AN  Y  Y  Y N N  N  N  N  N   M N  M  N N
    * C   Y  Y  Y Y Y  Y  Y  Y  Y   M N  M  N N
    * D   N  N  Y Y N  Y  N  N  N   M N  M  N N
    * T   N  N  Y N Y  Y  N  N  N   M N  M  N N
    * TS  N  N  Y Y Y  Y  N  N  N   M N  M  N N
    * YM  M  N  Y N N  N  Y  N  N   M N  M  N N
    * DT  M  N  Y N N  N  N  Y  N   M N  M  N N
    * BO  N  N  Y N N  N  N  N  Y   M N  M  N N
    * UDT M  M  M M M  M  M  M  M   M M  M  M N
    * B   N  N  N N N  N  N  N  N   M Y  M  N N
    * RT  M  M  M M M  M  M  M  M   M M  M  N N
    * CT  N  N  N N N  N  N  N  N   M N  N  M N
    * RW  N  N  N N N  N  N  N  N   N N  N  N M
    *
    * Where:
    * EN  = Exact Numeric
    * AN  = Approximate Numeric
    * C   = Character (Fixed- or Variable-Length, or Character Large Object)
    * D   = Date
    * T   = Time
    * TS  = Timestamp
    * YM  = Year-Month Interval
    * DT  = Day-Time Interval
    * BO  = Boolean
    * UDT  = User-Defined Type
    * B   = Binary (Fixed- or Variable-Length or Binary Large Object)
    * RT  = Reference type
    * CT  = Collection type
    * RW  = Row type
    *
    * Spark's ANSI mode follows the syntax rules, except it specially allow the following
    * straightforward type conversions which are disallowed as per the SQL standard:
    *   - Numeric <=> Boolean
    *   - String <=> Binary
    */
  def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (fromType, toType) if fromType == toType => true

    case (NullType, _) => true

    case (_, StringType) => true

//    case (StringType, _: BinaryType) => true

    case (StringType, BooleanType) => true
    case (_: NumericType, BooleanType) => true

//    case (StringType, TimestampType) => true
//    case (DateType, TimestampType) => true
//    case (TimestampNTZType, TimestampType) => true
//    case (_: NumericType, TimestampType) => SQLConf.get.allowCastBetweenDatetimeAndNumericInAnsi
//
//    case (StringType, TimestampNTZType) => true
//    case (DateType, TimestampNTZType) => true
//    case (TimestampType, TimestampNTZType) => true
//
//    case (StringType, _: CalendarIntervalType) => true
//    case (StringType, _: DayTimeIntervalType) => true
//    case (StringType, _: YearMonthIntervalType) => true
//
//    case (_: DayTimeIntervalType, _: DayTimeIntervalType) => true
//    case (_: YearMonthIntervalType, _: YearMonthIntervalType) => true
//
//    case (StringType, DateType) => true
//    case (TimestampType, DateType) => true
//    case (TimestampNTZType, DateType) => true

    case (_: NumericType, _: NumericType) => true
    case (StringType, _: NumericType) => true
    case (BooleanType, _: NumericType) => true
//    case (TimestampType, _: NumericType) => SQLConf.get.allowCastBetweenDatetimeAndNumericInAnsi
//    case (DateType, _: NumericType) => SQLConf.get.allowCastBetweenDatetimeAndNumericInAnsi

//    case (ArrayType(fromType, fn), ArrayType(toType, tn)) =>
//      canCast(fromType, toType) &&
//        resolvableNullability(fn || forceNullable(fromType, toType), tn)

//    case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
//      canCast(fromKey, toKey) &&
//        (!forceNullable(fromKey, toKey)) &&
//        canCast(fromValue, toValue) &&
//        resolvableNullability(fn || forceNullable(fromValue, toValue), tn)

    case (StructType(fromFields), StructType(toFields)) =>
      fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall {
          case (fromField, toField) =>
            canCast(fromField.dataType, toField.dataType) &&
              resolvableNullability(
                fromField.nullable || forceNullable(fromField.dataType, toField.dataType),
                toField.nullable)
        }

//    case (udt1: UserDefinedType[_], udt2: UserDefinedType[_]) if udt2.acceptsType(udt1) => true

    case _ => false
  }
}