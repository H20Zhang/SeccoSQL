package org.apache.spark.secco.errors

import org.apache.spark.secco.codegen.ExprValue
import org.apache.spark.secco.expression.Expression
import org.apache.spark.secco.types.DataType

object QueryExecutionErrors {

  def cannotEvaluateExpressionError(expression: Expression): Throwable = {
    new UnsupportedOperationException(s"INTERNAL_ERROR: Cannot generate code for expression: $expression")
  }

  def cannotGenerateCodeForExpressionError(expression: Expression): Throwable = {
    new UnsupportedOperationException(s"INTERNAL_ERROR: Cannot generate code for expression: $expression")
  }

  def castingCauseOverflowError(t: Any, targetType: String): ArithmeticException = {
    // edited by lgh
    new ArithmeticException(s"CAST_CAUSES_OVERFLOW! t: $t, targetType: $targetType")
    //    new SparkArithmeticException(errorClass = "CAST_CAUSES_OVERFLOW",
    //      messageParameters = Array(t.toString, targetType, SQLConf.ANSI_ENABLED.key))
  }
  def fieldIndexOnRowWithoutSchemaError(): Throwable = {
    new UnsupportedOperationException("fieldIndex on a Row without schema is undefined.")
  }

  def invalidInputSyntaxForBooleanError(s: String): Throwable = {  // edited by lgh: UTFString -> String
    new UnsupportedOperationException(s"invalid input syntax for type boolean: $s. " +
    s"To return NULL instead, use 'try_cast'. ")  // edited by lgh
    //      s"If necessary set ${SQLConf.ANSI_ENABLED.key} " +
    //      "to false to bypass this error.")
  }

  def invalidInputSyntaxForNumericError(e: NumberFormatException): NumberFormatException = {
    // edited by lgh
    new NumberFormatException(s"${e.getMessage}. To return NULL instead, use 'try_cast'. ")
//      s"If necessary set ${SQLConf.ANSI_ENABLED.key} to false to bypass this error.")
  }

  def noDefaultForDataTypeError(dataType: DataType): RuntimeException = {
    new RuntimeException(s"no default for type $dataType")
  }

  // This Error is defined by lgh
  def notSupportGetStructError(): Throwable = {
    new UnsupportedOperationException(s"INTERNAL_ERROR: InternalRow has no method named `getStruct` yet.")
  }

  def orderedOperationUnsupportedByDataTypeError(dataType: DataType): Throwable = {
    new IllegalArgumentException(s"Type $dataType does not support ordered operations")
  }

  def valueIsNullError(index: Int): Throwable = {
    new NullPointerException(s"Value at index $index is null")
  }
}
