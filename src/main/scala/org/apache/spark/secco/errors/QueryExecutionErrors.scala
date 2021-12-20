package org.apache.spark.secco.errors

import org.apache.spark.secco.types.DataType

object QueryExecutionErrors {
  def orderedOperationUnsupportedByDataTypeError(dataType: DataType): Throwable = {
    new IllegalArgumentException(s"Type $dataType does not support ordered operations")
  }
}
