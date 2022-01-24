package org.apache.spark.secco.util

import org.apache.spark.secco.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.secco.expression.RowOrdering
import org.apache.spark.secco.types._

object TypeUtils {

  def getInterpretedOrdering(t: DataType): Ordering[Any] = {
    t match {
      case i: AtomicType => i.ordering.asInstanceOf[Ordering[Any]]
//      case a: ArrayType => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
//      case s: StructType => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
//      case udt: UserDefinedType[_] => getInterpretedOrdering(udt.sqlType)
    }
  }

  def checkForOrderingExpr(dt: DataType, caller: String): TypeCheckResult = {
    if (RowOrdering.isOrderable(dt)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"$caller does not support ordering on type ${dt.catalogString}")
    }
  }

  def checkForSameTypeInputExpr(types: Seq[DataType], caller: String): TypeCheckResult = {
    if (TypeCoercion.haveSameType(types)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"input to $caller should all be the same type, but it's " +
          types.map(_.catalogString).mkString("[", ", ", "]"))
    }
  }
}
