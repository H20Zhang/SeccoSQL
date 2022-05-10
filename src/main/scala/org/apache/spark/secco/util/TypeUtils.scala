package org.apache.spark.secco.util

import org.apache.spark.secco.analysis.{AnalysisException, TypeCheckResult}
import org.apache.spark.secco.types.{CalendarIntervalType, ArrayType, AtomicType, BinaryType, DataType, MapType, NullType, NumericType, StructType}


/**
	* Functions to help with checking for valid data types and value comparison of various types.
	*/
object TypeUtils {
	def checkForNumericExpr(dt: DataType, caller: String): TypeCheckResult = {
		if (dt.isInstanceOf[NumericType] || dt == NullType) {
			TypeCheckResult.TypeCheckSuccess
		} else {
			TypeCheckResult.TypeCheckFailure(s"$caller requires numeric types, not ${dt.catalogString}")
		}
	}

	def checkForOrderingExpr(dt: DataType, caller: String): TypeCheckResult = {
		if (isOrderable(dt)) {
			TypeCheckResult.TypeCheckSuccess
		} else {
			TypeCheckResult.TypeCheckFailure(
				s"$caller does not support ordering on type ${dt.catalogString}")
		}
	}

	def isOrderable(dataType: DataType): Boolean = dataType match {
		case NullType => true
		case dt: AtomicType => true
		case struct: StructType => struct.fields.forall(f => isOrderable(f.dataType))
		case array: ArrayType => isOrderable(array.elementType)
//		case udt: UserDefinedType[_] => isOrderable(udt.sqlType)
		case _ => false
	}


	//	def checkForSameTypeInputExpr(types: Seq[DataType], caller: String): TypeCheckResult = {
//		if (TypeCoercion.haveSameType(types)) {
//			TypeCheckResult.TypeCheckSuccess
//		} else {
//			TypeCheckResult.TypeCheckFailure(
//				s"input to $caller should all be the same type, but it's " +
//					types.map(_.catalogString).mkString("[", ", ", "]"))
//		}
//	}

	def checkForMapKeyType(keyType: DataType): TypeCheckResult = {
		if (keyType.existsRecursively(_.isInstanceOf[MapType])) {
			TypeCheckResult.TypeCheckFailure("The key of map cannot be/contain map.")
		} else {
			TypeCheckResult.TypeCheckSuccess
		}
	}

//	def checkForAnsiIntervalOrNumericType(
//																				 dt: DataType, funcName: String): TypeCheckResult = dt match {
//		case _: AnsiIntervalType | NullType =>
//			TypeCheckResult.TypeCheckSuccess
//		case dt if dt.isInstanceOf[NumericType] => TypeCheckResult.TypeCheckSuccess
//		case other => TypeCheckResult.TypeCheckFailure(
//			s"function $funcName requires numeric or interval types, not ${other.catalogString}")
//	}

//	def getNumeric(t: DataType, exactNumericRequired: Boolean = false): Numeric[Any] = {
//		if (exactNumericRequired) {
//			t.asInstanceOf[NumericType].exactNumeric.asInstanceOf[Numeric[Any]]
//		} else {
//			t.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
//		}
//	}

//TODO
	def getInterpretedOrdering(t: DataType): Ordering[Any] = {
		t match {
			case i: AtomicType => i.ordering.asInstanceOf[Ordering[Any]]
//			case a: ArrayType => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
//			case s: StructType => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
//			case udt: UserDefinedType[_] => getInterpretedOrdering(udt.sqlType)
		}
	}

	/**
		* Returns true if the equals method of the elements of the data type is implemented properly.
		* This also means that they can be safely used in collections relying on the equals method,
		* as sets or maps.
		*/
	def typeWithProperEquals(dataType: DataType): Boolean = dataType match {
//		case BinaryType => false
		case _: AtomicType => true
		case _ => false
	}

//	def failWithIntervalType(dataType: DataType): Unit = {
//		invokeOnceForInterval(dataType, forbidAnsiIntervals = false) {
////			throw QueryCompilationErrors.cannotUseIntervalTypeInTableSchemaError()
//			throw new AnalysisException("Cannot use interval type in the table schema.")
//		}
//	}

//	def invokeOnceForInterval(dataType: DataType, forbidAnsiIntervals: Boolean)(f: => Unit): Unit = {
//		def isInterval(dataType: DataType): Boolean = dataType match {
//			case _: AnsiIntervalType => forbidAnsiIntervals
//			case CalendarIntervalType => true
//			case _ => false
//		}
//
//		if (dataType.existsRecursively(isInterval)) f
//	}
}
