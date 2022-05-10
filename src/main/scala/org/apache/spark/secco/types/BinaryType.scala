package org.apache.spark.secco.types

import org.apache.hadoop.classification.InterfaceStability.Stable

import scala.reflect.runtime.universe.typeTag
import org.apache.spark.unsafe.types.ByteArray
//import org.apache.spark.secco.types.ByteType

/**
	* The data type representing `Array[Byte]` values.
	* Please use the singleton `DataTypes.BinaryType`.
	*/
@Stable
abstract class BinaryType private() extends AtomicType {
	// The companion object and this class is separated so the companion object also subclasses
	// this type. Otherwise, the companion object would be of type "BinaryType$" in byte code.
	// Defined with a private constructor so the companion object is the only possible instantiation.

	type InternalType = Array[Byte]
//	val ordering: Ordering[InternalType]
	@transient lazy val tag = typeTag[InternalType]

//	private[secco] val ordering = (x: Array[Byte], y: Array[Byte]) => ByteArray.compareBinary(x, y)
//		(x: Array[Byte], y: Array[Byte]) => ByteArray.compareBinary(x, y)

	/**
		* The default size of a value of the BinaryType is 100 bytes.
		*/
	override def defaultSize: Int = 100

	override def asNullable: BinaryType = this
}

/**
	* @since 1.3.0
	*/
//@Stable
//case object BinaryType extends BinaryType

