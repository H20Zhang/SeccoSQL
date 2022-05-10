package org.apache.spark.secco.types

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


import org.apache.spark.annotation.InterfaceStability.Unstable

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag


/**
	* The timestamp without time zone type represents a local time in microsecond precision,
	* which is independent of time zone.
	* Its valid range is [0001-01-01T00:00:00.000000, 9999-12-31T23:59:59.999999].
	* To represent an absolute point in time, use `TimestampType` instead.
	*
	* Please use the singleton `DataTypes.TimestampNTZType` to refer the type.
	* @since 3.3.0
	*/
@Unstable
class TimestampNTZType private() extends AtomicType {
	/**
		* Internally, a timestamp is stored as the number of microseconds from
		* the epoch of 1970-01-01T00:00:00.000000(Unix system time zero)
		*/
	type InternalType = Long

	@transient lazy val tag = typeTag[InternalType]

	val ordering = implicitly[Ordering[InternalType]]

	/**
		* The default size of a value of the TimestampNTZType is 8 bytes.
		*/
	override def defaultSize: Int = 8

	override def typeName: String = "timestamp_ntz"

	override def asNullable: TimestampNTZType = this
}

/**
	* The companion case object and its class is separated so the companion object also subclasses
	* the TimestampNTZType class. Otherwise, the companion object would be of type
	* "TimestampNTZType" in byte code. Defined with a private constructor so the companion
	* object is the only possible instantiation.
	*
	* @since 3.3.0
	*/
@Unstable
case object TimestampNTZType extends TimestampNTZType

