package org.apache.spark.dolphin.playground

import org.apache.spark.dolphin.types.{
  AtomicType,
  BooleanType,
  DataTypes,
  FractionalType,
  IntegerType,
  IntegralType,
  NumericType
}
import org.scalatest.FunSuite

class DataTypePlayGround extends FunSuite {
  test("initialization") {
    val booleanType = DataTypes.BooleanType
    val intType = DataTypes.IntegerType
    val longType = DataTypes.LongType
    val floatType = DataTypes.FloatType
    val doubleType = DataTypes.DoubleType
    val stringType = DataTypes.StringType
    val structType = DataTypes.createStructType(
      Array(DataTypes.createStructField("a", DataTypes.IntegerType, false))
    )

    println(booleanType)
    println(intType)
    println(longType)
    println(floatType)
    println(doubleType)
    println(stringType)
    println(structType)

    assert(intType.isInstanceOf[IntegralType])
    assert(longType.isInstanceOf[IntegralType])
    assert(floatType.isInstanceOf[FractionalType])
    assert(doubleType.isInstanceOf[FractionalType])

    println(IntegralType.simpleString)
    println(NumericType.simpleString)

    println(IntegralType.defaultConcreteType)
    println(NumericType.defaultConcreteType)
  }

}
