package org.apache.spark.secco.types

object TypeInfer {

  def inferType(x: Any): DataType = {
    x match {
      case Boolean => DataTypes.BooleanType
      case Int     => DataTypes.IntegerType
      case Long    => DataTypes.LongType
      case Float   => DataTypes.FloatType
      case Double  => DataTypes.DoubleType
      case String  => DataTypes.StringType
      case _ =>
        throw new Exception(s"class of type ${x.getClass} cannot be supported.")
    }

  }

}
