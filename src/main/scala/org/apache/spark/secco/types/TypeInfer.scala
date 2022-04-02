package org.apache.spark.secco.types

object TypeInfer {

  def inferType(x: Any): DataType = {
    x match {
      case s: Boolean => DataTypes.BooleanType
      case s: Int     => DataTypes.IntegerType
      case s: Long    => DataTypes.LongType
      case s: Float   => DataTypes.FloatType
      case s: Double  => DataTypes.DoubleType
      case s: String  => DataTypes.StringType
      case _ =>
        throw new Exception(s"class of type ${x.getClass} cannot be supported.")
    }

  }

}
