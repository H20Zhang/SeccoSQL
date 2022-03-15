package org.apache.spark.secco

import org.apache.spark.secco.execution.planning.SeccoStrategy

package object execution {

  /** The underlying data storage is Double Type, which means
    * our system will store `char`, `char`, `int`, `float`, `long(small than < 10^15)`, `double` as Double.
    * It is worth noting that secco may fail when trying to store a long value that is too large.
    */
//  type OldInternalDataType = Double
//  type OldInternalRow = Array[Double]
  type Strategy = SeccoStrategy

  //TODO: this is a special case class whose equals are reference based, this is a bit strange, makes it a normal class.
  case class SharedParameter[T](res: T) {

    override def equals(obj: Any): Boolean = {
      obj match {
        case anyRef: AnyRef => this.eq(anyRef)
        case _              => super.equals(obj)
      }
    }
  }

}
