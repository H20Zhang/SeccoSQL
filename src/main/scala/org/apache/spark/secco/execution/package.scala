package org.apache.spark.secco

import org.apache.spark.secco.execution.planning.SeccoStrategy

package object execution {

  /** The underlying data storage is Double Type, which means
    * our system will store `char`, `char`, `int`, `float`, `long(small than < 10^15)`, `double` as Double.
    * It is worth noting that secco may fail when trying to store a long value that is too large.
    */
  type OldInternalDataType = Double
  type OldInternalRow = Array[Double]
  type Strategy = SeccoStrategy

  /** This class provides context for some instances of [[org.apache.spark.secco.trees.TreeNode]].
    *
    * Note: for preserve contextual information, this class overrides equals behaviors to reference based rather than
    * value based.
    */
  class SharedContext[T](val res: T) extends Serializable {

    override def equals(obj: Any): Boolean = {
      obj match {
        case anyRef: AnyRef => this.eq(anyRef)
        case _              => super.equals(obj)
      }
    }
  }

  object SharedContext {
    def apply[T](res: T): SharedContext[T] = new SharedContext(res)
  }

}
