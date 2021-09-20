package org.apache.spark.dolphin.expression

import org.apache.spark.dolphin.execution.storage.row.InternalRow

package object codegen {

//TODO: import Dolphin's RuleExecutor and uncomment below lines to make it work.
//  /** Canonicalizes an expression so those that differ only by names can reuse the same code. */
//  object ExpressionCanonicalizer extends RuleExecutor[Expression] {
//    val batches =
//      Batch("CleanExpressions", FixedPoint(20), CleanExpressions) :: Nil
//
//    object CleanExpressions extends Rule[Expression] {
//      def apply(e: Expression): Expression = e transform {
//        case Alias(c, _) => c
//      }
//    }
//  }

  /**
    * Converts a [[InternalRow]] to another Row given a sequence of expression that define each
    * column of the new row. If the schema of the input row is specified, then the given expression
    * will be bound to that schema.
    */
  abstract class Projection extends (InternalRow => InternalRow) {

    /**
      * Initializes internal states given the current partition index.
      * This is used by nondeterministic expressions to set initial states.
      * The default implementation does nothing.
      */
    def initialize(partitionIndex: Int): Unit = {}
  }

}
