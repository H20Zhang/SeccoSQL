package org.apache.spark.secco.expression.codegen

import org.apache.spark.secco.codegen.CodeGenerator
import org.apache.spark.secco.execution.storage.block.TrieInternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

abstract class BaseLeapFrogJoinIteratorProducer {
  def getIterator(tries: Array[TrieInternalBlock]): java.util.Iterator[AnyRef]
}

object GenerateLeapFrogJoinIterator extends
  CodeGenerator[(Seq[Attribute], Seq[Seq[Attribute]]), BaseLeapFrogJoinIteratorProducer] {
  /** Generates a class for a given input expression.  Called when there is not cached code
    * already available.
    */
  override protected def create(in: (Seq[Attribute], Seq[Seq[Attribute]])): BaseLeapFrogJoinIteratorProducer
  = create(in._1, in._2)

  /** Canonicalizes an input expression. Used to avoid double caching expressions that differ only
    * cosmetically.
    */
  override protected def canonicalize(in: (Seq[Attribute], Seq[Seq[Attribute]])): (Seq[Attribute], Seq[Seq[Attribute]])
  = in

  /** Binds an input expression to a given input schema */
  override protected def bind(in: (Seq[Attribute], Seq[Seq[Attribute]]), inputSchema: Seq[Attribute]):
  (Seq[Attribute], Seq[Seq[Attribute]])
  = in

  def create(schema: Seq[Attribute], childrenSchemas: Seq[Seq[Attribute]]): BaseLeapFrogJoinIteratorProducer = ???
}
