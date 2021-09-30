package org.apache.spark.secco.trees

import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.expression.{Attribute, Expression}
import org.apache.spark.secco.optimization.support.SessionSupport
import org.apache.spark.secco.types.DataType

/** An abstract query plan class
  */
abstract class QueryPlan[PlanType <: QueryPlan[PlanType]]
    extends TreeNode[PlanType]
    with SessionSupport {
  self: PlanType =>

  /** The output attributes */
  @deprecated
  def outputOld: Seq[String] = {
    throw new Exception(s"outputOld is deprecated, please use output instead.")
  }

  /** The output attributes */
  def output: Seq[Attribute]

  /** Returns the set of attributes that are output by this node.
    */
  def outputSet: AttributeSet = AttributeSet(output)

  /** All Attributes that appear in expressions from this operator.  Note that this set does not
    * include attributes that are implicitly referenced by being passed through to the output tuple.
    */
  def references: AttributeSet = AttributeSet(expressions.flatMap(_.references))

  /** The set of all attributes that are input to this operator by its children.
    */
  def inputSet: AttributeSet =
    AttributeSet(children.flatMap(_.asInstanceOf[QueryPlan[PlanType]].output))

  /** Runs [[transformExpressionsDown]] with `rule` on all expressions present
    * in this query operator.
    * Users should not expect a specific directionality. If a specific directionality is needed,
    * transformExpressionsDown or transformExpressionsUp should be used.
    *
    * @param rule the rule to be applied to every expression in this operator.
    */
  def transformExpressions(
      rule: PartialFunction[Expression, Expression]
  ): this.type = {
    transformExpressionsDown(rule)
  }

  /** Runs [[transformDown]] with `rule` on all expressions present in this query operator.
    *
    * @param rule the rule to be applied to every expression in this operator.
    */
  def transformExpressionsDown(
      rule: PartialFunction[Expression, Expression]
  ): this.type = {
    mapExpressions(_.transformDown(rule))
  }

  /** Runs [[transformUp]] with `rule` on all expressions present in this query operator.
    *
    * @param rule the rule to be applied to every expression in this operator.
    * @return
    */
  def transformExpressionsUp(
      rule: PartialFunction[Expression, Expression]
  ): this.type = {
    mapExpressions(_.transformUp(rule))
  }

  /** Returns the result of running [[transformExpressions]] on this node
    * and all its children.
    */
  def transformAllExpressions(
      rule: PartialFunction[Expression, Expression]
  ): this.type = {
    transform { case q: QueryPlan[_] =>
      q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /** Apply a map function to each expression present in this query operator, and return a new
    * query operator based on the mapped expressions.
    */
  def mapExpressions(f: Expression => Expression): this.type = {
    var changed = false

    @inline def transformExpression(e: Expression): Expression = {
      val newE = f(e)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef =
      arg match {
        case e: Expression       => transformExpression(e)
        case Some(value)         => Some(recursiveTransform(value))
        case m: Map[_, _]        => m
        case d: DataType         => d // Avoid unpacking Structs
        case seq: Traversable[_] => seq.map(recursiveTransform)
        case other: AnyRef       => other
        case null                => null
      }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /** Returns all of the expressions present in this query plan operator. */
  final def expressions: Seq[Expression] = {
    // Recursively find all expressions from a traversable.
    def seqToExpressions(seq: Traversable[Any]): Traversable[Expression] =
      seq.flatMap {
        case e: Expression     => e :: Nil
        case s: Traversable[_] => seqToExpressions(s)
        case other             => Nil
      }

    productIterator.flatMap {
      case e: Expression       => e :: Nil
      case s: Some[_]          => seqToExpressions(s.toSeq)
      case seq: Traversable[_] => seqToExpressions(seq)
      case other               => Nil
    }.toSeq
  }
}
