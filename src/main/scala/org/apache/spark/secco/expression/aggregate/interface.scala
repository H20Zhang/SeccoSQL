package org.apache.spark.secco.expression.aggregate

import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen.{
  CodeGenerator,
  CodegenContext,
  ExprCode,
  FalseLiteralValue
}
import org.apache.spark.secco.errors.QueryExecutionErrors
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{
  AttributeReference,
  Expression,
  LeafExpression,
  Unevaluable
}
import org.apache.spark.secco.types.StructType
//import org.apache.spark.sql.catalyst.expressions.codegen.FalseLiteral
//import org.apache.spark.sql.catalyst.expressions.Nondeterministic

//TODO: implement this
abstract class AggregateFunction extends Expression {

  /** An aggregate function is not foldable. */
  final override def foldable: Boolean = false

  /** The schema of the aggregation buffer. */
  def aggBufferSchema: StructType

  /** Attributes of fields in aggBufferSchema. */
  def aggBufferAttributes: Seq[AttributeReference]

  /** Attributes of fields in input aggregation buffers (immutable aggregation buffers that are
    * merged with mutable aggregation buffers in the merge() function or merge expressions).
    * These attributes are created automatically by cloning the [[aggBufferAttributes]].
    */
  def inputAggBufferAttributes: Seq[AttributeReference]

  def sql(isDistinct: Boolean): String = {
    val distinct = if (isDistinct) "DISTINCT " else ""
    s"$prettyName($distinct${children.map(_.sql).mkString(", ")})"
  }

  /** String representation used in explain plans. */
  def toAggString(isDistinct: Boolean): String = {
    val start = if (isDistinct) "(distinct " else "("
    prettyName + flatArguments.mkString(start, ", ", ")")
  }
}

/** API for aggregation functions that are expressed in terms of imperative initialize(), update(),
  * and merge() functions which operate on Row-based aggregation buffers.
  *
  * Within these functions, code should access fields of the mutable aggregation buffer by adding the
  * bufferSchema-relative field number to `mutableAggBufferOffset` then using this new field number
  * to access the buffer Row. This is necessary because this aggregation function's buffer is
  * embedded inside of a larger shared aggregation buffer when an aggregation operator evaluates
  * multiple aggregate functions at the same time.
  *
  * We need to perform similar field number arithmetic when merging multiple intermediate
  * aggregate buffers together in `merge()` (in this case, use `inputAggBufferOffset` when accessing
  * the input buffer).
  *
  * Correct ImperativeAggregate evaluation depends on the correctness of `mutableAggBufferOffset` and
  * `inputAggBufferOffset`, but not on the correctness of the attribute ids in `aggBufferAttributes`
  * and `inputAggBufferAttributes`.
  */
abstract class ImperativeAggregate extends AggregateFunction {

  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // LeafNode does not need `input`
    val input = if (this.isInstanceOf[LeafExpression]) "null" else ctx.INPUT_ROW
    val idx = ctx.references.length
    ctx.references += this
    val objectTerm = ctx.freshName("obj")
    val placeHolder = ctx.registerComment(this.toString)
    val javaType = CodeGenerator.javaType(this.dataType)
    if (nullable) {
      ev.copy(code = code"""
        $placeHolder
        Object $objectTerm = ((Expression) references[$idx]).eval($input);
        boolean ${ev.isNull} = $objectTerm == null;
        $javaType ${ev.value} = ${CodeGenerator.defaultValue(this.dataType)};
        if (!${ev.isNull}) {
          ${ev.value} = (${CodeGenerator.boxedType(this.dataType)}) $objectTerm;
        }""")
    } else {
      ev.copy(
        code = code"""
        $placeHolder
        Object $objectTerm = ((Expression) references[$idx]).eval($input);
        $javaType ${ev.value} = (${CodeGenerator
          .boxedType(this.dataType)}) $objectTerm;
        """,
        isNull = FalseLiteralValue
      )
    }
  }

  /** The offset of this function's start buffer value in the underlying shared input aggregation
    * buffer. An input aggregation buffer is used when we merge two aggregation buffers together in
    * the `update()` function and is immutable (we merge an input aggregation buffer and a mutable
    * aggregation buffer and then store the new buffer values to the mutable aggregation buffer).
    *
    * An input aggregation buffer may contain extra fields, such as grouping keys, at its start, so
    * mutableAggBufferOffset and inputAggBufferOffset are often different.
    *
    * For example, say we have a grouping expression, `key`, and two aggregate functions,
    * `avg(x)` and `avg(y)`. In the shared input aggregation buffer, the position of the first
    * buffer value of `avg(x)` will be 1 and the position of the first buffer value of `avg(y)`
    * will be 3 (position 0 is used for the value of `key`):
    * {{{
    *          avg(x) inputAggBufferOffset = 1
    *                   |
    *                   v
    *          +--------+--------+--------+--------+--------+
    *          |  key   |  sum1  | count1 |  sum2  | count2 |
    *          +--------+--------+--------+--------+--------+
    *                                     ^
    *                                     |
    *                       avg(y) inputAggBufferOffset = 3
    * }}}
    */
  protected val inputAggBufferOffset: Int

  /** Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
    * This new copy's attributes may have different ids than the original.
    */
  def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int
  ): ImperativeAggregate

  /** The offset of this function's first buffer value in the underlying shared mutable aggregation
    * buffer.
    *
    * For example, we have two aggregate functions `avg(x)` and `avg(y)`, which share the same
    * aggregation buffer. In this shared buffer, the position of the first buffer value of `avg(x)`
    * will be 0 and the position of the first buffer value of `avg(y)` will be 2:
    * {{{
    *          avg(x) mutableAggBufferOffset = 0
    *                  |
    *                  v
    *                  +--------+--------+--------+--------+
    *                  |  sum1  | count1 |  sum2  | count2 |
    *                  +--------+--------+--------+--------+
    *                                    ^
    *                                    |
    *                     avg(y) mutableAggBufferOffset = 2
    * }}}
    */
  protected val mutableAggBufferOffset: Int

  /** Returns a copy of this ImperativeAggregate with an updated mutableAggBufferOffset.
    * This new copy's attributes may have different ids than the original.
    */
  def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int
  ): ImperativeAggregate

  // Note: although all subclasses implement inputAggBufferAttributes by simply cloning
  // aggBufferAttributes, that common clone code cannot be placed here in the abstract
  // ImperativeAggregate class, since that will lead to initialization ordering issues.

  /** Initializes the mutable aggregation buffer located in `mutableAggBuffer`.
    *
    * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
    */
  def initialize(mutableAggBuffer: InternalRow): Unit

  /** Updates its aggregation buffer, located in `mutableAggBuffer`, based on the given `inputRow`.
    *
    * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
    *
    * Note that, the input row may be produced by unsafe projection and it may not be safe to cache
    * some fields of the input row, as the values can be changed unexpectedly.
    */
  def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit

  /** Combines new intermediate results from the `inputAggBuffer` with the existing intermediate
    * results in the `mutableAggBuffer.`
    *
    * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
    * Use `fieldNumber + inputAggBufferOffset` to access fields of `inputAggBuffer`.
    *
    * Note that, the input row may be produced by unsafe projection and it may not be safe to cache
    * some fields of the input row, as the values can be changed unexpectedly.
    */
  def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit
}

/** API for aggregation functions that are expressed in terms of Catalyst expressions.
  *
  * When implementing a new expression-based aggregate function, start by implementing
  * `bufferAttributes`, defining attributes for the fields of the mutable aggregation buffer. You
  * can then use these attributes when defining `updateExpressions`, `mergeExpressions`, and
  * `evaluateExpressions`.
  *
  * Please note that children of an aggregate function can be unresolved (it will happen when
  * we create this function in DataFrame API). So, if there is any fields in
  * the implemented class that need to access fields of its children, please make
  * those fields `lazy val`s.
  */
abstract class DeclarativeAggregate
    extends AggregateFunction
    with Serializable {

  /** Expressions for initializing empty aggregation buffers.
    */
  val initialValues: Seq[Expression]

  /** Expressions for updating the mutable aggregation buffer based on an input row.
    */
  val updateExpressions: Seq[Expression]

  /** A sequence of expressions for merging two aggregation buffers together. When defining these
    * expressions, you can use the syntax `attributeName.left` and `attributeName.right` to refer
    * to the attributes corresponding to each of the buffers being merged (this magic is enabled
    * by the [[RichAttribute]] implicit class).
    */
  val mergeExpressions: Seq[Expression]

  /** An expression which returns the final value for this aggregate function. Its data type should
    * match this expression's [[dataType]].
    */
  val evaluateExpression: Expression

  /** An expression-based aggregate's bufferSchema is derived from bufferAttributes. */
  final override def aggBufferSchema: StructType =
    StructType.fromAttributes(aggBufferAttributes)

  final lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  /** A helper class for representing an attribute used in merging two
    * aggregation buffers. When merging two buffers, `bufferLeft` and `bufferRight`,
    * we merge buffer values and then update bufferLeft. A [[RichAttribute]]
    * of an [[AttributeReference]] `a` has two functions `left` and `right`,
    * which represent `a` in `bufferLeft` and `bufferRight`, respectively.
    */
  implicit class RichAttribute(a: AttributeReference) {

    /** Represents this attribute at the mutable buffer side. */
    def left: AttributeReference = a

    /** Represents this attribute at the input buffer side (the data value is read-only). */
    def right: AttributeReference = inputAggBufferAttributes(
      aggBufferAttributes.indexOf(a)
    )
  }

  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  final override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

//TODO: 1. AggregateFunction
// 2. ImperativeAggregateFunction & DeclarativeAggregateFunction
// 3. UserDefinedAggregateFunction (Imperative)
// 4. Count, Sum, Min, Max, Avg (Declarative)
// 5. MutableProjection
// 6. Hash-based AggregationIterator
// 7. Implement other expression class used in the children of DeclarativeAggregateFunction
