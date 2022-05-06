package org.apache.spark.secco.expression

import java.util.Locale
import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteralValue, JavaCode}
import org.apache.spark.secco.errors.QueryExecutionErrors
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.trees.TreeNode
import org.apache.spark.secco.types.{AbstractDataType, DataType}
import org.apache.spark.secco.analysis.TypeCoercion
import org.apache.spark.util.Utils

/** An expression in Catalyst.
  *
  * If an expression wants to be exposed in the function registry (so users can call it with
  * "name(arguments...)", the concrete implementation must be a case class whose constructor
  * arguments are all Expressions types. See [[Add]] for an example.
  *
  * There are a few important traits:
  *
  * - [[Unevaluable]]: an expression that is not supposed to be evaluated.
  * - [[CodegenFallback]]: an expression that does not have code gen implemented and falls back to
  *                        interpreted mode.
  *
  * - [[LeafExpression]]: an expression that has no child.
  * - [[UnaryExpression]]: an expression that has one child.
  * - [[BinaryExpression]]: an expression that has two children.
  * - [[BinaryOperator]]: a special case of [[BinaryExpression]] that requires two children to have
  *                       the same output data type.
  */
abstract class Expression extends TreeNode[Expression] {

  /** Returns true when an expression is a candidate for static evaluation before the query is
    * executed.
    *
    * The following conditions are used to determine suitability for constant folding:
    *  - A [[Coalesce]] is foldable if all of its children are foldable
    *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
    *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
    *  - A [[Literal]] is foldable
    *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
    */
  def foldable: Boolean = false

  /** Returns true when the current expression always return the same result for fixed inputs from
    * children.
    *
    * Note that this means that an expression should be considered as non-deterministic if:
    * - it relies on some mutable internal state, or
    * - it relies on some implicit input that is not part of the children expression list.
    * - it has non-deterministic child or children.
    * - it assumes the input satisfies some certain condition via the child operator.
    *
    * An example would be `SparkPartitionID` that relies on the partition id returned by TaskContext.
    * By default leaf expressions are deterministic as Nil.forall(_.deterministic) returns true.
    */
  def deterministic: Boolean = children.forall(_.deterministic)

  def nullable: Boolean

  def references: AttributeSet =
    AttributeSet(children.flatMap(_.references))

  /** Returns the result of evaluating this expression on a given input Row */
  def eval(input: InternalRow = null): Any

  /** Returns an [[ExprCode]], that contains the Java source code to generate the result of
    * evaluating the expression on an input row.
    *
    * @param ctx a [[CodegenContext]]
    * @return [[ExprCode]]
    */
  def genCode(ctx: CodegenContext): ExprCode = {
//    ctx.subExprEliminationExprs.get(this).map { subExprState =>
//      // This expression is repeated which means that the code to evaluate it has already been added
//      // as a function before. In that case, we just re-use it.
//      ExprCode(ctx.registerComment(this.toString), subExprState.isNull, subExprState.value)
//    }.getOrElse {
    val isNull = ctx.freshName("isNull")
    val value = ctx.freshName("value")
    val eval = doGenCode(
      ctx,
      ExprCode(
        JavaCode.isNullVariable(isNull),
        JavaCode.variable(value, dataType)
      )
    )
    if (eval.code.toString.nonEmpty) {
      // Add `this` in the comment.
      eval.copy(code = ctx.registerComment(this.toString) + eval.code)
    } else {
      eval
    }
//    }
  }

  /** Returns Java source code that can be compiled to evaluate this expression.
    * The default behavior is to call the eval method of the expression. Concrete expression
    * implementations should override this to do actual code generation.
    *
    * @param ctx a [[CodegenContext]]
    * @param ev an [[ExprCode]] with unique terms， which is to be assigned value of the expression.
    * @return an [[ExprCode]] containing the Java source code to generate the given expression
    */
  protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode

  /** Returns `true` if this expression and all its children have been resolved to a specific schema
    * and input data types checking passed, and `false` if it still contains any unresolved
    * placeholders or has data types mismatch.
    * Implementations of expressions should override this if the resolution of this type of
    * expression involves more than just the resolution of its children and type checking.
    */
  lazy val resolved: Boolean = childrenResolved
//  && checkInputDataTypes().isSuccess

  /** Returns the [[DataType]] of the result of evaluating this expression.  It is
    * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
    */
  def dataType: DataType

  /** Returns true if  all the children of this expression have been resolved to a specific schema
    * and false if any still contains any unresolved placeholders.
    */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /** Returns an expression where a best effort attempt has been made to transform `this` in a way
    * that preserves the result but removes cosmetic variations (case sensitivity, ordering for
    * commutative operations, etc.)  See [[Canonicalize]] for more details.
    *
    * `deterministic` expressions where `this.canonicalized == other.canonicalized` will always
    * evaluate to the same result.
    */
  lazy val canonicalized: Expression = {
    val canonicalizedChildren = children.map(_.canonicalized)
    Canonicalize.execute(withNewChildren(canonicalizedChildren))
  }

  /** Returns true when two expressions will always compute the same result, even if they differ
    * cosmetically (i.e. capitalization of names in attributes may be different).
    *
    * See [[Canonicalize]] for more details.
    */
  def semanticEquals(other: Expression): Boolean =
    deterministic && other.deterministic && canonicalized == other.canonicalized

  /** Returns a `hashCode` for the calculation performed by this expression. Unlike the standard
    * `hashCode`, an attempt has been made to eliminate cosmetic differences.
    *
    * See [[Canonicalize]] for more details.
    */
  def semanticHash(): Int = canonicalized.hashCode()

//  /**
//    * Checks the input data types, returns `TypeCheckResult.success` if it's valid,
//    * or returns a `TypeCheckResult` with an error message if invalid.
//    * Note: it's not valid to call this method until `childrenResolved == true`.
//    */
//  def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  /** Returns a user-facing string representation of this expression's name.
    * This should usually match the name of the function in SQL.
    */
  def prettyName: String = nodeName.toLowerCase(Locale.ROOT)

  protected def flatArguments: Iterator[Any] =
    productIterator.flatMap {
      case t: Traversable[_] => t
      case single            => single :: Nil
    }

  // Marks this as final, Expression.verboseString should never be called, and thus shouldn't be
  // overridden by concrete classes.
  final override def verboseString: String = simpleString

  override def simpleString: String = toString

  override def toString: String =
    prettyName + Utils.truncatedString(flatArguments.toSeq, "(", ", ", ")")

  /** Returns SQL representation of this expression.  For expressions extending [[NonSQLExpression]],
    * this method may return an arbitrary user facing string.
    */
  def sql: String = {
    val childrenSQL = children.map(_.sql).mkString(", ")
    s"$prettyName($childrenSQL)"
  }

}

/** An expression that cannot be evaluated. Some expressions don't live past analysis or optimization
  * time (e.g. Star). This trait is used by those expressions.
  */
trait Unevaluable extends Expression {
  /** Unevaluable is not foldable because we don't have an eval for it. */
  final override def foldable: Boolean = false

  final override def eval(input: InternalRow = null): Any =
    throw QueryExecutionErrors.cannotEvaluateExpressionError(this)

  final override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw QueryExecutionErrors.cannotGenerateCodeForExpressionError(this)
}

/** A leaf expression, i.e. one without any child expressions.
  */
abstract class LeafExpression extends Expression {
  override final def children: Seq[Expression] = Nil
}

/** An expression with one input and one output. The output is by default evaluated to null
  * if the input is evaluated to null.
  */
abstract class UnaryExpression extends Expression {

  def child: Expression

  override final def children: Seq[Expression] = child :: Nil

  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  /** Default behavior of evaluation according to the default nullability of UnaryExpression.
    * If subclass of UnaryExpression override nullable, probably should also override this.
    */
  override def eval(input: InternalRow): Any = {
    val value = child.eval(input)
    if (value == null) {
      null
    } else {
      nullSafeEval(value)
    }
  }

  /** Called by default [[eval]] implementation.  If subclass of UnaryExpression keep the default
    * nullability, they can override this method to save null-check code.  If we need full control
    * of evaluation process, we should override [[eval]].
    */
  protected def nullSafeEval(input: Any): Any =
    sys.error(s"UnaryExpressions must override either eval or nullSafeEval")

  /** Called by unary expressions to generate a code block that returns null if its parent returns
    * null, and if not null, use `f` to generate the expression.
    *
    * As an example, the following does a boolean inversion (i.e. NOT).
    * {{{
    *   defineCodeGen(ctx, ev, c => s"!($c)")
    * }}}
    *
    * @param f function that accepts a variable name and returns Java code to compute the output.
    */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String
  ): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      eval => {
        s"${ev.value} = ${f(eval)};"
      }
    )
  }

  /** Called by unary expressions to generate a code block that returns null if its parent returns
    * null, and if not null, use `f` to generate the expression.
    *
    * @param f function that accepts the non-null evaluation result name of child and returns Java
    *          code to compute the output.
    */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: String => String
  ): ExprCode = {
    val childGen = child.genCode(ctx)
    val resultCode = f(childGen.value)

    if (nullable) {
      val nullSafeEval =
        ctx.nullSafeExec(child.nullable, childGen.isNull)(resultCode)
      ev.copy(code = code"""
        ${childGen.code}
        boolean ${ev.isNull} = ${childGen.isNull};
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator
        .defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(
        code = code"""
        ${childGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator
          .defaultValue(dataType)};
        $resultCode""",
        isNull = FalseLiteralValue
      )
    }
  }
}

abstract class BinaryExpression extends Expression {

  def left: Expression
  def right: Expression

  override final def children: Seq[Expression] = Seq(left, right)

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable || right.nullable

  /** Default behavior of evaluation according to the default nullability of BinaryExpression.
    * If subclass of BinaryExpression override nullable, probably should also override this.
    */
  override def eval(input: InternalRow): Any = {
    val value1 = left.eval(input)
    if (value1 == null) {
      null
    } else {
      val value2 = right.eval(input)
      if (value2 == null) {
        null
      } else {
        nullSafeEval(value1, value2)
      }
    }
  }

  /** Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
    * nullability, they can override this method to save null-check code.  If we need full control
    * of evaluation process, we should override [[eval]].
    */
  protected def nullSafeEval(input1: Any, input2: Any): Any =
    sys.error(s"BinaryExpressions must override either eval or nullSafeEval")

  /** Short hand for generating binary evaluation code.
    * If either of the sub-expressions is null, the result of this computation
    * is assumed to be null.
    *
    * @param f accepts two variable names and returns Java code to compute the output.
    */
  protected def defineCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String
  ): ExprCode = {
    nullSafeCodeGen(
      ctx,
      ev,
      (eval1, eval2) => {
        s"${ev.value} = ${f(eval1, eval2)};"
      }
    )
  }

  /** Short hand for generating binary evaluation code.
    * If either of the sub-expressions is null, the result of this computation
    * is assumed to be null.
    *
    * @param f function that accepts the 2 non-null evaluation result names of children
    *          and returns Java code to compute the output.
    */
  protected def nullSafeCodeGen(
      ctx: CodegenContext,
      ev: ExprCode,
      f: (String, String) => String
  ): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val resultCode = f(leftGen.value, rightGen.value)

    if (nullable) {
      val nullSafeEval =
        leftGen.code + ctx.nullSafeExec(left.nullable, leftGen.isNull) {
          rightGen.code + ctx.nullSafeExec(right.nullable, rightGen.isNull) {
            s"""
              ${ev.isNull} = false; // resultCode could change nullability.
              $resultCode
            """
          }
        }

      ev.copy(code = code"""
        boolean ${ev.isNull} = true;
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator
        .defaultValue(dataType)};
        $nullSafeEval
      """)
    } else {
      ev.copy(
        code = code"""
        ${leftGen.code}
        ${rightGen.code}
        ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator
          .defaultValue(dataType)};
        $resultCode""",
        isNull = FalseLiteralValue
      )
    }
  }
}

/** TODO: add type check
  *
  * A [[BinaryExpression]] that is an operator, with two properties:
  *
  * 1. The string representation is "x symbol y", rather than "funcName(x, y)".
  *
  * 2. Two inputs are expected to be of the same type. If the two inputs have different types,
  *    the analyzer will find the tightest common type and do the proper type casting.
  */
abstract class BinaryOperator extends BinaryExpression {

  /** Expected input type from both left/right child expressions, similar to the
    * [[ImplicitCastInputTypes]] trait.
    */
  def inputType: AbstractDataType

  def symbol: String

  def sqlOperator: String = symbol

  override def toString: String = s"($left $symbol $right)"

  def inputTypes: Seq[AbstractDataType] = Seq(inputType, inputType)

  override def sql: String = s"(${left.sql} $sqlOperator ${right.sql})"

}

object BinaryOperator {
  def unapply(e: BinaryOperator): Option[(Expression, Expression)] =
    Some((e.left, e.right))
}

/**
  * A trait used for resolving nullable flags, including `nullable`, `containsNull` of [[ArrayType]]
  * and `valueContainsNull` of [[MapType]], containsNull, valueContainsNull flags of the output date
  * type. This is usually utilized by the expressions (e.g. [[CaseWhen]]) that combine data from
  * multiple child expressions of non-primitive types.
  */
trait ComplexTypeMergingExpression extends Expression {

  /**
    * A collection of data types used for resolution the output type of the expression. By default,
    * data types of all child expressions. The collection must not be empty.
    */
  @transient
  lazy val inputTypesForMerging: Seq[DataType] = children.map(_.dataType)

  def dataTypeCheck: Unit = {
    require(
      inputTypesForMerging.nonEmpty,
      "The collection of input data types must not be empty.")
    require(
      TypeCoercion.haveSameType(inputTypesForMerging),
      "All input types must be the same except nullable, containsNull, valueContainsNull flags." +
        s" The input types found are\n\t${inputTypesForMerging.mkString("\n\t")}")
  }

  private lazy val internalDataType: DataType = {
    dataTypeCheck
    inputTypesForMerging.reduceLeft(TypeCoercion.findCommonTypeDifferentOnlyInNullFlags(_, _).get)
  }

  override def dataType: DataType = internalDataType
}

/**
  * Common base trait for user-defined functions, including UDF/UDAF/UDTF of different languages
  * and Hive function wrappers.
  */
trait UserDefinedExpression {
  def name: String
}
