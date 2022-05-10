package org.apache.spark.secco.expression

import org.apache.spark.secco.analysis.TypeCheckResult
import org.apache.spark.secco.codegen.Block.BlockHelper
import org.apache.spark.secco.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteralValue}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.codegen.PredicateFunc
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.types.{AbstractDataType, AnyDataType, AtomicType, BooleanType, DataType, DoubleType, FloatType}
import org.apache.spark.secco.util.TypeUtils

object InterpretedPredicateFunc {
  def create(
      expression: Expression,
      inputSchema: Seq[Attribute]
  ): InterpretedPredicateFunc =
    create(BindReferences.bindReference(expression, inputSchema))

  def create(expression: Expression): InterpretedPredicateFunc =
    new InterpretedPredicateFunc(expression)
}

/** A [[PredicateFunc]] executed by interpreting expression tree. */
case class InterpretedPredicateFunc(expression: Expression)
    extends PredicateFunc {
  override def eval(r: InternalRow): Boolean =
    expression.eval(r).asInstanceOf[Boolean]

  override def initialize(partitionIndex: Int): Unit = {
    super.initialize(partitionIndex)
    expression.foreach {
//      case n: Nondeterministic => n.initialize(partitionIndex)
      case _ =>
    }
  }
}

/**
  * An [[Expression]] that returns a boolean value.
  */
trait Predicate extends Expression {
  override def dataType: DataType = BooleanType
}

trait PredicateHelper {
  protected def splitConjunctivePredicates(
      condition: Expression
  ): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  protected def splitDisjunctivePredicates(
      condition: Expression
  ): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  // Substitute any known alias from a map.
  protected def replaceAlias(
      condition: Expression,
      aliases: AttributeMap[Expression]
  ): Expression = {
    // Use transformUp to prevent infinite recursion when the replacement expression
    // redefines the same ExprId,
    condition.transformUp {
      case a: Attribute =>
        aliases.getOrElse(a, a)
    }
  }

//  /**
//   * Returns true if `expr` can be evaluated using only the output of `plan`.  This method
//   * can be used to determine when it is acceptable to move expression evaluation within a query
//   * plan.
//   *
//   * For example consider a join between two relations R(a, b) and S(c, d).
//   *
//   * - `canEvaluate(EqualTo(a,b), R)` returns `true`
//   * - `canEvaluate(EqualTo(a,c), R)` returns `false`
//   * - `canEvaluate(Literal(1), R)` returns `true` as literals CAN be evaluated on any plan
//   */
//  protected def canEvaluate(expr: Expression, plan: LogicalPlan): Boolean =
//    expr.references.subsetOf(plan.outputSet)
//
//  /**
//   * Returns true iff `expr` could be evaluated as a condition within join.
//   */
//  protected def canEvaluateWithinJoin(expr: Expression): Boolean = expr match {
//    // Non-deterministic expressions are not allowed as join conditions.
//    case e if !e.deterministic => false
//    case _: ListQuery | _: Exists =>
//      // A ListQuery defines the query which we want to search in an IN subquery expression.
//      // Currently the only way to evaluate an IN subquery is to convert it to a
//      // LeftSemi/LeftAnti/ExistenceJoin by `RewritePredicateSubquery` rule.
//      // It cannot be evaluated as part of a Join operator.
//      // An Exists shouldn't be push into a Join operator too.
//      false
//    case e: SubqueryExpression =>
//      // non-correlated subquery will be replaced as literal
//      e.children.isEmpty
//    case a: AttributeReference => true
//    case e: Unevaluable => false
//    case e => e.children.forall(canEvaluateWithinJoin)
//  }
}

case class Not(child: Expression) extends UnaryExpression with Predicate {
  override def toString: String = s"NOT $child"

//  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  // +---------+-----------+
  // | CHILD   | NOT CHILD |
  // +---------+-----------+
  // | TRUE    | FALSE     |
  // | FALSE   | TRUE      |
  // | UNKNOWN | UNKNOWN   |
  // +---------+-----------+
  protected override def nullSafeEval(input: Any): Any =
    !input.asInstanceOf[Boolean]

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"!($c)")
  }

  override def sql: String = s"(NOT ${child.sql})"
}

case class And(left: Expression, right: Expression) extends BinaryComparison {
  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "&&"

  override def sqlOperator: String = "AND"

  // +---------+---------+---------+---------+
  // | AND     | TRUE    | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  // | TRUE    | TRUE    | FALSE   | UNKNOWN |
  // | FALSE   | FALSE   | FALSE   | FALSE   |
  // | UNKNOWN | UNKNOWN | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    if (input1 == false) {
      false
    } else {
      val input2 = right.eval(input)
      if (input2 == false) {
        false
      } else {
        if (input1 != null && input2 != null) {
          true
        } else {
          null
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)

    // The result should be `false`, if any of them is `false` whenever the other is null or not.
    if (!left.nullable && !right.nullable) {
      ev.copy(
        code = code"""
        ${eval1.code}
        boolean ${ev.value} = false;

        if (${eval1.value}) {
          ${eval2.code}
          ${ev.value} = ${eval2.value};
        }""",
        isNull = FalseLiteralValue
      )
    } else {
      ev.copy(code = code"""
        ${eval1.code}
        boolean ${ev.isNull} = false;
        boolean ${ev.value} = false;

        if (!${eval1.isNull} && !${eval1.value}) {
        } else {
          ${eval2.code}
          if (!${eval2.isNull} && !${eval2.value}) {
          } else if (!${eval1.isNull} && !${eval2.isNull}) {
            ${ev.value} = true;
          } else {
            ${ev.isNull} = true;
          }
        }
      """)
    }
  }
}

case class Or(left: Expression, right: Expression) extends BinaryComparison {
  override def inputType: AbstractDataType = BooleanType

  override def symbol: String = "||"

  override def sqlOperator: String = "OR"

  // +---------+---------+---------+---------+
  // | OR      | TRUE    | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  // | TRUE    | TRUE    | TRUE    | TRUE    |
  // | FALSE   | TRUE    | FALSE   | UNKNOWN |
  // | UNKNOWN | TRUE    | UNKNOWN | UNKNOWN |
  // +---------+---------+---------+---------+
  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    if (input1 == true) {
      true
    } else {
      val input2 = right.eval(input)
      if (input2 == true) {
        true
      } else {
        if (input1 != null && input2 != null) {
          false
        } else {
          null
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)

    // The result should be `true`, if any of them is `true` whenever the other is null or not.
    if (!left.nullable && !right.nullable) {
      ev.isNull = FalseLiteralValue
      ev.copy(
        code = code"""
        ${eval1.code}
        boolean ${ev.value} = true;

        if (!${eval1.value}) {
          ${eval2.code}
          ${ev.value} = ${eval2.value};
        }""",
        isNull = FalseLiteralValue
      )
    } else {
      ev.copy(code = code"""
        ${eval1.code}
        boolean ${ev.isNull} = false;
        boolean ${ev.value} = true;

        if (!${eval1.isNull} && ${eval1.value}) {
        } else {
          ${eval2.code}
          if (!${eval2.isNull} && ${eval2.value}) {
          } else if (!${eval1.isNull} && !${eval2.isNull}) {
            ${ev.value} = false;
          } else {
            ${ev.isNull} = true;
          }
        }
      """)
    }
  }
}

//case class In(value: Expression, list: Seq[Expression]) extends Predicate {
//  require(list != null, "list should not be null")
//
//  override def children: Seq[Expression] = value +: list
//
//  override def nullable: Boolean = children.exists(_.nullable)
//  override def foldable: Boolean = children.forall(_.foldable)
//
//  override def toString: String = s"$value IN ${list.mkString("(", ",", ")")}"
//
//  override def eval(input: InternalRow): Any = ???
//}

abstract class BinaryComparison extends BinaryOperator with Predicate {

  // Note that we need to give a superset of allowable input types since orderable types are not
  // finitely enumerable. The allowable types are checked below by checkInputDataTypes.
  override def inputType: AbstractDataType = AnyDataType

//  override def checkInputDataTypes(): TypeCheckResult = super.checkInputDataTypes() match {
//    case TypeCheckResult.TypeCheckSuccess =>
//      TypeUtils.checkForOrderingExpr(left.dataType, this.getClass.getSimpleName)
//    case failure => failure
//  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (
      CodeGenerator.isPrimitiveType(left.dataType)
      && left.dataType != BooleanType // java boolean doesn't support > or < operator
      && left.dataType != FloatType
      && left.dataType != DoubleType
    ) {
      // faster version
      defineCodeGen(ctx, ev, (c1, c2) => s"$c1 $symbol $c2")
    } else {
      defineCodeGen(
        ctx,
        ev,
        (c1, c2) => s"${ctx.genComp(left.dataType, c1, c2)} $symbol 0"
      )
    }
  }

  protected lazy val ordering: Ordering[Any] =
    left.dataType.asInstanceOf[AtomicType].ordering.asInstanceOf[Ordering[Any]]
}

object BinaryComparison {
  def unapply(e: BinaryComparison): Option[(Expression, Expression)] =
    Some((e.left, e.right))
}
object Equality {
  def unapply(e: BinaryComparison): Option[(Expression, Expression)] = e match {
    case EqualTo(l, r) => Some((l, r))
    case EqualNullSafe(l, r) => Some((l, r))
    case _ => None
  }
}
case class EqualNullSafe(left: Expression, right: Expression) extends BinaryComparison {

  override def symbol: String = "<=>"

  override def nullable: Boolean = false

  // +---------+---------+---------+---------+
  // | <=>     | TRUE    | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  // | TRUE    | TRUE    | FALSE   | FALSE   |
  // | FALSE   | FALSE   | TRUE    | FALSE   |
  // | UNKNOWN | FALSE   | FALSE   | TRUE    |
  // +---------+---------+---------+---------+
  override def eval(input: InternalRow): Any = {
    val input1 = left.eval(input)
    val input2 = right.eval(input)
    if (input1 == null && input2 == null) {
      true
    } else if (input1 == null || input2 == null) {
      false
    } else {
      ordering.equiv(input1, input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val equalCode = ctx.genEqual(left.dataType, eval1.value, eval2.value)
    ev.copy(code = eval1.code + eval2.code + code"""
        boolean ${ev.value} = (${eval1.isNull} && ${eval2.isNull}) ||
           (!${eval1.isNull} && !${eval2.isNull} && $equalCode);""", isNull = FalseLiteralValue)
  }

//  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): EqualNullSafe =
//    copy(left = newLeft, right = newRight)
}
case class EqualTo(left: Expression, right: Expression)
    extends BinaryComparison {
  override def symbol: String = "="

  // +---------+---------+---------+---------+
  // | =       | TRUE    | FALSE   | UNKNOWN |
  // +---------+---------+---------+---------+
  // | TRUE    | TRUE    | FALSE   | UNKNOWN |
  // | FALSE   | FALSE   | TRUE    | UNKNOWN |
  // | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN |
  // +---------+---------+---------+---------+
  protected override def nullSafeEval(left: Any, right: Any): Any =
    ordering.equiv(left, right)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c1, c2) => ctx.genEqual(left.dataType, c1, c2))
  }
}

case class GreaterThan(left: Expression, right: Expression)
    extends BinaryComparison {
  override def symbol: String = ">"

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    ordering.gt(input1, input2)
}

case class GreaterThanOrEqual(left: Expression, right: Expression)
    extends BinaryComparison {
  override def symbol: String = ">="

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    ordering.gteq(input1, input2)
}

case class LessThan(left: Expression, right: Expression)
    extends BinaryComparison {
  override def symbol: String = "<"

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    ordering.lt(input1, input2)
}

case class LessThanOrEqual(left: Expression, right: Expression)
    extends BinaryComparison {
  override def symbol: String = "<="

  protected override def nullSafeEval(input1: Any, input2: Any): Any =
    ordering.lteq(input1, input2)
}

case class In(value: Expression, list: Seq[Expression]) extends Predicate {

  require(list != null, "list should not be null")

   def checkInputDataTypes(): TypeCheckResult = {
    val mismatchOpt = list.find(l => !DataType.equalsStructurally(l.dataType, value.dataType,
      ignoreNullability = true))
    if (mismatchOpt.isDefined) {
      TypeCheckResult.TypeCheckFailure(s"Arguments must be same type but were: " +
        s"${value.dataType.catalogString} != ${mismatchOpt.get.dataType.catalogString}")
    } else {
      TypeUtils.checkForOrderingExpr(value.dataType, s"function $prettyName")
    }
  }

  override def children: Seq[Expression] = value +: list
  lazy val inSetConvertible = list.forall(_.isInstanceOf[Literal])
  private lazy val ordering = TypeUtils.getInterpretedOrdering(value.dataType)

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

//  final override val nodePatterns: Seq[TreePattern] = Seq(IN)

  override def toString: String = s"$value IN ${list.mkString("(", ",", ")")}"

  override def eval(input: InternalRow): Any = {
    val evaluatedValue = value.eval(input)
    if (evaluatedValue == null) {
      null
    } else {
      var hasNull = false
      list.foreach { e =>
        val v = e.eval(input)
        if (v == null) {
          hasNull = true
        } else if (ordering.equiv(v, evaluatedValue)) {
          return true
        }
      }
      if (hasNull) {
        null
      } else {
        false
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaDataType = CodeGenerator.javaType(value.dataType)
    val valueGen = value.genCode(ctx)
    val listGen = list.map(_.genCode(ctx))
    // inTmpResult has 3 possible values:
    // -1 means no matches found and there is at least one value in the list evaluated to null
    val HAS_NULL = -1
    // 0 means no matches found and all values in the list are not null
    val NOT_MATCHED = 0
    // 1 means one value in the list is matched
    val MATCHED = 1
    val tmpResult = ctx.freshName("inTmpResult")
    val valueArg = ctx.freshName("valueArg")
    // All the blocks are meant to be inside a do { ... } while (false); loop.
    // The evaluation of variables can be stopped when we find a matching value.
    val listCode = listGen.map(x =>
      s"""
         |${x.code}
         |if (${x.isNull}) {
         |  $tmpResult = $HAS_NULL; // ${ev.isNull} = true;
         |} else if (${ctx.genEqual(value.dataType, valueArg, x.value)}) {
         |  $tmpResult = $MATCHED; // ${ev.isNull} = false; ${ev.value} = true;
         |  continue;
         |}
       """.stripMargin)

    val codes = ctx.splitExpressionsWithCurrentInputs(
      expressions = listCode,
      funcName = "valueIn",
      extraArguments = (javaDataType, valueArg) :: (CodeGenerator.JAVA_BYTE, tmpResult) :: Nil,
      returnType = CodeGenerator.JAVA_BYTE,
      makeSplitFunction = body =>
        s"""
           |do {
           |  $body
           |} while (false);
           |return $tmpResult;
         """.stripMargin,
      foldFunctions = _.map { funcCall =>
        s"""
           |$tmpResult = $funcCall;
           |if ($tmpResult == $MATCHED) {
           |  continue;
           |}
         """.stripMargin
      }.mkString("\n"))

    ev.copy(code =
      code"""
            |${valueGen.code}
            |byte $tmpResult = $HAS_NULL;
            |if (!${valueGen.isNull}) {
            |  $tmpResult = $NOT_MATCHED;
            |  $javaDataType $valueArg = ${valueGen.value};
            |  do {
            |    $codes
            |  } while (false);
            |}
            |final boolean ${ev.isNull} = ($tmpResult == $HAS_NULL);
            |final boolean ${ev.value} = ($tmpResult == $MATCHED);
       """.stripMargin)
  }

  override def sql: String = {
    val valueSQL = value.sql
    val listSQL = list.map(_.sql).mkString(", ")
    s"($valueSQL IN ($listSQL))"
  }

//  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): In =
//    copy(value = newChildren.head, list = newChildren.tail)
}

