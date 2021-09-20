package org.apache.spark.dolphin.expression

import org.apache.spark.dolphin.codegen.Block.BlockHelper
import org.apache.spark.dolphin.codegen.{
  CodeGenerator,
  CodegenContext,
  ExprCode,
  FalseLiteralValue,
  JavaCode
}
import org.apache.spark.dolphin.execution.storage.row.InternalRow
import org.apache.spark.dolphin.trees.attachTree
import org.apache.spark.dolphin.types.DataType
import org.apache.spark.internal.Logging

/**
  * A bound reference points to a specific slot in the input tuple, allowing the actual value
  * to be retrieved more efficiently.  However, since operations like column pruning can change
  * the layout of intermediate tuples, BindReferences should be run after all such transformations.
  */
case class BoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
    extends LeafExpression {

  override def toString: String =
    s"input[$ordinal, ${dataType.simpleString}, $nullable]"

  private val accessor: (InternalRow, Int) => Any =
    InternalRow.getAccessor(dataType)

  // Use special getter for primitive types (for UnsafeRow)
  override def eval(input: InternalRow): Any = {
    if (nullable && input.isNullAt(ordinal)) {
      null
    } else {
      accessor(input, ordinal)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if (ctx.currentVars != null && ctx.currentVars(ordinal) != null) {
      val oev = ctx.currentVars(ordinal)
      ev.isNull = oev.isNull
      ev.value = oev.value
      ev.copy(code = oev.code)
    } else {
      assert(
        ctx.INPUT_ROW != null,
        "INPUT_ROW and currentVars cannot both be null."
      )
      val javaType = JavaCode.javaType(dataType)
      val value =
        CodeGenerator.getValue(ctx.INPUT_ROW, dataType, ordinal.toString)
      if (nullable) {
        ev.copy(code = code"""
                |boolean ${ev.isNull} = ${ctx.INPUT_ROW}.isNullAt($ordinal);
                |$javaType ${ev.value} = ${ev.isNull} ?
                |  ${CodeGenerator.defaultValue(dataType)} : ($value);
           """.stripMargin)
      } else {
        ev.copy(
          code = code"$javaType ${ev.value} = $value;",
          isNull = FalseLiteralValue
        )
      }
    }
  }
}

object BindReferences extends Logging {

  def bindReference[A <: Expression](
      expression: A,
      input: AttributeSeq,
      allowFailures: Boolean = false
  ): A = {
    expression
      .transform {
        case a: AttributeReference =>
          attachTree(a, "Binding attribute") {
            val ordinal = input.indexOf(a.exprId)
            if (ordinal == -1) {
              if (allowFailures) {
                a
              } else {
                sys.error(
                  s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}"
                )
              }
            } else {
              BoundReference(ordinal, a.dataType, input(ordinal).nullable)
            }
          }
      }
      .asInstanceOf[
        A
      ] // Kind of a hack, but safe.  TODO: Tighten return type when possible.
  }
}
