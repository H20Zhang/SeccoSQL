package org.apache.spark.secco.expression.codegen

import org.apache.spark.secco.codegen.{
  CodeAndComment,
  CodeGenerator,
  CodegenContext,
  ExprCode,
  ExprValue,
  FalseLiteralValue
}
import org.apache.spark.secco.execution.storage.row.GenericInternalRow
import org.apache.spark.secco.expression.Literal.FalseLiteral
import org.apache.spark.secco.expression.{
  Attribute,
  BindReferences,
  BoundReference,
  Expression,
  NoOp
}
import org.apache.spark.secco.types.DataType
import org.apache.spark.secco.util.DebugUtils.printlnDebug

/** Java can not access Projection (in package object)
  */
abstract class BaseProjection extends Projection {}

object GenerateSafeProjection
    extends CodeGenerator[Seq[Expression], Projection] {

  override protected def bind(
      in: Seq[Expression],
      inputSchema: Seq[Attribute]
  ): Seq[Expression] = in.map(BindReferences.bindReference(_, inputSchema))

  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] = in

//  @tailrec
  private def convertToSafe(
      ctx: CodegenContext,
      input: ExprValue,
      dataType: DataType
  ): ExprCode =
    dataType match {
//    case s: StructType => createCodeForStruct(ctx, input, s)
//    case ArrayType(elementType, _) => createCodeForArray(ctx, input, elementType)
//    case MapType(keyType, valueType, _) => createCodeForMap(ctx, input, keyType, valueType)
//    case udt: UserDefinedType[_] => convertToSafe(ctx, input, udt.sqlType)
      case _ => ExprCode(FalseLiteralValue, input)
    }

  override protected def create(expressions: Seq[Expression]): Projection = {
    val ctx = newCodeGenContext()
    val expressionCodes = expressions.zipWithIndex.map {
      case (NoOp, _) => ""
      case (e, i) =>
        val evaluationCode = e.genCode(ctx)
        val converter = convertToSafe(ctx, evaluationCode.value, e.dataType)
        evaluationCode.code +
          s"""
            if (${evaluationCode.isNull}) {
              mutableRow.setNullAt($i);
            } else {
              ${converter.code}
              ${CodeGenerator.setColumn(
            "mutableRow",
            e.dataType,
            i,
            converter.value
          )};
            }
          """
    }
    val allExpressions = ctx.splitExpressionsWithCurrentInputs(expressionCodes)

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificSafeProjection(references);
      }

      class SpecificSafeProjection extends ${classOf[BaseProjection].getName} {

        private Object[] references;
        private InternalRow mutableRow;
        ${ctx.declareMutableStates()}

        public SpecificSafeProjection(Object[] references) {
          this.references = references;
          mutableRow = (InternalRow) references[references.length - 1];
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        public java.lang.Object apply(java.lang.Object _i) {
          InternalRow ${ctx.INPUT_ROW} = (InternalRow) _i;
          $allExpressions
          return mutableRow;
        }

        ${ctx.declareAddedFunctions()}
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments())
    )
    logDebug(
      s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}"
    )

    val (clazz, _) = CodeGenerator.compile(code)
    val resultRow = new GenericInternalRow(
      expressions.map(_.dataType.asInstanceOf[Any]).toArray
    )
    clazz.generate(ctx.references.toArray :+ resultRow).asInstanceOf[Projection]

  }

}
