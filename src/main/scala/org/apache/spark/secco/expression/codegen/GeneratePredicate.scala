package org.apache.spark.secco.expression.codegen

import org.apache.spark.secco.codegen.{CodeAndComment, CodeGenerator}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{Attribute, BindReferences, Expression}

/**
  * Interface for generated predicate
  */
abstract class PredicateFunc {
  def eval(r: InternalRow): Boolean

  /**
    * Initializes internal states given the current partition index.
    * This is used by nondeterministic expressions to set initial states.
    * The default implementation does nothing.
    */
  def initialize(partitionIndex: Int): Unit = {}
}

object GeneratePredicate extends CodeGenerator[Expression, PredicateFunc] {

  override protected def create(predicate: Expression): PredicateFunc = {
    val ctx = newCodeGenContext()
    val eval = predicate.genCode(ctx)

    val codeBody = s"""
      public SpecificPredicateFunc generate(Object[] references) {
        return new SpecificPredicateFunc(references);
      }

      class SpecificPredicateFunc extends ${classOf[PredicateFunc].getName} {
        private final Object[] references;
        ${ctx.declareMutableStates()}

        public SpecificPredicateFunc(Object[] references) {
          this.references = references;
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        public boolean eval(InternalRow ${ctx.INPUT_ROW}) {
          ${eval.code}
          return !${eval.isNull} && ${eval.value};
        }

        ${ctx.declareAddedFunctions()}
      }"""

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments())
    )
    logDebug(
      s"Generated predicate '$predicate':\n${CodeFormatter.format(code)}"
    )

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[PredicateFunc]

  }

  //TODO: make the ExpressionCanonicalizer work, currently, we just bypass it
  override protected def canonicalize(in: Expression): Expression = {
//    val canonlicalizedExpr = ExpressionCanonicalizer.execute(in)
    in
  }

  override protected def bind(
      in: Expression,
      inputSchema: Seq[Attribute]
  ): Expression = BindReferences.bindReference(in, inputSchema)
}
