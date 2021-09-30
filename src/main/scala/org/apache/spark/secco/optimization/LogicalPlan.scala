package org.apache.spark.secco.optimization

import org.apache.spark.secco.analysis.UnresolvedAttribute
import org.apache.spark.secco.expression.{
  Attribute,
  Expression,
  NamedExpression
}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.plan.LeafNode
import org.apache.spark.secco.optimization.support.{
  CostModelSupport,
  LogicalPlanStatsSupport,
  SessionSupport
}
import org.apache.spark.secco.trees.QueryPlan
import org.apache.spark.internal.Logging

/** An abstract logical plan class
  */
abstract class LogicalPlan
    extends QueryPlan[LogicalPlan]
    with Logging
    with LogicalPlanStatsSupport
    with CostModelSupport {

  /** The execution mode of this operator, which can be either
    * 1. Coupled: the operator performs both communication and computation, and can be decoupled
    * 2. Computation: the operator performs only computation
    * 3. Communication: the operator performs only communication
    * 4. Atomic: the operator performs both communication and computation, and cannot be decoupled
    */
  def mode: ExecMode

  /** The primary key of the output, using Seq() to denote no output key exists */
  @deprecated
  def primaryKeyOld: Seq[String] = {
    throw new Exception(
      "primaryKeyOld is deprecated, please use primaryKey instead."
    )
  }

  /** The primary key of the output, using Seq() to denote no output key exists */
  def primaryKey: Seq[Attribute] = Seq()

  /** The attributes that do not existed in database but are generated in operator */
  @deprecated
  def producedOutputOld: Seq[String] = {
    throw new Exception(
      "producedOutputOld is deprecated, please stop using it."
    )
  }

  /** Returns true if this node and its children have already been gone through analysis and
    * verification.  Note that this is only an optimization used to avoid analyzing trees that
    * have already been analyzed, and can be reset by transformations.
    */
  var analyzed: Boolean = false

  /** Returns true if this expression and all its children have been resolved to a specific schema
    * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
    * can override this (e.g.
    * [[org.apache.spark.secco.analysis.UnresolvedRelation UnresolvedRelation]]
    * should return `false`).
    */
  lazy val resolved: Boolean =
    expressions.forall(_.resolved) && childrenResolved

  /** Returns true if all its children of this query plan have been resolved.
    */
  def childrenResolved: Boolean = children.forall(_.resolved)

  /** Returns a copy of this node where `rule` has been recursively applied first to all of its
    * children and then itself (post-order). When `rule` does not apply to a given node, it is left
    * unchanged.  This function is similar to `transformUp`, but skips sub-trees that have already
    * been marked as analyzed.
    *
    * @param rule the function use to transform this nodes children
    */
  def resolveOperators(
      rule: PartialFunction[LogicalPlan, LogicalPlan]
  ): LogicalPlan = {
    if (!analyzed) {
      val afterRuleOnChildren = mapChildren(_.resolveOperators(rule))
      if (this fastEquals afterRuleOnChildren) {
        rule.applyOrElse(this, identity[LogicalPlan])
      } else {
        rule.applyOrElse(afterRuleOnChildren, identity[LogicalPlan])
      }
    } else {
      this
    }
  }

  /** Recursively transforms the expressions of a tree, skipping nodes that have already
    * been analyzed.
    */
  def resolveExpressions(
      r: PartialFunction[Expression, Expression]
  ): LogicalPlan = {
    this resolveOperators { case p =>
      p.transformExpressions(r)
    }
  }

  /** Optionally resolves the given strings to a [[NamedExpression]] using the input from all child
    * nodes of this LogicalPlan. The attribute is expressed as
    * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
    */
  def resolveAttributeByChildren(
      nameParts: Seq[String]
  ): Option[NamedExpression] =
    resolveAttribute(nameParts, children.flatMap(_.output))

  /** Optionally resolves the given strings to a [[NamedExpression]] based on the output of this
    * LogicalPlan. The attribute is expressed as string in the following form:
    * `[scope].AttributeName.[nested].[fields]...`.
    */
  def resolveAttributeBySelf(nameParts: Seq[String]): Option[NamedExpression] =
    resolveAttribute(nameParts, output)

  /** Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
    *
    * This assumes `name` has multiple parts, where the 1st part is a qualifier
    * (i.e. table name, alias, or subquery alias).
    * See the comment above `candidates` variable in resolve() for semantics the returned data.
    */
  private def resolveNameAsTableColumn(
      nameParts: Seq[String],
      attribute: Attribute
  ): Option[(Attribute, List[String])] = {
    assert(nameParts.length > 1)
    if (attribute.qualifier.exists(_ == nameParts.head)) {
      // At least one qualifier matches. See if remaining parts match.
      val remainingParts = nameParts.tail
      resolveNameAsColumn(remainingParts, attribute)
    } else {
      None
    }
  }

  /** Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
    *
    * Different from resolveNameAsTableColumn, this assumes `name` does NOT start with a qualifier.
    * See the comment above `candidates` variable in resolve() for semantics the returned data.
    */
  private def resolveNameAsColumn(
      nameParts: Seq[String],
      attribute: Attribute
  ): Option[(Attribute, List[String])] = {
    if (!attribute.isGenerated && attribute.name == nameParts.head) {
      Option((attribute.withName(nameParts.head), nameParts.tail.toList))
    } else {
      None
    }
  }

  /** Performs attribute resolution given a name and a sequence of possible attributes. */
  protected def resolveAttribute(
      nameParts: Seq[String],
      input: Seq[Attribute]
  ): Option[NamedExpression] = {

    // A sequence of possible candidate matches.
    // Each candidate is a tuple. The first element is a resolved attribute, followed by a list
    // of parts that are to be resolved.
    // For example, consider an example where "a" is the table name, "b" is the column name,
    // and "c" is the struct field name, i.e. "a.b.c". In this case, Attribute will be "a.b",
    // and the second element will be List("c").
    var candidates: Seq[(Attribute, List[String])] = {
      // If the name has 2 or more parts, try to resolve it as `table.column` first.
      if (nameParts.length > 1) {
        input.flatMap { option =>
          resolveNameAsTableColumn(nameParts, option)
        }
      } else {
        Seq.empty
      }
    }

    // If none of attributes match `table.column` pattern, we try to resolve it as a column.
    if (candidates.isEmpty) {
      candidates = input.flatMap { candidate =>
        resolveNameAsColumn(nameParts, candidate)
      }
    }

    def name = UnresolvedAttribute(nameParts).name

    candidates.distinct match {
      // One match, no nested fields, use it.
      case Seq((a, Nil)) => Some(a)

      // No matches.
      case Seq() =>
        logTrace(s"Could not find $name in ${input.mkString(", ")}")
        None

      // More than one match.
      case ambiguousReferences =>
        val referenceNames = ambiguousReferences.map(_._1).mkString(", ")
        throw new Exception(
          s"Reference '$name' is ambiguous, could be: $referenceNames."
        )
    }
  }

  override def verboseString: String =
    simpleString + s"-> (${outputOld.mkString(",")})"

  /** The relational symbol of the operator */
  def relationalSymbol = nodeName

  /** The Relational Algebra representation for this operator and its children */
  def relationalString: String = {
    this match {
      case node: LeafNode => s"$relationalSymbol"
      case _ =>
        s"$relationalSymbol(${children.map(_.relationalString).mkString(",")})"
    }
  }
}
