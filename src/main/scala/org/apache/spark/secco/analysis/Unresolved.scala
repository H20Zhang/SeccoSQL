package org.apache.spark.secco.analysis

import org.apache.spark.secco.codegen.{CodegenContext, ExprCode}
import org.apache.spark.secco.expression.{Attribute, Star}
import org.apache.spark.secco.expression._
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.plan.{
  GraphRelation,
  LeafNode,
  UnaryNode
}
import org.apache.spark.secco.execution.storage.row
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.parsing.{
  BiDirection,
  EdgeDirection,
  Left2Right,
  Right2Left
}
import org.apache.spark.secco.trees.{TreeNode, TreeNodeException}
import org.apache.spark.secco.types.{DataType, IntegerType}

/** Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully
  * resolved.
  */
class UnresolvedException[TreeType <: TreeNode[_]](
    tree: TreeType,
    function: String
) extends TreeNodeException(
      tree,
      s"Invalid call to $function on unresolved object",
      null
    )

case class UnresolvedAttribute(nameParts: Seq[String]) extends Attribute {
  def name: String =
    nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType =
    throw new UnresolvedException(this, "dataType")
  override def nullable: Boolean =
    throw new UnresolvedException(this, "nullable")
  override def qualifier: Option[String] =
    throw new UnresolvedException(this, "qualifier")
  override lazy val resolved = false

  override def newInstance(): UnresolvedAttribute = this
  override def withNullability(newNullability: Boolean): UnresolvedAttribute =
    this
  override def withQualifier(
      newQualifier: Option[String]
  ): UnresolvedAttribute = this
  override def withName(newName: String): UnresolvedAttribute =
    UnresolvedAttribute(Seq(newName))

  override def toString: String = s"'$name"

  override def sql: String = s"`$name`"

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = ???

  /** Returns Java source code that can be compiled to evaluate this expression.
    * The default behavior is to call the eval method of the expression. Concrete expression
    * implementations should override this to do actual code generation.
    *
    * @param ctx a [[CodegenContext]]
    * @param ev  an [[ExprCode]] with unique terms， which is to be assigned value of the expression.
    * @return an [[ExprCode]] containing the Java source code to generate the given expression
    */
  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = ???

  override def withExprId(newExprId: ExprId): Attribute = this
}

case class UnresolvedAlias(
    child: Expression,
    aliasFunc: Option[Expression => String] = None
) extends UnaryExpression
    with NamedExpression
    with Unevaluable {
  override def toAttribute: Attribute =
    throw new UnresolvedException(this, "toAttribute")
  override def qualifier: Option[String] =
    throw new UnresolvedException(this, "qualifier")
  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def nullable: Boolean =
    throw new UnresolvedException(this, "nullable")
  override def dataType: DataType =
    throw new UnresolvedException(this, "dataType")
  override def name: String = throw new UnresolvedException(this, "name")
  override def newInstance(): NamedExpression =
    throw new UnresolvedException(this, "newInstance")

  override lazy val resolved = false
}

/** Represents all of the input attributes to a given relational operator, for example in
  * "SELECT * FROM ...".
  *
  * This is also used to expand structs. For example:
  * "SELECT record.* from (SELECT struct(a,b,c) as record ...)
  *
  * @param target an optional name that should be the target of the expansion.  If omitted all
  *              targets' columns are produced. This can either be a table name or struct name. This
  *              is a list of identifiers that is the path of the expansion.
  */
case class UnresolvedStar(target: Option[Seq[String]])
    extends Star
    with Unevaluable {

//  override def expand(input: LogicalPlan): Seq[NamedExpression] = ???
  override def expand(input: LogicalPlan): Seq[NamedExpression] = {
    // If there is no table specified, use all input attributes.
    if (target.isEmpty) return input.output

    val expandedAttributes =
      if (target.get.size == 1) {
        // If there is a table, pick out attributes that are part of this table.
        input.output.filter(_.qualifier.exists(_ == target.get.head))
      } else {
        List()
      }
    if (expandedAttributes.nonEmpty) {
      expandedAttributes
    } else {
      throw new Exception(s"resolution of Star: ${toString} failed")
    }

  }

}

case class UnresolvedFunction(
    name: String,
    children: Seq[Expression],
    isDistinct: Boolean
) extends Expression
    with Unevaluable {

  override def dataType: DataType =
    throw new UnresolvedException(this, "dataType")

//  // commented by lgh, "method foldable cannot override final member"
//  override def foldable: Boolean =
//    throw new UnresolvedException(this, "foldable")

  override def nullable: Boolean =
    throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def prettyName: String = s"`$name`"
  override def toString: String = s"'$name(${children.mkString(", ")})"
}

case class UnresolvedRelation(
    tableName: String,
    mode: ExecMode = ExecMode.Coupled
) extends LeafNode {}

case class UnresolvedEdge(
    id: Attribute,
    src: UnresolvedNode,
    dst: UnresolvedNode,
    edgeLabels: Seq[Literal],
    edgeCondition: Option[Expression],
    edgeDirection: EdgeDirection
) {
  override def toString: String = {

    val edge = edgeLabels.isEmpty match {
      case true  => s"[${id}]"
      case false => s"[${id}:${edgeLabels.mkString(":")}]"
    }

    edgeDirection match {
      case Left2Right  => s"${src}-${edge}->${dst}"
      case Right2Left  => s"${src}<-${edge}-${dst}"
      case BiDirection => s"${src}-${edge}-${dst}"
    }
  }
}

case class UnresolvedNode(
    id: Attribute,
    nodeLabels: Seq[Literal],
    nodeCondition: Option[Expression]
) {
  override def toString: String = {
    nodeLabels.isEmpty match {
      case true  => s"(${id})"
      case false => s"(${id}:${nodeLabels.mkString(":")})"
    }
  }
}

case class UnresolvedPattern(
    edges: Seq[UnresolvedEdge],
    projectionList: Seq[Attribute]
) extends Expression
    with Unevaluable {

  override def nullable: Boolean =
    throw new UnresolvedException(this, "nullable")

  override def dataType: DataType =
    throw new UnresolvedException(this, "dataType")

  override def children: Seq[Expression] = Nil
}

case class UnresolvedSubgraphQuery(
    graph: GraphRelation,
    pattern: UnresolvedPattern,
    mode: ExecMode = ExecMode.Coupled
) extends UnaryNode {
  override def child: LogicalPlan = graph
}
