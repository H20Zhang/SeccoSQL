package org.apache.spark.secco.expression

import org.apache.spark.secco.analysis.UnresolvedException
import org.apache.spark.secco.types.{DataType, LongType}

import java.util.{Objects, UUID}
import org.apache.spark.secco.codegen.{CodegenContext, ExprCode}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.utils.AttributeSet
import org.apache.spark.secco.optimization.LogicalPlan

object NamedExpression {
  private val curId = new java.util.concurrent.atomic.AtomicLong()
  val jvmId = UUID.randomUUID()
  def newExprId: ExprId = ExprId(curId.getAndIncrement(), jvmId)
  def unapply(expr: NamedExpression): Option[(String, DataType)] =
    Some(expr.name, expr.dataType)
}

/** A globally unique id for a given named expression.
  * Used to identify which attribute output by a relation is being
  * referenced in a subsequent computation.
  *
  * The `id` field is unique within a given JVM, while the `uuid` is used to uniquely identify JVMs.
  */
case class ExprId(id: Long, jvmId: UUID)

object ExprId {
  def apply(id: Long): ExprId = ExprId(id, NamedExpression.jvmId)
}

/** An [[Expression]] that is named.
  */
trait NamedExpression extends Expression {

  /** We should never fold named expressions in order to not remove the alias. */
  override def foldable: Boolean = false

  def name: String
  def exprId: ExprId

  /** Returns a dot separated fully qualified name for this attribute.  Given that there can be
    * multiple qualifiers, it is possible that there are other possible way to refer to this
    * attribute.
    */
  def qualifiedName: String = (qualifier.toSeq :+ name).mkString(".")

  /** Optional qualifier for the expression.
    *
    * For now, since we do not allow using original table name to qualify a column name once the
    * table is aliased, this can only be:
    *
    * 1. Empty Seq: when an attribute doesn't have a qualifier,
    *    e.g. top level attributes aliased in the SELECT clause, or column from a LocalRelation.
    * 2. Single element: either the table name or the alias name of the table.
    */
  def qualifier: Option[String]

  def toAttribute: Attribute

  //  /** Returns the metadata when an expression is a reference to another expression with metadata. */
  //  def metadata: Metadata = Metadata.empty

  /** Returns true if the expression is generated by Catalyst */
  def isGenerated: java.lang.Boolean = false

  /** Returns a copy of this expression with a new `exprId`. */
  def newInstance(): NamedExpression

  protected def typeSuffix =
    if (resolved) {
      dataType match {
        case LongType => "L"
        case _        => ""
      }
    } else {
      ""
    }
}

/** Used to assign a new name to a computation.
  * For example the SQL expression "1 + 1 AS a" could be represented as follows:
  *  Alias(Add(Literal(1), Literal(1)), "a")()
  *
  * Note that exprId and qualifiers are in a separate parameter list because
  * we only pattern match on child and name.
  *
  * @param child The computation being performed
  * @param name The name to be associated with the result of computing [[child]].
  * @param exprId A globally unique id used to check if an [[AttributeReference]] refers to this
  *               alias. Auto-assigned if left blank.
  * @param qualifier An optional string that can be used to referred to this attribute in a fully
  *                   qualified way. Consider the examples tableName.name, subQueryAlias.name.
  *                   tableName and subQueryAlias are possible qualifiers.
  */
case class Alias(child: Expression, name: String)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Option[String] = None
) extends UnaryExpression
    with NamedExpression {

  override def dataType: DataType = child.dataType
  override def nullable: Boolean = child.nullable

  def newInstance(): NamedExpression =
    Alias(child, name)(qualifier = qualifier)

  override def toAttribute: Attribute = {
    if (resolved) {
      AttributeReference(name, child.dataType, child.nullable)(
        exprId,
        qualifier
      )
    } else {
      UnresolvedAttribute(Seq(name))
    }
  }

  override def toString: String = s"$child AS $name#${exprId.id}$typeSuffix"

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: Nil
  }

  override def hashCode(): Int = {
    val state = Seq(name, exprId, child, qualifier)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(other: Any): Boolean =
    other match {
      case a: Alias =>
        name == a.name && exprId == a.exprId && child == a.child && qualifier == a.qualifier
      case _ => false
    }

  override def sql: String = {
    val qualifierPrefix = qualifier.map(_ + ".").getOrElse("")
    s"${child.sql} AS $qualifierPrefix`${name}`"
  }

  override def eval(input: InternalRow): Any = child.eval(input)

  // just a through for code generation.
  override def genCode(ctx: CodegenContext): ExprCode = child.genCode(ctx)

  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = {
    throw new Exception("Alias.doGenCode should not be called")
  }
}

abstract class Attribute extends LeafExpression with NamedExpression {
  override def references: AttributeSet = AttributeSet(Set(this))

  def withNullability(newNullability: Boolean): Attribute
  def withQualifier(newQualifier: Option[String]): Attribute
  def withName(newName: String): Attribute

  override def toAttribute: Attribute = this
  def newInstance(): Attribute
}

/** A reference to an attribute produced by another operator in the tree.
  *
  * @param name The name of this attribute, should only be used during analysis or for debugging.
  * @param dataType The [[DataType]] of this attribute.
  * @param nullable True if null is a valid value for this attribute.
  * @param exprId A globally unique id used to check if different AttributeReferences refer to the
  *               same attribute.
  * @param qualifier An optional string that can be used to referred to this attribute in a fully
  *                  qualified way. Consider the examples tableName.name, subQueryAlias.name.
  *                  tableName and subQueryAlias are possible qualifiers.
  */
case class AttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true
)(
    val exprId: ExprId = NamedExpression.newExprId,
    val qualifier: Option[String] = None
) extends Attribute
    with Unevaluable {

  /** Returns true iff the expression id is the same for both attributes.
    */
  def sameRef(other: AttributeReference): Boolean = this.exprId == other.exprId

  override def equals(other: Any): Boolean =
    other match {
      case ar: AttributeReference =>
        name == ar.name && dataType == ar.dataType && nullable == ar.nullable && exprId == ar.exprId && qualifier == ar.qualifier
      case _ => false
    }

  override def hashCode: Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    var h = 17
    h = h * 37 + name.hashCode()
    h = h * 37 + dataType.hashCode()
    h = h * 37 + nullable.hashCode()
    h = h * 37 + exprId.hashCode()
    h = h * 37 + qualifier.hashCode()
    h
  }

  override def newInstance(): AttributeReference =
    AttributeReference(name, dataType, nullable)(
      qualifier = qualifier
    )

  /** Returns a copy of this [[AttributeReference]] with changed nullability.
    */
  override def withNullability(newNullability: Boolean): AttributeReference = {
    if (nullable == newNullability) {
      this
    } else {
      AttributeReference(name, dataType, newNullability)(
        exprId,
        qualifier
      )
    }
  }

  override def withName(newName: String): AttributeReference = {
    if (name == newName) {
      this
    } else {
      AttributeReference(newName, dataType, nullable)(
        exprId,
        qualifier
      )
    }
  }

  /** Returns a copy of this [[AttributeReference]] with new qualifier.
    */
  override def withQualifier(
      newQualifier: Option[String]
  ): AttributeReference = {
    if (newQualifier == qualifier) {
      this
    } else {
      AttributeReference(name, dataType, nullable)(
        exprId,
        newQualifier
      )
    }
  }

  def withExprId(newExprId: ExprId): AttributeReference = {
    if (exprId == newExprId) {
      this
    } else {
      AttributeReference(name, dataType, nullable)(
        newExprId,
        qualifier
      )
    }
  }

  override protected final def otherCopyArgs: Seq[AnyRef] = {
    exprId :: qualifier :: isGenerated :: Nil
  }

  override def toString: String = s"$name#${exprId.id}$typeSuffix"

  // Since the expression id is not in the first constructor it is missing from the default
  // tree string.
  override def simpleString: String =
    s"$name#${exprId.id}: ${dataType.simpleString}"

  override def sql: String = {
    val qualifierPrefix = qualifier.map(_ + ".").getOrElse("")
    s"$qualifierPrefix`${name}`"
  }
}

case class UnresolvedAttribute(nameParts: Seq[String]) extends Attribute {
  def name: String =
    nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  override def exprId: ExprId = ???
  override def dataType: DataType = ???
  override def nullable: Boolean = ???
  override def qualifier: Option[String] = ???
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

  override def eval(input: InternalRow): Any = ???

  /** Returns Java source code that can be compiled to evaluate this expression.
    * The default behavior is to call the eval method of the expression. Concrete expression
    * implementations should override this to do actual code generation.
    *
    * @param ctx a [[CodegenContext]]
    * @param ev  an [[ExprCode]] with unique terms.
    * @return an [[ExprCode]] containing the Java source code to generate the given expression
    */
  override protected def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode
  ): ExprCode = ???
}

abstract class Star extends LeafExpression with NamedExpression {
  override def name: String = throw new UnresolvedException(this, "name")
  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType =
    throw new UnresolvedException(this, "dataType")
  override def nullable: Boolean =
    throw new UnresolvedException(this, "nullable")
  override def qualifier: Option[String] =
    throw new UnresolvedException(this, "qualifier")
  override def toAttribute: Attribute =
    throw new UnresolvedException(this, "toAttribute")
  override def newInstance(): NamedExpression =
    throw new UnresolvedException(this, "newInstance")
  override lazy val resolved = false

  def expand(input: LogicalPlan): Seq[NamedExpression]
}

/** Represents all the resolved input attributes to a given relational operator. This is used
  * in the data frame DSL.
  *
  * @param expressions Expressions to expand.
  */
case class ResolvedStar(expressions: Seq[NamedExpression])
    extends Star
    with Unevaluable {
  override def newInstance(): NamedExpression =
    throw new UnresolvedException(this, "newInstance")
  override def toString: String =
    expressions.mkString("ResolvedStar(", ", ", ")")

  override def expand(input: LogicalPlan): Seq[NamedExpression] = expressions
}
