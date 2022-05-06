package org.apache.spark.secco.expression.utils

import org.apache.spark.secco.expression.{Canonicalize, Expression}

import scala.collection.{GenTraversableOnce, mutable}
import scala.collection.mutable.ArrayBuffer

object ExpressionSet {

  /** Constructs a new [[ExpressionSet]] by applying [[Canonicalize]] to `expressions`. */
  def apply(expressions: TraversableOnce[Expression]): ExpressionSet = {
    val set = new ExpressionSet()
    expressions.foreach(set.add)
    set
  }
}

/** A [[Set]] where membership is determined based on determinacy and a canonical representation of
  * an [[Expression]] (i.e. one that attempts to ignore cosmetic differences).
  * See [[Canonicalize]] for more details.
  *
  * Internally this set uses the canonical representation, but keeps also track of the original
  * expressions to ease debugging.  Since different expressions can share the same canonical
  * representation, this means that operations that extract expressions from this set are only
  * guaranteed to see at least one such expression.  For example:
  *
  * {{{
  *   val set = ExpressionSet(a + 1, 1 + a)
  *
  *   set.iterator => Iterator(a + 1)
  *   set.contains(a + 1) => true
  *   set.contains(1 + a) => true
  *   set.contains(a + 2) => false
  * }}}
  *
  * For non-deterministic expressions, they are always considered as not contained in the [[Set]].
  * On adding a non-deterministic expression, simply append it to the original expressions.
  * This is consistent with how we define `semanticEquals` between two expressions.
  */
class ExpressionSet protected (
    protected val baseSet: mutable.Set[Expression] = new mutable.HashSet,
    protected val originals: mutable.Buffer[Expression] = new ArrayBuffer
) extends Set[Expression] {

  protected def add(e: Expression): Unit = {
    if (!e.deterministic) {
      originals += e
    } else if (!baseSet.contains(e.canonicalized)) {
      baseSet.add(e.canonicalized)
      originals += e
    }
  }

  override def contains(elem: Expression): Boolean =
    baseSet.contains(elem.canonicalized)

  override def +(elem: Expression): ExpressionSet = {
    val newSet = new ExpressionSet(baseSet.clone(), originals.clone())
    newSet.add(elem)
    newSet
  }

  override def ++(elems: GenTraversableOnce[Expression]): ExpressionSet = {
    val newSet = new ExpressionSet(baseSet.clone(), originals.clone())
    elems.foreach(newSet.add)
    newSet
  }

  override def -(elem: Expression): ExpressionSet = {
    if (elem.deterministic) {
      val newBaseSet = baseSet.clone().filterNot(_ == elem.canonicalized)
      val newOriginals =
        originals.clone().filterNot(_.canonicalized == elem.canonicalized)
      new ExpressionSet(newBaseSet, newOriginals)
    } else {
      new ExpressionSet(baseSet.clone(), originals.clone())
    }
  }

  override def iterator: Iterator[Expression] = originals.iterator

  /** Returns a string containing both the post [[Canonicalize]] expressions and the original
    * expressions in this set.
    */
  def toDebugString: String =
    s"""
       |baseSet: ${baseSet.mkString(", ")}
       |originals: ${originals.mkString(", ")}
     """.stripMargin
}
