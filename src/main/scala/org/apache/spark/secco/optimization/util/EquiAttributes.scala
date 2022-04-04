package org.apache.spark.secco.optimization.util

import org.apache.spark.secco.expression.{
  Attribute,
  AttributeReference,
  AttributeSeq,
  EqualTo,
  Expression,
  PredicateHelper
}
import org.apache.spark.secco.expression.utils.{
  AttributeMap,
  AttributeSet,
  attributeMatched
}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** The class for storing the equivilance attributes in terms of EqualTo condition. */
class EquiAttributes(val attr2RepAttr: AttributeMap[Attribute]) {

  /** The representative attributes for each set of attributes related by EqualTo in `condition`. */
  lazy val repAttrs: Seq[Attribute] = attr2RepAttr.values.toSeq.distinct

  /** The map from representative attribute to attributes it represents. */
  lazy val repAttr2Attr: AttributeMap[Array[Attribute]] = {

    val repAttr2AttrBuilder =
      mutable.HashMap[Attribute, ArrayBuffer[Attribute]]()

    attr2RepAttr.foreach { case (attr, repAttr) =>
      val attrArray = repAttr2AttrBuilder.getOrElse(repAttr, ArrayBuffer())
      attrArray += attr
      repAttr2AttrBuilder(repAttr) = attrArray
    }

    AttributeMap(repAttr2AttrBuilder.map(f => (f._1, f._2.toArray)).toSeq)
  }

  /** Merge two [[EquiAttributes]] into a new one.
    *
    * Note: same attributes from two EquiAttributes will be merged into one.
    */
  def merge(other: EquiAttributes): EquiAttributes = {

    val otherAttr2RepAttr = other.attr2RepAttr

    // Find common attributes between the two.
    val commonAttrs = toAttributeSet().intersect(other.toAttributeSet())

    // Find unique attributes in each EquiAttributes.
    val leftCommonRepAttrs = AttributeSet(
      commonAttrs.map(attr => attr2RepAttr(attr)).toSeq
    )
    val rightCommonRepAttrs = AttributeSet(
      commonAttrs.map(attr => otherAttr2RepAttr(attr)).toSeq
    )

    val leftUniqueAttrs = repAttr2Attr
      .filter { repAttr =>
        !leftCommonRepAttrs.contains(repAttr._1)
      }
      .flatMap(_._2)

    val rightUniqueAttrs = other.repAttr2Attr
      .filter { repAttr =>
        !rightCommonRepAttrs.contains(repAttr._1)
      }
      .flatMap(_._2)

    // Construct new attr2RepAttr
    val leftAttr2RepAttr = leftUniqueAttrs.map { attr =>
      (attr, attr2RepAttr(attr))
    }.toSeq

    val rightAttr2RepAttr = rightUniqueAttrs.map { attr =>
      (attr, otherAttr2RepAttr(attr))
    }.toSeq

    val commonAttr2RepAttr = commonAttrs.flatMap { attr =>
      val repAttr = attr2RepAttr(attr)
      val otherRepAttr = otherAttr2RepAttr(attr)
      attr2RepAttr.filter(_._2 == repAttr) ++ otherAttr2RepAttr
        .filter(_._2 == otherRepAttr)
        .map(f => (f._1, repAttr))
    }.toSeq

    val newAttr2RepAttr =
      leftAttr2RepAttr ++ rightAttr2RepAttr ++ commonAttr2RepAttr

    new EquiAttributes(AttributeMap(newAttr2RepAttr))
  }

  /** Check if the equivalent attribute is empty */
  def isEmpty(): Boolean = attr2RepAttr.isEmpty

  /** Put equivalent attributes to an [[Seq]] */
  def toSeq(): Seq[Attribute] = attr2RepAttr.keys.toSeq

  /** Put equivalent attributes to an [[AttributeSeq]] */
  def toAttributeSeq(): AttributeSeq = AttributeSeq(attr2RepAttr.keys.toSeq)

  /** Put equivalent attributes to an [[AttributeSet]] */
  def toAttributeSet(): AttributeSet = AttributeSet(attr2RepAttr.keys)

  /** Put mapping of equivalent attribute tp representative attribute to an [[AttributeMap]] */
  def toAttributeMap(): AttributeMap[Attribute] = attr2RepAttr

  override def toString: String = {
    attr2RepAttr.toString()
  }

}

object EquiAttributes extends PredicateHelper {

  /** Build an naive equivalence attributes from given attributes */
  def fromAttributes(attrs: Seq[Attribute]): EquiAttributes = {
    val attr2RepAttr = AttributeMap(attrs.map(f => (f, f)))
    new EquiAttributes(attr2RepAttr)
  }

  /** Build the equivalence attributes from an expression. */
  def fromCondition(cond: Expression): EquiAttributes = fromConditions(
    Seq(cond)
  )

  /** Build the equivalence attributes of the given attributes from an expression.
    *
    * Note: for attributes only exists in cond, we just ignore them.
    *       for attribute only exists in attrs, we add an naive mapping for it,  e.g., attr -> attr.
    */
  def fromCondition(attrs: Seq[Attribute], cond: Expression): EquiAttributes =
    fromConditions(attrs, Seq(cond))

  /** Build the equivalence attributes from expressions. */
  def fromConditions(conds: Seq[Expression]): EquiAttributes = {

    // Find attributes involved in EqualTo conditions.
    val attrs = AttributeSet(conds.flatMap { expr =>
      splitConjunctivePredicates(expr).flatMap { subExpr =>
        subExpr match {
          case EqualTo(a: Attribute, b: Attribute) => Seq(a, b)
          case _                                   => Seq()
        }
      }
    }).toSeq

    fromConditions(attrs, conds)
  }

  /** Build the equivalence attributes of the given attributes from expressions.
    *
    * Note: For attributes only exists in cond, we just ignore them.
    *       For attribute only exists in attrs, we add an naive mapping for it,  e.g., attr -> attr.
    */
  def fromConditions(
      attrs: Seq[Attribute],
      condition: Seq[Expression]
  ): EquiAttributes = {
    // Construct the set of equivalence attributes in terms of EqualTo.
    val equivSet = {

      // Find out all equivalence relationship.
      val equiv = condition.flatMap(splitConjunctivePredicates).flatMap { f =>
        f match {
          case EqualTo(a: AttributeReference, b: AttributeReference) =>
            Seq((a, b))
          case _ => Seq()
        }
      }

      // Propagate the equivalence relationship.
      val equivSetArr = ArrayBuffer[AttributeSet]()
      equiv.foreach { case (a, b) =>
        var newEquivSet = AttributeSet(a :: b :: Nil)
        var oldEquivSetOpt: Option[AttributeSet] = None

        // Check if a or b are already contains in the equivSet. If so, expand equivSet by adding b and a.
        equivSetArr.foreach { equivSet =>
          if (equivSet.contains(a)) { // add to existing equivSet
            oldEquivSetOpt = Some(equivSet)
            newEquivSet = equivSet ++ AttributeSet(b)
          } else if (equivSet.contains(b)) { // add to existing equivSet
            oldEquivSetOpt = Some(equivSet)
            newEquivSet = equivSet ++ AttributeSet(a)
          }
        }

        // Remove old equivSet.
        oldEquivSetOpt.foreach(oldEquivSet =>
          equivSetArr.remove(equivSetArr.indexOf(oldEquivSet))
        )

        // Add new equivSet.
        equivSetArr += newEquivSet
      }

      equivSetArr
    }

    // Give each equivSet a representative element.
    val representativeAttributes = equivSet.map(_.head)

    val joinAttr2RepresentativeAttr =
      AttributeMap(representativeAttributes.zip(equivSet).flatMap {
        case (representativeAttr, equivSet) =>
          equivSet.toSeq.map(attr => (attr, representativeAttr))
      })

    val nonJoinAttr2RepresentativeAttr = attrs
      .filterNot(attr => joinAttr2RepresentativeAttr.contains(attr))
      .map(f => (f, f))

    val attr2RepresentativeAttr = AttributeMap(
      joinAttr2RepresentativeAttr.toSeq ++ nonJoinAttr2RepresentativeAttr
    )

    new EquiAttributes(attr2RepresentativeAttr)
  }
}

/** The class for storing the attribute orders.
  * @param equiAttrs the equivalence relation between attributes in terms of EqualTo.
  * @param order the attribute order.
  */
case class AttributeOrder(
    equiAttrs: EquiAttributes,
    order: Array[Attribute]
) {

  /** Find the attribute order for representative attributes. */
  lazy val repAttrOrder: AttributeOrder = {
    val repSet = equiAttrs.repAttrs.toSet
    copy(order = order.filter(attr => repSet.contains(attr)))
  }

  /** Find the relative attribute orders given an array of attributes. */
  def findRelativeAttributeOrder(attrs: Seq[Attribute]): AttributeOrder = {
    copy(order = order.filter(attr => attrs.contains(attr)))
  }

}
