package unit.optimization.utils

import org.apache.spark.secco.analysis.UnresolvedAttribute
import org.apache.spark.secco.expression.{And, EqualTo}
import org.apache.spark.secco.optimization.util.EquiAttributes
import util.SeccoFunSuite

class EquiAttributesSuite extends SeccoFunSuite {

  override def setupDB(): Unit = {
    createDummyRelation("R1", "a", "b")()
    createDummyRelation("R2", "b", "c")()
    createDummyRelation("R3", "c", "d")()
    createDummyRelation("R4", "d", "e")()
  }

  test("basic") {

    val R1 = seccoSession.table("R1")
    val R2 = seccoSession.table("R2")
    val R3 = seccoSession.table("R3")
    val R4 = seccoSession.table("R4")

    val attrA = R1.logical.output(0)
    val attrB = R1.logical.output(1)
    val attrC = R3.logical.output(0)
    val attrD = R3.logical.output(1)
    val attrE = R4.logical.output(1)

    // Check fromCondition(expr).
    val expr = And(EqualTo(attrA, attrB), EqualTo(attrB, attrC))
    val equiAttrs = EquiAttributes.fromCondition(expr)

    assert(
      equiAttrs.attr2RepAttr == Map(
        attrA -> attrA,
        attrB -> attrA,
        attrC -> attrA
      )
    )
    assert(equiAttrs.repAttrs == Seq(attrA))
    assert(
      equiAttrs.repAttr2Attr.map(f => (f._1, f._2.toSeq)) == Map(
        attrA -> Seq(attrA, attrB, attrC)
      )
    )

    // Check fromCondition(attrs, expr).
    val equiAttrs1 =
      EquiAttributes.fromCondition(Seq(attrA, attrB, attrC, attrD, attrE), expr)

    assert(
      equiAttrs1.attr2RepAttr == Map(
        attrA -> attrA,
        attrB -> attrA,
        attrC -> attrA,
        attrD -> attrD,
        attrE -> attrE
      )
    )
    assert(equiAttrs1.repAttrs.toSet == Set(attrA, attrD, attrE))
    assert(
      equiAttrs1.repAttr2Attr.map(f => (f._1, f._2.toSet)) == Map(
        attrA -> Set(attrA, attrB, attrC),
        attrD -> Set(attrD),
        attrE -> Set(attrE)
      )
    )

    // Check merge.
    val expr2 = And(EqualTo(attrA, attrB), EqualTo(attrB, attrC))
    val expr3 = EqualTo(attrC, attrD)
    val equiAttrs2 = EquiAttributes.fromCondition(expr2)
    val equiAttrs3 = EquiAttributes.fromCondition(expr3)

    println(equiAttrs2)
    println(equiAttrs3)
    println(equiAttrs2.merge(equiAttrs3))

  }

}
