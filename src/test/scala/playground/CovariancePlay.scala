//package playground
//
//import org.scalatest.FunSuite
//
//class CovariancePlay extends FunSuite {
//
//  test("basic") {}
//}
//
////If A <: B then Node[A] <: Node[B]
//trait Node[+B] {
//
//  //define a new type U in case we use Node[A] as Node[B].
//  def prepend[U >: B](elem: U): Node[U]
//}
//
//case class ListNode[+B](h: B, t: Node[B]) extends Node[B] {
//  def prepend[U >: B](elem: U): Node[U] = ListNode(elem, this)
//  def head: B = h
//  def tail: Node[B] = t
//}
//
//case class Nil[+B]() extends Node[B] {
//  def prepend[U >: B](elem: U): Node[U] = ListNode(elem, this)
//}
