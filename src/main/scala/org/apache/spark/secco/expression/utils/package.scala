package org.apache.spark.secco.expression

package object utils {
  // lgh: used in join algorithm. TODO: confirm the correctness
  def attributeMatched(a: Attribute, b: Attribute): Boolean = a.name == b.name && a.dataType == b.dataType
}
