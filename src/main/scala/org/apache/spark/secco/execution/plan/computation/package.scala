package org.apache.spark.secco.execution.plan

package object computation {
//  object PreparationTask extends Enumeration {
//    type PreparationTask = Value
//    val ConstructTrie, Sort, ConstructHashMap = Value
//  }

  sealed trait PreparationTask

  object PreparationTask {
    case object ConstructTrie extends PreparationTask
    case object Sort extends PreparationTask
    case class ConstructHashMap(keyAttr: Seq[String]) extends PreparationTask
  }

}
