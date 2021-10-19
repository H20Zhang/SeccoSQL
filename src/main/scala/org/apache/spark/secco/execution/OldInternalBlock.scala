package org.apache.spark.secco.execution

import org.apache.spark.secco.execution.plan.computation.utils.{
  ConsecutiveRowArray,
  InternalRowHashMap,
  Trie
}

abstract class OldInternalBlock extends Serializable {
  def output: Seq[String]
  def blockContents: Seq[InternalBlockContent]
  def isEmpty: Boolean
}

/** Block Contents */
trait InternalBlockContent {}
case class RowBlockContent(content: Array[OldInternalRow])
    extends InternalBlockContent
case class TrieBlockContent(content: Trie) extends InternalBlockContent
case class HashMapBlockContent(content: InternalRowHashMap)
    extends InternalBlockContent
case class ConsecitiveRowBlockContent(content: ConsecutiveRowArray)
    extends InternalBlockContent
case class GeneralBlockContent[V](content: V) extends InternalBlockContent

trait IndexedBlockCapability {
  self: OldInternalBlock =>
  def index: Array[Int]
}

case class MultiTableIndexedBlockOld(
    output: Seq[String],
    index: Array[Int],
    subBlocks: Seq[OldInternalBlock]
) extends OldInternalBlock
    with IndexedBlockCapability {

  override def blockContents: Seq[InternalBlockContent] =
    subBlocks.flatMap(_.blockContents)

  override def isEmpty: Boolean = subBlocks.forall(_.isEmpty)
}

case class RowIndexedBlockOld(
    output: Seq[String],
    index: Array[Int],
    blockContent: RowBlockContent
) extends OldInternalBlock
    with IndexedBlockCapability {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class HashMapIndexedBlockOld(
    output: Seq[String],
    index: Array[Int],
    blockContent: HashMapBlockContent
) extends OldInternalBlock
    with IndexedBlockCapability {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class TrieIndexedBlockOld(
    output: Seq[String],
    index: Array[Int],
    blockContent: TrieBlockContent
) extends OldInternalBlock
    with IndexedBlockCapability {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  //TODO: implement more efficient version
  override def isEmpty: Boolean = blockContent.content.toInternalRows().isEmpty
}

case class ConsecutiveRowIndexedBlockOld(
    output: Seq[String],
    index: Array[Int],
    blockContent: ConsecitiveRowBlockContent
) extends OldInternalBlock
    with IndexedBlockCapability {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)
  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class HashMapBlockOld(
    output: Seq[String],
    blockContent: HashMapBlockContent
) extends OldInternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class TrieBlockOld(
    output: Seq[String],
    blockContent: TrieBlockContent
) extends OldInternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  //TODO: implement more efficient version
  override def isEmpty: Boolean = blockContent.content.toInternalRows().isEmpty
}

case class ConsecutiveRowBlockOld(
    output: Seq[String],
    blockContent: ConsecitiveRowBlockContent
) extends OldInternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)
  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class RowBlockOld(
    output: Seq[String],
    blockContent: RowBlockContent
) extends OldInternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class MultiBlockOld(
    output: Seq[String],
    subBlocks: Seq[OldInternalBlock]
) extends OldInternalBlock {

  override def blockContents: Seq[InternalBlockContent] =
    subBlocks.flatMap(_.blockContents)

  override def isEmpty: Boolean = subBlocks.forall(_.isEmpty)
}

case class GeneralBlockOld[V](
    output: Seq[String],
    blockContent: GeneralBlockContent[V]
) extends OldInternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = {
    throw new Exception("isEmpty is not implemented for GeneralBlock")
  }
}
