package org.apache.spark.secco.execution

import org.apache.spark.secco.execution.plan.computation.utils.{
  ConsecutiveRowArray,
  InternalRowHashMap,
  Trie
}

abstract class InternalBlock extends Serializable {
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
  self: InternalBlock =>
  def index: Array[Int]
}

case class MultiTableIndexedBlock(
    output: Seq[String],
    index: Array[Int],
    subBlocks: Seq[InternalBlock]
) extends InternalBlock
    with IndexedBlockCapability {

  override def blockContents: Seq[InternalBlockContent] =
    subBlocks.flatMap(_.blockContents)

  override def isEmpty: Boolean = subBlocks.forall(_.isEmpty)
}

case class RowIndexedBlock(
    output: Seq[String],
    index: Array[Int],
    blockContent: RowBlockContent
) extends InternalBlock
    with IndexedBlockCapability {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class HashMapIndexedBlock(
    output: Seq[String],
    index: Array[Int],
    blockContent: HashMapBlockContent
) extends InternalBlock
    with IndexedBlockCapability {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class TrieIndexedBlock(
    output: Seq[String],
    index: Array[Int],
    blockContent: TrieBlockContent
) extends InternalBlock
    with IndexedBlockCapability {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  //TODO: implement more efficient version
  override def isEmpty: Boolean = blockContent.content.toInternalRows().isEmpty
}

case class ConsecutiveRowIndexedBlock(
    output: Seq[String],
    index: Array[Int],
    blockContent: ConsecitiveRowBlockContent
) extends InternalBlock
    with IndexedBlockCapability {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)
  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class HashMapBlock(
    output: Seq[String],
    blockContent: HashMapBlockContent
) extends InternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class TrieBlock(
    output: Seq[String],
    blockContent: TrieBlockContent
) extends InternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  //TODO: implement more efficient version
  override def isEmpty: Boolean = blockContent.content.toInternalRows().isEmpty
}

case class ConsecutiveRowBlock(
    output: Seq[String],
    blockContent: ConsecitiveRowBlockContent
) extends InternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)
  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class RowBlock(
    output: Seq[String],
    blockContent: RowBlockContent
) extends InternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = blockContent.content.isEmpty
}

case class MultiBlock(
    output: Seq[String],
    subBlocks: Seq[InternalBlock]
) extends InternalBlock {

  override def blockContents: Seq[InternalBlockContent] =
    subBlocks.flatMap(_.blockContents)

  override def isEmpty: Boolean = subBlocks.forall(_.isEmpty)
}

case class GeneralBlock[V](
    output: Seq[String],
    blockContent: GeneralBlockContent[V]
) extends InternalBlock {
  override def blockContents: Seq[InternalBlockContent] = Seq(blockContent)

  override def isEmpty: Boolean = {
    throw new Exception("isEmpty is not implemented for GeneralBlock")
  }
}
