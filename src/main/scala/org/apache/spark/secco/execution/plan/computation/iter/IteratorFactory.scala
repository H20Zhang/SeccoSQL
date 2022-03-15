//package org.apache.spark.secco.execution.plan.computation.iter
//
//import org.apache.spark.secco.execution.OldInternalRow
//import org.apache.spark.secco.execution.plan.computation.utils.{
//  ConsecutiveRowArray,
//  InternalRowHashMap,
//  Trie
//}
//import org.apache.spark.secco.execution.plan.support.FuncGenSupport
//
//import scala.collection.mutable.ArrayBuffer
//
///** Factory for instantiating [[SeccoIterator]].
//  */
//object IteratorFactory extends FuncGenSupport {
//
//  //warning: length of prefix must be smaller than length of localAttributeOrder
//  /** Return an iterator that performs selection predicates on [[OldInternalRow]] produced by child iterator.
//    *
//    * @param childIter           child iterator that produce [[OldInternalRow]]
//    * @param selectionExprs      selection predicates, which is of form ("a", "<", "b")
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  def makeSelectIter(
//      childIter: SeccoIterator,
//      selectionExprs: Seq[(String, String, String)],
//      localAttributeOrder: Array[String]
//  ): SeccoIterator = {
//    val selectionExecFunc =
//      genSelectionFunc(selectionExprs, localAttributeOrder)
//
//    SelectIter(childIter, selectionExecFunc, localAttributeOrder)
//  }
//
//  //warning: length of prefix must be smaller than or equal to length of common attribute of iter and its childIter
//  /** Return an iterator that performs projection predicates on [[OldInternalRow]] produced by child iterator.
//    *
//    * @param childIter           child iterator that produce [[OldInternalRow]]
//    * @param projectionList      projection list, which is of form ("a", "b", "c").
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  def makeProjectIter(
//      childIter: SeccoIterator,
//      projectionList: Seq[String],
//      localAttributeOrder: Array[String]
//  ): SeccoIterator = {
//
//    val childAttributeOrder = childIter.localAttributeOrder
//
//    if (childAttributeOrder.startsWith(localAttributeOrder)) {
//      MatchedPrefixProjectIter(
//        childIter,
//        localAttributeOrder.filter(projectionList.contains),
//        localAttributeOrder
//      )
//    } else if (childAttributeOrder(0) == localAttributeOrder(0)) {
//      PartiallyMatchedPrefixProjectIter(
//        childIter,
//        localAttributeOrder.filter(projectionList.contains),
//        localAttributeOrder
//      )
//    } else {
//      NoMatchedPrefixProjectIter(
//        childIter,
//        localAttributeOrder.filter(projectionList.contains),
//        localAttributeOrder
//      )
//    }
//  }
//
//  //warning: length of prefix must be smaller than or equal to length of common attribute of iter and its childIter
//  /** Return an iterator that performs aggregation on [[OldInternalRow]] produced by child iterator.
//    *
//    * @param childIter           child iterator that produce [[OldInternalRow]]
//    * @param groupingList        grouping list, which is of form ("a", "b", "c").
//    * @param semiringList        semiring operators, which is of form ("sum", "a").
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  def makeAggregateIter(
//      childIter: SeccoIterator,
//      groupingList: Seq[String],
//      semiringList: (String, String),
//      localAttributeOrder: Array[String]
//  ): SemiringAggregateIter = {
//
//    val childAttributeOrder = childIter.localAttributeOrder
//    val orderedGroupingList = localAttributeOrder.filter(groupingList.contains)
//
//    //TODO: add some assert to ensure attributes in groupingList are on the front of localAttributeOrder
//    if (childAttributeOrder.startsWith(orderedGroupingList)) {
//      MatchedPrefixSemiringAggregateIter(
//        childIter,
//        localAttributeOrder.filter(groupingList.contains),
//        semiringList,
//        localAttributeOrder
//      )
//    } else if (childAttributeOrder(0) == orderedGroupingList(0)) {
//      PartiallyMatchedPrefixSemiringAggregateIter(
//        childIter,
//        localAttributeOrder.filter(groupingList.contains),
//        semiringList,
//        localAttributeOrder
//      )
//    } else {
//      NoMatchedPrefixSemiringAggregateIter(
//        childIter,
//        localAttributeOrder.filter(groupingList.contains),
//        semiringList,
//        localAttributeOrder
//      )
//    }
//  }
//
//  /** Return an iterator that contains the results of cartesian product between baseIt and indexIt.
//    *
//    * @param baseIt base iterator
//    * @param indexIt index iterator
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  def makeCartesianProductIter(
//      baseIt: SeccoIterator,
//      indexIt: SeccoIterator,
//      localAttributeOrder: Array[String]
//  ): CartesianProductIter =
//    CartesianProductIter(baseIt, indexIt, localAttributeOrder)
//
//  //warning: length of prefix must be smaller than or equal to length of common attribute of iter and its baseIt
//  /** Return an iterator that contains the results of join between baseIt and indexIt.
//    *
//    * @param baseIt base iterator
//    * @param indexIt index iterator
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  def makeBinaryJoinIter(
//      baseIt: SeccoIterator,
//      indexIt: SeccoIterator,
//      localAttributeOrder: Array[String]
//  ): BinaryJoinIter = BinaryJoinIter(baseIt, indexIt, localAttributeOrder)
//
//  //warning: length of prefix must be smaller than length of localAttributeOrder
//  /** Return an iterator that contains the join results between tables represented by tries.
//    *
//    * @param tries tries of the tables
//    * @param schemas schemas of the tables
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  //TODO: this method should accept children iterators rather than tries.
//  def makeLeapFrogJoinIter(
//      tries: Array[Trie],
//      schemas: Seq[Seq[String]],
//      localAttributeOrder: Array[String]
//  ): GHDJoinIter = GHDJoinIter(tries, schemas, localAttributeOrder)
//
//  //warning: length of prefix must be smaller than length of localAttributeOrder
//  /** Return an iterator that contains the pre-built trie for the table
//    *
//    * @param trie tries of the table
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  def makeTrieTableIter(
//      trie: Trie,
//      localAttributeOrder: Array[String]
//  ): TrieTableIter =
//    TrieTableIter(trie, localAttributeOrder)
//
//  /** Return an iterator that contains the pre-built hashmap for the table
//    *
//    * @param hashMap HashMap of the table
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  def makeHashMapTableIter(
//      hashMap: InternalRowHashMap,
//      localAttributeOrder: Array[String]
//  ): HashMapTableIter = HashMapTableIter(hashMap, localAttributeOrder)
//
//  //warning: length of prefix must be smaller than length of localAttributeOrder
//  /** Return an iterator that contains the [[OldInternalRow]] of the table
//    *
//    * @param array [[OldInternalRow]] of the table stored in [[Array]]
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  def makeArrayTableIter(
//      array: Array[OldInternalRow],
//      localAttributeOrder: Array[String]
//  ): ArrayTableIter = ArrayTableIter(array, localAttributeOrder)
//
//  //warning: length of prefix must be smaller than length of localAttributeOrder
//  /** Return an iterator that contains the [[OldInternalRow]] of the table
//    *
//    * @param array [[OldInternalRow]] of the table stored in [[ConsecutiveRowArray]]
//    * @param localAttributeOrder local attribute order
//    * @return An iterator that returns [[OldInternalRow]]
//    */
//  def makeConsecutiveRowArrayTableIter(
//      array: ConsecutiveRowArray,
//      localAttributeOrder: Array[String]
//  ): ConsecutiveRowArrayTableIter =
//    ConsecutiveRowArrayTableIter(array, localAttributeOrder)
//
//}
//
///** The trait for internal iterator that returns InternalRow */
//trait SeccoIterator extends Iterator[OldInternalRow] {
//
//  //warning: this operation is unsafe, prefix must follows some constraints, which is explained in IteratorFactory
//  /** reset the iterator based on prefix */
//  def reset(prefix: OldInternalRow): SeccoIterator
//
//  /** local attribute order of this iterator's output */
//  def localAttributeOrder: Array[String]
//
//  /** result of this iterator */
//  def result(): Array[OldInternalRow] = {
//    val buffer = ArrayBuffer[OldInternalRow]()
//    while (hasNext) {
//      buffer += next().clone()
//    }
//    buffer.toArray
//  }
//}
//
///** A Iterator that stores just single [[OldInternalRow]] */
//class SingularIterator extends SeccoIterator {
//
//  private var _row: OldInternalRow = _
//  private var _isDefined: Boolean = false
//
//  override def hasNext: Boolean = _isDefined
//
//  override def next(): OldInternalRow = {
//    val res = _row
//    _isDefined = false
//    _row = null
//    res
//  }
//
//  override def reset(prefix: OldInternalRow): SeccoIterator = {
//    _isDefined = true
//    _row = prefix
//    this
//  }
//
//  override def localAttributeOrder: Array[String] = ???
//}
//
///** A Iterator that stores no [[OldInternalRow]] */
//class EmptyIterator extends SeccoIterator {
//  override def reset(prefix: OldInternalRow): SeccoIterator = ???
//
//  override def localAttributeOrder: Array[String] = ???
//
//  override def hasNext: Boolean = false
//
//  override def next(): OldInternalRow = ???
//}
