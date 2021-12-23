/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.secco.expression.codegen

import org.apache.spark.secco.codegen.{CodeAndComment, CodeGenerator, CodegenContext}
import org.apache.spark.secco.execution.storage.block.TrieInternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.types.DataType

abstract class BaseUnaryIteratorProducer {
  def getIterator(prefix: InternalRow, tries: Array[TrieInternalBlock]): java.util.Iterator[AnyRef]
}


/**
  * A code generator for building a LeapFrogUnaryIterator.
  */
object GenerateUnaryIterator extends CodeGenerator[(Seq[Attribute], Seq[Seq[Attribute]]), BaseUnaryIteratorProducer]
{

  override protected def create(in: (Seq[Attribute], Seq[Seq[Attribute]])): BaseUnaryIteratorProducer = {
    create(in._1, in._2)
  }

  override protected def canonicalize(in: (Seq[Attribute], Seq[Seq[Attribute]])): (Seq[Attribute], Seq[Seq[Attribute]])
  = in

  override protected def bind(in: (Seq[Attribute], Seq[Seq[Attribute]]), inputSchema: Seq[Attribute])
  : (Seq[Attribute], Seq[Seq[Attribute]]) = {
    in
  }

  def UnaryIteratorCode(dt: DataType, ctx: CodegenContext): String = {

    val jt = CodeGenerator.javaType(dt)
    val bt = CodeGenerator.boxedType(dt)

    val defineArrayFirstElementComparator =
      s"""
         |static class ArrayFirstElementComparator implements java.util.Comparator<$jt[]> {
         |
         |    @Override
         |    public int compare(Object l, Object r) {
         |        $jt[] o1 = ($jt[]) l;
         |        $jt[] o2 = ($jt[]) r;
         |        if (o1.length == 0 && o2.length == 0) return 0;
         |        else if (o1.length == 0) return -1;
         |        else if (o2.length == 0) return 1;
         |
         |        return ${ctx.genComp(dt, "o1[0]", "o2[0]")};
         |    }
         |
         |}
         |""".stripMargin

    val defineLeapFrogJoinUnaryIterator =
      s"""
         |static class LeapFrogUnaryIterator implements java.util.Iterator<$bt> {
         |
         |    private final $jt[][] childrenInArrays;
         |    private int numArrays;
         |
         |    private $bt valueCache;
         |    private boolean hasNextCache;
         |    private boolean cacheValid = false;
         |
         |    private final int[] currentCursors;
         |    private int childIdx = 0;
         |
         |
         |    LeapFrogUnaryIterator($jt[][] tries){
         |        childrenInArrays = tries;
         |        numArrays = tries.length;
         |        currentCursors = new int[numArrays];
         |        for($jt[] trie: tries){
         |            if (trie.length == 0) {
         |                hasNextCache = false;
         |                cacheValid = true;
         |                return;
         |            }
         |        }
         |        java.util.Arrays.sort(childrenInArrays, new ArrayFirstElementComparator());
         |    }
         |
         |    //  find the position i so that array[i] >= value and i is the minimal value
         |    //  noted: the input array should be sorted
         |    private int seek($jt[] array, $jt value, int _left) {
         |        int left = _left;
         |        int right = array.length;
         |
         |        while (right > left) {
         |            int mid = left + (right - left) / 2;
         |            $jt midVal = array[mid];
         |
         |            int comp = ${ctx.genComp(dt, "midVal", "value")};
         |
         |            if (comp == 0)
         |                return mid;
         |            else if (comp > 0)
         |                right = mid;
         |            else left = mid + 1;
         |        }
         |
         |        return right;
         |    }
         |
         |    @Override
         |    public boolean hasNext() {
         |        if (cacheValid) return hasNextCache;
         |        int prevIdx = Math.floorMod((childIdx - 1), numArrays);
         |        $jt curMax = childrenInArrays[prevIdx][currentCursors[prevIdx]];
         |        while (!cacheValid) {
         |            valueCache = childrenInArrays[childIdx][currentCursors[childIdx]];
         |
         |            if (valueCache.equals(curMax)) {
         |                hasNextCache = true;
         |                cacheValid = true;
         |            }
         |            else{
         |                $jt[] curArray = childrenInArrays[childIdx];
         |                int curPos = seek(curArray, curMax, currentCursors[childIdx]);
         |                currentCursors[childIdx] = curPos;
         |
         |                if (curPos == curArray.length){
         |                    hasNextCache = false;
         |                    cacheValid = true;
         |                } else {
         |                    curMax = curArray[curPos];
         |                    childIdx = (childIdx + 1) % numArrays;
         |                }
         |            }
         |        }
         |        return hasNextCache;
         |    }
         |
         |    @Override
         |    public $bt next() {
         |        if (!hasNext())
         |            throw new  java.util.NoSuchElementException("This iterator has been traversed.");
         |        else {
         |            cacheValid = false;
         |            currentCursors[childIdx] += 1;
         |            if (currentCursors[childIdx] == childrenInArrays[childIdx].length) {
         |                hasNextCache = false;
         |                cacheValid = true;
         |            }else {
         |                childIdx = (childIdx + 1) % numArrays;
         |            }
         |            return valueCache;
         |        }
         |    }
         |
         |    $defineArrayFirstElementComparator
         |}
         |""".stripMargin

    defineLeapFrogJoinUnaryIterator
  }

  def create(schema: Seq[Attribute], childrenSchemas: Seq[Seq[Attribute]]): BaseUnaryIteratorProducer = {
    val ctx = new CodegenContext

    val relevantRelationIndicesForEachAttr : Seq[Seq[Int]] =
      schema.indices.map { attrIdx =>
        val curAttr = schema(attrIdx)
        childrenSchemas.indices.filter { childIdx =>
          val idx = childrenSchemas(childIdx).map(_.name).indexOf(curAttr.name)
          idx > -1 && childrenSchemas(childIdx)(idx).dataType == curAttr.dataType
        }
      }

    def getPrefixIndices(attrIdx: Int, childIdx: Int): Seq[Int] =
      relevantRelationIndicesForEachAttr.slice(0, attrIdx).zipWithIndex.filter {
        case (childIndices, _) => childIndices.contains(childIdx)
      }.map(_._2)

    // Things that are known at code generation time:
    // 1. cur level
    // 2. prefix schema
    // 3. relevant relations for cur level
    // 4. relevant prefix attributes for each relevant relation

    val prefixLength = schema.size - 1
    val curLevel = prefixLength


    //    |    final DataType[] dataTypes = {${schema.slice(0, prefixLength).map (attr => {
    //                                     val boxedType = CodeGenerator.boxedType(attr.dataType)
    //                                     "DataTypes." + boxedType.substring(boxedType.lastIndexOf(".") + 1) + "Type"
    //                                     }).mkString(", ")}};

    val dt = schema(curLevel).dataType
    val jt = CodeGenerator.javaType(dt)
    val fullPt = CodeGenerator.primitiveTypeName(dt)
    val pt = fullPt.substring(fullPt.lastIndexOf(".") + 1)

    val prefixSchema = schema.slice(0, prefixLength)
    val curRelevantRelationIndices = relevantRelationIndicesForEachAttr(curLevel)
    val numRelevantRelations = curRelevantRelationIndices.length
    val prefixIndicesForEachChild = curRelevantRelationIndices.map(getPrefixIndices(curLevel, _))

    val defineLeapFrogJoinUnaryIterator: String = UnaryIteratorCode(dt, ctx)

    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificIteratorProducer();
         |}
         |
         |public class SpecificIteratorProducer extends ${classOf[BaseUnaryIteratorProducer].getName} {
         |
         |    final DataType[] dataTypes = ${prefixSchema.map("DataTypes." + _.dataType).mkString("{", ",", "}")};
         |    final int[] curRelevantRelationIndices = ${curRelevantRelationIndices.mkString("{", ",", "}")};
         |    final int[][] prefixIndicesForEachChild = ${prefixIndicesForEachChild.map(_.mkString(","))
                                                                                    .mkString("{{", "},{", "}}")};
         |    final $jt[][] childrenInArrays = new $jt[$numRelevantRelations][];
         |
         |    ${ctx.declareAddedFunctions()}
         |
         |    $defineLeapFrogJoinUnaryIterator
         |
         |    // at the code generation time, the schema, curRelevantRelationIndices are known.
         |    @Override
         |    public java.util.Iterator<java.lang.Object> getIterator(InternalRow prefix, TrieInternalBlock[] tries) {
         |        for (int i=0; i<$numRelevantRelations; i++) {
         |            int curChildIndex = curRelevantRelationIndices[i];
         |            int[] curPrefixIndices = prefixIndicesForEachChild[i];
         |            java.lang.Object[] curPrefix = new java.lang.Object[curPrefixIndices.length];
         |            for (int j=0; j<curPrefixIndices.length; j++){
         |                curPrefix[j] = prefix.get(curPrefixIndices[j], dataTypes[curPrefixIndices[j]]);
         |            }
         |            TrieInternalBlock curTrie = tries[curChildIndex];
         |            childrenInArrays[i] = curTrie.get$pt(InternalRow.apply(curPrefix));
         |        }
         |        return new LeapFrogUnaryIterator(childrenInArrays);
         |    }
         |}
         |""".stripMargin

    val code = CodeFormatter.stripOverlappingComments(new CodeAndComment(codeBody, Map.empty))
    logDebug(s"SpecificIteratorProducer():\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(Array.empty).asInstanceOf[BaseUnaryIteratorProducer]
  }
}


//lgh code fragments:

// 1.
//  def getIterator(prefix: InternalRow, tries: java.util.List[IndexableTableIterator]): SeccoIterator

