package org.apache.spark.secco.expression.codegen;

import org.apache.spark.secco.execution.storage.block.TrieInternalBlock;
import org.apache.spark.secco.execution.storage.row.InternalRow;
import org.apache.spark.secco.expression.Attribute;
import org.apache.spark.secco.types.DataType;
import org.apache.spark.secco.types.StructField;

import java.util.ArrayList;

import static org.apache.spark.secco.types.DataTypes.DoubleType;
import static org.apache.spark.secco.types.DataTypes.StringType;

public class SpecificUnaryIteratorProducer extends BaseUnaryIteratorProducer {

    private java.util.List<Attribute> schema = new ArrayList<Attribute>(4);

//    int[] getPrefixIndices(int attrIdx, int childIdx){
//        relevantRelationIndicesForEachAttr.slice(0, attrIdx).zipWithIndex.filter {
//            case (childIndices, _) => childIndices.contains(childIdx)
//        }.map(_._2)
//        return new int[]{1, 3, 4};
//    }
    int[][] relevantRelationIndicesForEachAttr = new int[schema.size()][];


//    int prefixLength = prefix.numFields();
    int prefixLength = 5;
    int curLevel = prefixLength - 1;
    StructField[] fieldArray = new StructField[prefixLength];
    {
        for (int i=0; i < prefixLength; i++){
        fieldArray[i] = new StructField(schema.get(i).name(), schema.get(i).dataType(), schema.get(i).nullable());
        }
    }
//    StructType structType = new StructType(fieldArray);
    DataType[] dataTypes = {StringType, DoubleType};
    //        Object[] prefixArray = ((List<Object>) prefix.toSeq(structType)).toArray();
    int[] curRelevantRelationIndices = relevantRelationIndicesForEachAttr[curLevel];
    int numRelevantRelations = curRelevantRelationIndices.length;
//    int[][] prefixIndicesForEachChild = new int[numRelevantRelations][];
    int[][] prefixIndicesForEachChild = {{1,2}, {1}, {0,2}, {2}};
    Object[][] childrenTries = new Object[numRelevantRelations][];
    int curChildIndex;
    int[] curPrefixIndices;
    Object[] curPrefix;



    static class LeapFrogUnaryIterator implements java.util.Iterator<Object> {

        private final Object[][] childrenTries;
        private final int numArrays;

        private double valueCache;
        private boolean hasNextCache;
        private boolean cacheValid = false;

        private final int[] currentCursors;
        private int childIdx = 0;


        LeapFrogUnaryIterator(Object[][] tries){
            childrenTries = tries;
            numArrays = tries.length;
            currentCursors = new int[numArrays];
            for(Object[] trie: tries){
                if (trie.length == 0) {
                    hasNextCache = false;
                    cacheValid = true;
                    return;
                }
            }
            java.util.Arrays.sort(childrenTries, new ArrayFirstElementComparator());
        }

        //  find the position i so that array[i] >= value and i is the minimal value
        //  noted: the input array should be sorted
        private int seek(Object[] array, Object value, int _left) {
            int left = _left;
            int right = array.length;

            while (right > left) {
                int mid = left + (right - left) / 2;
                Object midVal = array[mid];

                int comp = 0;
                if ((double) midVal > (double) value) {
                    comp = 1;
                } else if ((double) midVal < (double) value) {
                    comp = -1;
                }

                if (comp == 0)
                    return mid; //negative if test < value
                else if (comp > 0) //list(mid) > value
                    right = mid;
                else left = mid + 1;
            }

            return right;
        }

        @Override
        public boolean hasNext() {
            if (cacheValid) return hasNextCache;
//            int prevIdx = (childIdx - 1) % numArrays;
            int prevIdx = Math.floorMod((childIdx - 1), numArrays);
            Object curMax = childrenTries[prevIdx][currentCursors[prevIdx]];
            while (!cacheValid) {
                valueCache = (double) childrenTries[childIdx][currentCursors[childIdx]];

                if (valueCache == (double) curMax) {
                    hasNextCache = true;
                    cacheValid = true;
                }
                else{
                    Object[] curArray = childrenTries[childIdx];
                    int curPos = seek(curArray, curMax, currentCursors[childIdx]);
                    currentCursors[childIdx] = curPos;

                    if (curPos == curArray.length){
                        hasNextCache = false;
                        cacheValid = true;
                    } else {
                        curMax = curArray[curPos];
                        childIdx = (childIdx + 1) % numArrays;
                    }
                }
            }
            return hasNextCache;
        }

        @Override
        public Object next() {
            if (!hasNext())
                throw new  java.util.NoSuchElementException("This iterator has been traversed.");
            else {
                cacheValid = false;
                currentCursors[childIdx] += 1;
                if (currentCursors[childIdx] == childrenTries[childIdx].length) {
                    hasNextCache = false;
                    cacheValid = true;
                }else {
                    childIdx = (childIdx + 1) % numArrays;
                }
                return valueCache;
            }
        }
    }


    static class ArrayFirstElementComparator implements java.util.Comparator<Object[]> {

        @Override
        public int compare(Object[] o1, Object[] o2) {
            if (o1.length == 0 && o2.length == 0) return 0;
            else if (o1.length == 0) return -1;
            else if (o2.length == 0) return 1;
            if ((double)o1[0] < (double)o2[0])
                return -1;
            else if ((double) o1[0] > (double) o2[0])
                return 1;

            return 0;
        }

    }



    // at the code generation time, the schema, curRelevantRelationIndices are known.
    @Override
    public java.util.Iterator<Object> getIterator(InternalRow prefix, TrieInternalBlock[] tries) {

        for (int i=0; i<numRelevantRelations; i++) {
            curChildIndex = curRelevantRelationIndices[i];
            curPrefixIndices = prefixIndicesForEachChild[i];
            curPrefix = new Object[curPrefixIndices.length];
            for (int j=0; j<curPrefixIndices.length; j++){
//                curPrefix[j] = prefix.get(curPrefixIndices[j], schema.get(curPrefixIndices[j]).dataType());
//                curPrefix[j] = prefix.get(curPrefixIndices[j], ((StructField[]) structType.toArray(ClassTag$.MODULE$.apply(StructField.class)))[j].dataType());
                curPrefix[j] = prefix.get(curPrefixIndices[j], dataTypes[j]);
            }
            TrieInternalBlock curTrie = tries[curChildIndex];
//            curTrie.setKey(InternalRow.apply(curPrefix));
//            childrenTries[i] = curTrie.get(InternalRow.apply(curPrefix), ClassTag$.MODULE$.apply(Object[].class));
            childrenTries[i] = curTrie.get(InternalRow.apply(curPrefix));
        }
        return new LeapFrogUnaryIterator(childrenTries);
    }
}





//lgh code fragments:

// 1. hasNext()
//
//
//            Object[] curArray = childrenTries[childIdx];
//            int curPos = Intersection.seek(curArray, maximalElement, currentCursors[childIdx]);
//
//            if (curPos < curArray.length) {
//                Object curVal = curArray[curPos];
//                if ((double)curVal == maximalElement) {
//                    count += 1;
//                    if (count == numArrays) {
//                        valueCache = maximalElement;
//                        count = 0;
//                        hasNextCache = true;
//                        cacheValid = true;
//                    }
//                } else {
//                    count = 1;
//                    maximalElement = (double)curVal;
//                }
//            } else {
//                hasNextCache = false;
//                cacheValid = true;
//            }
//            currentCursors[childIdx] = curPos;
//            childIdx = (childIdx + 1) % numArrays;

//2. next()
//        int curPos = currentCursors[p];
//        Object[] curArray = childrenTries[p];
//        curPos += 1;
//        if (curPos < curArray.length) {
//            maximalElement = (double)curArray[curPos];
//            count = 1;
//        } else {
//            isEnd = true;
//        }
//        currentCursors[p] = curPos;
//        p = (p + 1) % numOfArrays;
//
//        return value;

//3.
//    private double maximalElement = Double.MIN_VALUE;
//    private int count;
//    private boolean isEnd = false;
//  var nextConsumed:Boolean = true

//4.
//class Intersection {
//
//    //  find the position i so that array[i] >= value and i is the minimal value
//    //  noted: the input array should be sorted
//    static int seek(Object[] array, Object value, int _left) {
//        int left = _left;
//        int right = array.length;
//
//        while (right > left) {
//            int mid = left + (right - left) / 2;
//            Object midVal = array[mid];
//
//            int comp = 0;
//            if ((double) midVal > (double) value) {
//                comp = 1;
//            } else if ((double) midVal < (double) value) {
//                comp = -1;
//            }
//
//            if (comp == 0)
//                return mid; //negative if test < value
//            else if (comp > 0) //list(mid) > value
//                right = mid;
//            else left = mid + 1;
//        }
//
//        return right;
//    }
//}

