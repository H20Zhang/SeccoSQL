package org.apache.spark.secco.expression.codegen;

import org.apache.spark.secco.execution.plan.computation.newIter.IndexableTableIterator;
import org.apache.spark.secco.execution.storage.block.InternalBlock;
import org.apache.spark.secco.execution.storage.block.TrieInternalBlock;
import org.apache.spark.secco.execution.storage.block.TrieInternalBlockBuilder;
import org.apache.spark.secco.execution.storage.row.InternalRow;
import org.apache.spark.secco.expression.Attribute;
import org.apache.spark.secco.types.StructField;
import org.apache.spark.secco.types.StructType;

//import java.util.Iterator;
import java.util.NoSuchElementException;

//public class SpecificLeapFrogJoinIteratorProducer extends BaseLeapFrogJoinIteratorProducer{
class LeapFrogJoinIterator implements java.util.Iterator<InternalRow> {
    private final TrieInternalBlock[] childrenTries;
    private final int arity;
    private final StructType rowSchema;
    private final BaseUnaryIteratorProducer[] producers;

    private final java.util.Iterator<Object>[] iterators;
    private final Object[] arrayCache;
    private boolean hasNextCache;
    private boolean hasNextCacheValid;

    public LeapFrogJoinIterator(Class<java.util.Iterator<Object>> clazz,
                                TrieInternalBlock[] children, Attribute[] localAttributeOrder)
    {
        childrenTries = children;
        arity = localAttributeOrder.length;
        StructField[] fields = new StructField[arity];
        for (int i = 0; i < arity; i++){
            Attribute attr = localAttributeOrder[i];
            fields[i] = new StructField(attr.name(), attr.dataType(), true);
        }
        rowSchema = new StructType(fields);
        arrayCache = new Object[arity];
        hasNextCacheValid = false;
        producers = new BaseUnaryIteratorProducer[arity];
        iterators = (java.util.Iterator<Object>[]) java.lang.reflect.Array.newInstance(clazz, arity);
        init();
    }

    private void init() {
        int i = 0;
        while (!hasNextCacheValid && i < arity) {
            java.util.Iterator<Object> curIter = producers[i].getIterator(
                    InternalRow.apply(java.util.Arrays.copyOf(arrayCache, i)), childrenTries);
            iterators[i] = curIter;
            if (curIter.hasNext()) {
                arrayCache[i] = curIter.next();
            }
            else {
                hasNextCache = false;
                hasNextCacheValid = true;
            }
            i += 1;
        }
    }

    TrieInternalBlock[] tries() {
        return childrenTries;
    }

    boolean isSorted() {
        return true;
    }

    boolean isBreakPoint() {
        return false;
    }

    InternalBlock results() {
        TrieInternalBlockBuilder builder = new TrieInternalBlockBuilder(rowSchema);
        while(hasNext()){
            builder.add(next());
        }
        return builder.build();
    }

    @Override
    public boolean hasNext() {
        if (hasNextCacheValid) return hasNextCache;
        int i = arity;
        while(!hasNextCacheValid){
            java.util.Iterator<Object> curIter = iterators[i];
            if (curIter.hasNext()) {
                arrayCache[i] = curIter.next();
                if(i == arity){
                    hasNextCache = true;
                    hasNextCacheValid = true;
                }
                else{
                    i += 1;
                    iterators[i] = producers[i].getIterator(
                            InternalRow.apply(java.util.Arrays.copyOf(arrayCache, i)), childrenTries);
                }
            }
            else {
                if(i == 0)
                {
                    hasNextCache = false;
                    hasNextCacheValid = true;
                }
                else{
                    i -= 1;
                }
            }
        }
        return hasNextCache;
    }

    @Override
    public InternalRow next() {
        if(!hasNext()) throw new NoSuchElementException("next on empty iterator");
        else
        {
            hasNextCacheValid = false;
            return InternalRow.apply(arrayCache);
        }
    }
}