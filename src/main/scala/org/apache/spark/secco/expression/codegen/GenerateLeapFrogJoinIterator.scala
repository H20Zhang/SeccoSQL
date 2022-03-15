package org.apache.spark.secco.expression.codegen

import org.apache.spark.secco.codegen.{CodeAndComment, CodeGenerator, CodegenContext}
import org.apache.spark.secco.execution.storage.block.TrieInternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.types.DataType

import scala.collection.mutable

abstract class BaseLeapFrogJoinIteratorProducer {
  def getIterator(tries: Array[TrieInternalBlock]): java.util.Iterator[InternalRow]
}

object GenerateLeapFrogJoinIterator extends
  CodeGenerator[(Seq[Attribute], Seq[Seq[Attribute]]), BaseLeapFrogJoinIteratorProducer] {
  /** Generates a class for a given input expression.  Called when there is not cached code
    * already available.
    */
  override protected def create(in: (Seq[Attribute], Seq[Seq[Attribute]])): BaseLeapFrogJoinIteratorProducer
  = create(in._1, in._2)

  /** Canonicalizes an input expression. Used to avoid double caching expressions that differ only
    * cosmetically.
    */
  override protected def canonicalize(in: (Seq[Attribute], Seq[Seq[Attribute]])): (Seq[Attribute], Seq[Seq[Attribute]])
  = in

  /** Binds an input expression to a given input schema */
  override protected def bind(in: (Seq[Attribute], Seq[Seq[Attribute]]), inputSchema: Seq[Attribute]):
  (Seq[Attribute], Seq[Seq[Attribute]])
  = in


  def getLeapFrogJoinIteratorCode(ctx: CodegenContext,
                                  schema: Seq[Attribute], childrenSchemas: Seq[Seq[Attribute]]): (String, String) = {

    val schemaLength = schema.length
    val dataTypes = schema.map(_.dataType).distinct
    val iteratorCodes = new mutable.HashMap[DataType, (String, String)]
    for(dt <- dataTypes){
      iteratorCodes(dt) = GenerateUnaryIterator.getUnaryIteratorCode(ctx, dt)
    }
    val producerCodes = new Array[(String, String)](schemaLength)
    for(i <- producerCodes.indices){
      producerCodes(i) = GenerateUnaryIterator.getUnaryIteratorProducerCode(ctx,
        schema.slice(0, i + 1), childrenSchemas, iteratorCodes(schema(i).dataType)._1)
    }

    val className = s"LeapFrogJoinIterator"

    val producerAssignments: Seq[String] =
      producerCodes.indices.map {
        i => s"producers[$i] = new ${producerCodes(i)._1}();"
      }

    val baseUnaryIteratorProducerClassName = classOf[BaseUnaryIteratorProducer].getName

    val classDefinition =
      s"""
         |// Iterator definitions
         |${iteratorCodes.values.map(_._2).mkString("\n")}
         |
         |// IteratorProducer definitions
         |${producerCodes.map(_._2).mkString("\n")}
         |
         |class $className implements java.util.Iterator<InternalRow> {
         |    private final TrieInternalBlock[] childrenTries;
         |    private final ${baseUnaryIteratorProducerClassName}[] producers =
         |                                                  new $baseUnaryIteratorProducerClassName[$schemaLength];
         |
         |    private final java.util.Iterator<Object>[] iterators = new java.util.Iterator<Object>[$schemaLength];
         |    private final Object[] arrayCache = new Object[$schemaLength];
         |    private boolean hasNextCache;
         |    private boolean hasNextCacheValid;
         |
         |    public $className(TrieInternalBlock[] children)
         |    {
         |        childrenTries = children;
         |        hasNextCacheValid = false;
         |        // Producers assignment
         |        ${producerAssignments.mkString("\n")}
         |        init();
         |    }
         |
         |    private void init() {
         |        int i = 0;
         |        while (!hasNextCacheValid && i < $schemaLength) {
         |            java.util.Iterator<Object> curIter = producers[i].getIterator(
         |                    InternalRow.apply(java.util.Arrays.copyOf(arrayCache, i)), childrenTries);
         |            iterators[i] = curIter;
         |            if (curIter.hasNext()) {
         |                arrayCache[i] = curIter.next();
         |            }
         |            else {
         |                hasNextCache = false;
         |                hasNextCacheValid = true;
         |            }
         |            i += 1;
         |            if (i == $schemaLength) {
         |               hasNextCache = true;
         |               hasNextCacheValid = true;
         |            }
         |        }
         |    }
         |
         |    TrieInternalBlock[] tries() {
         |        return childrenTries;
         |    }
         |
         |    boolean isSorted() {
         |        return true;
         |    }
         |
         |    boolean isBreakPoint() {
         |        return false;
         |    }
         |
         |    @Override
         |    public boolean hasNext() {
         |        if (hasNextCacheValid) return hasNextCache;
         |        int i = ${schemaLength - 1};
         |        while(!hasNextCacheValid){
         |            java.util.Iterator<Object> curIter = iterators[i];
         |            if (curIter.hasNext()) {
         |                arrayCache[i] = curIter.next();
         |                System.out.println("in hasNext():" + arrayCache[i]);
         |                if(i == ${schemaLength - 1}){
         |                    hasNextCache = true;
         |                    hasNextCacheValid = true;
         |                }
         |                else{
         |                    i += 1;
         |                    iterators[i] = producers[i].getIterator(
         |                            InternalRow.apply(java.util.Arrays.copyOf(arrayCache, i)), childrenTries);
         |                }
         |            }
         |            else {
         |                if(i == 0)
         |                {
         |                    hasNextCache = false;
         |                    hasNextCacheValid = true;
         |                }
         |                else{
         |                    i -= 1;
         |                }
         |            }
         |        }
         |        return hasNextCache;
         |    }
         |
         |    @Override
         |    public InternalRow next() {
         |        if(!hasNext()) throw new java.util.NoSuchElementException("next on empty iterator");
         |        else
         |        {
         |            hasNextCacheValid = false;
         |            return InternalRow.apply(arrayCache);
         |        }
         |    }
         |}
         |""".stripMargin

    (className, classDefinition)
  }

  def create(schema: Seq[Attribute], childrenSchemas: Seq[Seq[Attribute]]): BaseLeapFrogJoinIteratorProducer = {

    val ctx = new CodegenContext
    val (leapFrogJoinClassName, leapFrogJoinClassDefinition) = getLeapFrogJoinIteratorCode(ctx, schema, childrenSchemas)

    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificLeapFrogJoinIteratorProducer();
         |}
         |
         |public class SpecificLeapFrogJoinIteratorProducer extends ${classOf[BaseLeapFrogJoinIteratorProducer]
                                                                                                       .getName} {
         |
         |    $leapFrogJoinClassDefinition
         |
         |    @Override
         |    public java.util.Iterator<InternalRow> getIterator(TrieInternalBlock[] tries) {
         |        return new $leapFrogJoinClassName(tries);
         |    }
         |}
         |""".stripMargin

    val code = CodeFormatter.stripOverlappingComments(new CodeAndComment(codeBody, Map.empty))
    logDebug(s"SpecificLeapFrogJoinIteratorProducer():\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(Array.empty).asInstanceOf[BaseLeapFrogJoinIteratorProducer]
  }
}
