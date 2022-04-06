package org.apache.spark.secco.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.secco.codegen._
import org.apache.spark.secco.execution.storage.block.{TrieInternalBlock, TrieInternalBlockBuilder}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{Attribute, BoundReference}
import org.apache.spark.secco.expression.codegen.GenerateUnaryIterator
import org.apache.spark.secco.optimization.util.AttributeOrder
import org.apache.spark.secco.types.{DataType, StructType}

import scala.collection.mutable

/** Physical plan for Build Trie Block. */
case class BuildTrieExec(child: SeccoPlan)
  extends BuildExecPushBasedCodegen {

  private var trieBuilderTerm: String = _

  override protected def doProduceBulk(ctx: CodegenContext): (String, String) = {
    val builderClassName = classOf[TrieInternalBlockBuilder].getName
    trieBuilderTerm = ctx.addMutableState(builderClassName, "trieBuilder")
    val blockClassName = classOf[TrieInternalBlock].getName
    val trieBlockTerm = ctx.addMutableState(blockClassName, "trieBlock")
    val codeStr = {
      s"""
         |$trieBuilderTerm = ($builderClassName) ${
            ctx.addReferenceObj(s"HashMapBuilder",
            new TrieInternalBlockBuilder(StructType.fromAttributes(child.output)))};
         |${child.asInstanceOf[PushBasedCodegen].produce(ctx, this)}
         |$trieBlockTerm = $trieBuilderTerm.build();
         |""".stripMargin
    }
    (codeStr, trieBlockTerm)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       |${row.code}
       |$trieBuilderTerm.add(${row.value}.copy());
       |""".stripMargin
  }

  override protected def doExecute(): RDD[OldInternalBlock] = ???

  override def inputRowIterators(): Seq[Iterator[InternalRow]] =
    child.asInstanceOf[PushBasedCodegen].inputRowIterators()
}

case class LeapFrogJoinExec(children:Seq[SeccoPlan], attrOrder: AttributeOrder)
  extends PushBasedCodegen with MultipleChildrenExecNode {

  private val numChildren = children.length
  private lazy val repAttrs = attrOrder.repAttrOrder.order
  private lazy val numRepAttrs = repAttrs.length

  var iteratorClassNames: mutable.HashMap[DataType, String] = _
  var producerClassNames: Array[String] = _
  // Strings named as "...Term" are for variable names used in generated code
  var trieBuildersTerm: String = _
  var triesTerm: String = _

  private var producerTerms: Seq[String] = _
  private var unaryIterTerms: Seq[String] = _

  private var bindingTerm: String = _

  override def output: Seq[Attribute] = attrOrder.order

  protected override def doProduce(ctx: CodegenContext): String = {
    bindingTerm = ctx.addMutableState("java.lang.Object[]",
      "binding", v => s"$v = new java.lang.Object[${numRepAttrs}];", forceInline = true)
    triesTerm = ctx.addMutableState(s"${classOf[TrieInternalBlock].getName}[]",
      "trieBlocks", v => s"$v = new ${classOf[TrieInternalBlock].getName}[${numChildren}];",
      forceInline = true)

    val dataTypes = repAttrs.map(_.dataType).distinct
    iteratorClassNames = new mutable.HashMap[DataType, String]
    for(dt <- dataTypes){
      val (className, classDefinition) = GenerateUnaryIterator.getUnaryIteratorCode(ctx, dt)
      iteratorClassNames(dt) = className
      ctx.addInnerClass(classDefinition)
    }
    producerClassNames = new Array[String](numRepAttrs)
    for(i <- producerClassNames.indices){
      val (className, classDefinition) = getUnaryIteratorProducerCode(ctx,  i,
        children.map(_.asInstanceOf[PushBasedCodegen].output), iteratorClassNames(repAttrs(i).dataType))
      producerClassNames(i) = className
      ctx.addInnerClass(classDefinition)
    }
    producerTerms = repAttrs.indices.map( idx => {
      val className = producerClassNames(idx)
      ctx.addMutableState(className,
        s"unaryIterProducer_${idx}", v => s"$v = new $className();", forceInline = true)}
    )
    unaryIterTerms = repAttrs.zipWithIndex.map { case (attr, idx) =>
      val className = iteratorClassNames(attr.dataType)
      ctx.addMutableState(className, s"unaryIterProducer_${idx}", forceInline = true)
    }

    val builtOnceTerm = ctx.addMutableState("boolean", "builtOnce", v => s"$v = false;")
    val loopedOnceTerm = ctx.addMutableState("boolean", "loopedOnce", v => s"$v = false;")

    var builtTrieTerms: Seq[String] = null
    s"""
       |System.out.println("in LeapFrogExec before children produce()");
       |${val tupleSeq = children.map{ child =>
            val result = child.asInstanceOf[BuildTrieExec].produceBulk(ctx, this)
            ctx.incrementCurInputIndex()
            result}
         builtTrieTerms = tupleSeq.map{ case (_, term) => term }
         tupleSeq.map{ case (codeStr, _) => codeStr}.mkString("\n")
         }
       |System.out.println("in LeapFrogExec after children produceBulk()");
       |if (!$builtOnceTerm){
       |  for(int i = 0; i < $numChildren; i++){
       |      ${builtTrieTerms.zipWithIndex.map{ case(term, idx) => s"$triesTerm[$idx] = $term;"}.mkString("\n")}
       |  }
       |  $builtOnceTerm = true;
       |}
       |System.out.println("in LeapFrogExec before loop:");
       |if(!$loopedOnceTerm){
       |  ${generateLeapFrogUnaryIteratorCode(ctx, 0)}
       |  $loopedOnceTerm = true;
       |}
       |System.out.println("in LeapFrogExec after loop:");
    """.stripMargin
  }

  // recursive 用于生成LeapFrogJoin的while循环代码。递归函数，每个attribute对应一层调用。最内层调用consume，把row push到上层。
  private def generateLeapFrogUnaryIteratorCode(ctx: CodegenContext, idx: Int): String = {
    if(idx < numRepAttrs) {
      val curUnaryIterTerm = unaryIterTerms(idx)
      s"""
         |${curUnaryIterTerm} = (${iteratorClassNames(repAttrs(idx).dataType)})${producerTerms(idx)}.getIterator();
         |while(${curUnaryIterTerm}.hasNext())
         |{
         |  ${bindingTerm}[${idx}] = ${curUnaryIterTerm}.next();
         |  ${generateLeapFrogUnaryIteratorCode(ctx, idx + 1)}
         |}
  """.stripMargin
    }
    else {
      val rowClassName = classOf[InternalRow].getName
      val naturalJoinLikeRowTerm = ctx.addMutableState(rowClassName, "outputRow")
      val outputVars = {
        // creating the vars will make the parent consume add an unsafe projection.
        ctx.INPUT_ROW = naturalJoinLikeRowTerm
        ctx.currentVars = null
        output.map { a =>
          val repAttrIdxForCurAttr = repAttrs.indexOf(attrOrder.equiAttrs.attr2RepAttr(a))
          BoundReference(repAttrIdxForCurAttr, a.dataType, a.nullable).genCode(ctx)
        }
      }
      logInfo(s"outputVars: $outputVars")
      s"""
        |$naturalJoinLikeRowTerm = $rowClassName.apply($bindingTerm);
        |${consume(ctx, outputVars = outputVars)}
        |""".stripMargin
    }
  }

  private def getUnaryIteratorProducerCode(ctx: CodegenContext,
                                   curLevel: Int, childrenSchemas: Seq[Seq[Attribute]],
                                   unaryIteratorClassName: String): (String, String) = {
    val className = ctx.freshName("SpecificUnaryIteratorProducer")

    val relevantRelationIndicesForEachAttr: Seq[Seq[Int]] =
      repAttrs.map { curAttr =>
        childrenSchemas.indices.filter { childIdx =>
          attrOrder.equiAttrs.repAttr2Attr(curAttr).exists{i =>
            childrenSchemas(childIdx).contains(i)}
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

    val prefixLength = curLevel

    val dt = repAttrs(curLevel).dataType
    val jt = CodeGenerator.javaType(dt)
    val bt = CodeGenerator.boxedType(dt)
    val fullPt = CodeGenerator.primitiveTypeName(dt)
    val pt = fullPt.substring(fullPt.lastIndexOf(".") + 1)

    val prefixSchema = repAttrs.slice(0, prefixLength)
    val curRelevantRelationIndices = relevantRelationIndicesForEachAttr(curLevel)
    val numRelevantRelations = curRelevantRelationIndices.length
    val prefixIndicesForEachChild = curRelevantRelationIndices.map(getPrefixIndices(curLevel, _))

    val classDefinition =
      s"""
         |private class $className {
         |
         |    final DataType[] dataTypes = ${prefixSchema.map("DataTypes." + _.dataType).mkString("{", ",", "}")};
         |    final int[] curRelevantRelationIndices = ${curRelevantRelationIndices.mkString("{", ",", "}")};
         |    final int[][] prefixIndicesForEachChild = ${prefixIndicesForEachChild
        .map(_.mkString(",")).mkString("{{", "},{", "}}")};
         |    final $jt[][] childrenInArrays = new $jt[$numRelevantRelations][];
         |
         |    //at the code generation time, the schema, curRelevantRelationIndices are known.
         |    public java.util.Iterator<$bt> getIterator() {
         |        for (int i=0; i<$numRelevantRelations; i++) {
         |            int curChildIndex = curRelevantRelationIndices[i];
         |            int[] curPrefixIndices = prefixIndicesForEachChild[i];
         |            java.lang.Object[] curPrefix = new java.lang.Object[curPrefixIndices.length];
         |            for (int j=0; j<curPrefixIndices.length; j++){
         |                curPrefix[j] = $bindingTerm[curPrefixIndices[j]];
         |            }
         |            TrieInternalBlock curTrie = $triesTerm[curChildIndex];
         |            // System.out.println("in getIterator: curTrie" + curTrie);
         |            System.out.println("in getIterator: curPrefix" + InternalRow.apply(curPrefix));
         |            childrenInArrays[i] = curTrie.get$pt(InternalRow.apply(curPrefix));
         |            java.lang.Object[] tempArray = new java.lang.Object[childrenInArrays[i].length];
         |            for (int j=0; j<childrenInArrays[i].length; j++){
         |                tempArray[j] = childrenInArrays[i][j];
         |            }
         |            System.out.printf("in getIterator: childrenInArrays[%d]: (" + tempArray.length + ")", i);
         |            for (int j=0; j<tempArray.length; j++){
         |                System.out.print(tempArray[j] + ",");
         |            }
         |            System.out.println();
         |            //System.out.printf("in getIterator: childrenInArrays[%d]: (" + tempArray.length + ")" + InternalRow.apply(tempArray) + "%n", i);
         |        }
         |        return new $unaryIteratorClassName(childrenInArrays);
         |    }
         |}""".stripMargin
    (className, classDefinition)
  }


  /** Perform the computation for computing the result of the query as an `RDD[InternalBlock]`
    *
    * Overridden by concrete implementations of SparkPlan.
    */
  override protected def doExecute(): RDD[OldInternalBlock] = ???

  override def inputRowIterators(): Seq[Iterator[InternalRow]] = {
    println("in LeapFrogExec.inputRowIterators()")
    children.flatMap(_.asInstanceOf[PushBasedCodegen].inputRowIterators())
  }
}