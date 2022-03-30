package org.apache.spark.secco.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.secco.codegen._
import org.apache.spark.secco.execution.storage.block.{TrieInternalBlock, TrieInternalBlockBuilder}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{Attribute, BoundReference}
import org.apache.spark.secco.expression.codegen.{BaseUnaryIteratorProducer, GenerateUnaryIterator}
import org.apache.spark.secco.types.{DataType, StructType}

import scala.collection.mutable

case class LeapFrogJoinExec(children:Seq[SeccoPlan], outputAttrs: Array[Attribute])
  extends PushBasedCodegen with MultipleChildrenExecNode {

  private val numChildren = children.length
  private val numAttrs = outputAttrs.length

  var iteratorClassNames: mutable.HashMap[DataType, String] = _
  var producerClassNames: Array[String] = _
  // Strings named as "...Term" are for variable names used in generated code
  var trieBuildersTerm: String = _
  var triesTerm: String = _

  private var producerTerms: Seq[String] = _
  private var unaryIterTerms: Seq[String] = _

  private var bindingTerm: String = _


  protected override def doProduce(ctx: CodegenContext): String = {
    bindingTerm = ctx.addMutableState("java.lang.Object[]",
      "binding", v => s"$v = new java.lang.Object[${numAttrs}];", forceInline = true)
    trieBuildersTerm = ctx.addMutableState(s"${classOf[TrieInternalBlockBuilder].getName}[]",
      "trieBuilders", v => s"$v = new ${classOf[TrieInternalBlockBuilder].getName}[${numChildren}];",
      forceInline = true)
    triesTerm = ctx.addMutableState(s"${classOf[TrieInternalBlock].getName}[]",
      "trieBlocks", v => s"$v = new ${classOf[TrieInternalBlock].getName}[${numChildren}];",
      forceInline = true)

    val dataTypes = outputAttrs.map(_.dataType).distinct
    iteratorClassNames = new mutable.HashMap[DataType, String]
    for(dt <- dataTypes){
      val (className, classDefinition) = GenerateUnaryIterator.getUnaryIteratorCode(ctx, dt)
      iteratorClassNames(dt) = className
      ctx.addInnerClass(classDefinition)
    }
    producerClassNames = new Array[String](numAttrs)
    for(i <- producerClassNames.indices){
      val (className, classDefinition) = getUnaryIteratorProducerCode(ctx,  outputAttrs.slice(0, i + 1),
        children.map(_.asInstanceOf[PushBasedCodegen].output), iteratorClassNames(outputAttrs(i).dataType))
      producerClassNames(i) = className
      ctx.addInnerClass(classDefinition)
    }
    producerTerms = outputAttrs.indices.map( idx => {
      val className = producerClassNames(idx)
      ctx.addMutableState(className,
        s"unaryIterProducer_${idx}", v => s"$v = new $className();", forceInline = true)}
    )
    unaryIterTerms = outputAttrs.zipWithIndex.map { case (attr, idx) =>
      val className = iteratorClassNames(attr.dataType)
      ctx.addMutableState(className, s"unaryIterProducer_${idx}", forceInline = true)
    }

    val builtOnceTerm = ctx.addMutableState("boolean", "builtOnce", v => s"$v = false;")
    val loopedOnceTerm = ctx.addMutableState("boolean", "loopedOnce", v => s"$v = false;")

//    ${children.zipWithIndex.map{case (child, idx) =>
//      s"${trieBuildersTerm}[$idx] = new ${classOf[TrieInternalBlockBuilder].getName}" +
//        s"(${ctx.addReferenceObj(s"schema $idx",
//          StructType.fromAttributes(child.output))});"}
    // the generated code string is as below:
    s"""
       |System.out.println("in LeapFrogExec before children produce()");
       |${children.zipWithIndex.map{case (child, idx) =>
           s"${trieBuildersTerm}[$idx] = ${ctx.addReferenceObj(s"builder $idx",
               new TrieInternalBlockBuilder(StructType.fromAttributes(child.output)))};"}
       .mkString("\n")}
       |${children.zipWithIndex.map{ case (child, childIdx) => {
             ctx.setLeapFrogJoinChildIndex(childIdx)
             val childCode = child.asInstanceOf[PushBasedCodegen].produce(ctx, this);
             ctx.incrementCurInputIndex()
             childCode
         } }.mkString("\n")}
       |System.out.println("in LeapFrogExec after children produce()");
       |if (!$builtOnceTerm){
       |  for(int i = 0; i < $numChildren; i++){
       |      ${triesTerm}[i] = ${trieBuildersTerm}[i].build();
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
    if(idx < numAttrs) {
      val curUnaryIterTerm = unaryIterTerms(idx)
      s"""
         |${curUnaryIterTerm} = (${iteratorClassNames(outputAttrs(idx).dataType)})${producerTerms(idx)}.getIterator();
         |while(${curUnaryIterTerm}.hasNext())
         |{
         |  ${bindingTerm}[${idx}] = ${curUnaryIterTerm}.next();
         |  ${generateLeapFrogUnaryIteratorCode(ctx, idx + 1)}
         |}
  """.stripMargin
    }
    else {
      val rowClassName = classOf[InternalRow].getName
      val outputRowTerm = ctx.addMutableState(rowClassName, "outputRow")
      val outputVars = {
        // creating the vars will make the parent consume add an unsafe projection.
        ctx.INPUT_ROW = outputRowTerm
        ctx.currentVars = null
        output.zipWithIndex.map { case (a, i) =>
          BoundReference(i, a.dataType, a.nullable).genCode(ctx)
        }
      }
      s"""
        |$outputRowTerm = $rowClassName.apply($bindingTerm);
        |${consume(ctx, outputVars = outputVars, row=outputRowTerm)}
        |""".stripMargin
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val doCopy = if (needCopyResult) {
      ".copy()"
    } else {
      ""
    }
    s"""
       |${row.code}
       |$trieBuildersTerm[${ctx.getLeapFrogJoinChildIndex}].add(${row.value});
     """.stripMargin.trim
  }

  def getUnaryIteratorProducerCode(ctx: CodegenContext,
                                   prefixAndCurAttributes: Seq[Attribute], childrenSchemas: Seq[Seq[Attribute]],
                                   unaryIteratorClassName: String): (String, String) = {
    val className = ctx.freshName("SpecificUnaryIteratorProducer")

    val relevantRelationIndicesForEachAttr: Seq[Seq[Int]] =
      prefixAndCurAttributes.indices.map { attrIdx =>
        val curAttr = prefixAndCurAttributes(attrIdx)
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

    val prefixLength = prefixAndCurAttributes.size - 1
    val curLevel = prefixLength


    //    |    final DataType[] dataTypes = {${schema.slice(0, prefixLength).map (attr => {
    //                                     val boxedType = CodeGenerator.boxedType(attr.dataType)
    //                                     "DataTypes." + boxedType.substring(boxedType.lastIndexOf(".") + 1) + "Type"
    //                                     }).mkString(", ")}};

    val dt = prefixAndCurAttributes(curLevel).dataType
    val jt = CodeGenerator.javaType(dt)
    val bt = CodeGenerator.boxedType(dt)
    val fullPt = CodeGenerator.primitiveTypeName(dt)
    val pt = fullPt.substring(fullPt.lastIndexOf(".") + 1)

    val prefixSchema = prefixAndCurAttributes.slice(0, prefixLength)
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

//  override def inputRowIterator(): Iterator[InternalRow] = ???
  override def inputRowIterators(): Seq[Iterator[InternalRow]] = {
    println("in LeapFrogExec.inputRowIterators()")
    children.flatMap(_.asInstanceOf[PushBasedCodegen].inputRowIterators())
  }
}