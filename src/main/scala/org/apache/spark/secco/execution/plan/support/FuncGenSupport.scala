package org.apache.spark.secco.execution.plan.support

import org.apache.spark.secco.execution.{InternalDataType, InternalRow}

trait FuncGenSupport {

  def genSelectionFunc(
      selectionExprs: Seq[(String, String, String)],
      localAttributeOrder: Seq[String]
  ): InternalRow => Boolean = {
    val selectionFuncs = selectionExprs.map {
      case (l, op, r) =>
        val lPos = localAttributeOrder.indexOf(l)
        val rPos = localAttributeOrder.indexOf(r)

        op match {
          case "="  => (row: InternalRow) => row(lPos) == row(rPos)
          case "<"  => (row: InternalRow) => row(lPos) < row(rPos)
          case ">"  => (row: InternalRow) => row(lPos) > row(rPos)
          case "!=" => (row: InternalRow) => row(lPos) != row(rPos)
        }
    }.toArray

    (row: InternalRow) => selectionFuncs.forall(func => func(row))
  }

  def genProjectionFunc(
      projectionList: Seq[String],
      inputAttributOrder: Seq[String]
  ): (InternalRow => Array[InternalDataType], Array[InternalDataType]) = {
    val pos =
      projectionList.map(attr => inputAttributOrder.indexOf(attr)).toArray
    val size = pos.length
    val processedRow = new Array[InternalDataType](size)
    var i = 0
    while (i < size) {
      processedRow(i) = Double.MaxValue
      i += 1
    }

    (
      (row: InternalRow) => {
        var i = 0
        while (i < size) {
          processedRow(i) = row(pos(i))
          i += 1
        }
        processedRow
      },
      processedRow
    )
  }

  def genSumFunc(
      funcName: String
  ): (InternalDataType, InternalDataType) => InternalDataType = {
    funcName match {
      case "Sum"   => (x: InternalDataType, y: InternalDataType) => x + y
      case "Count" => (x: InternalDataType, y: InternalDataType) => x + y
      case "Min"   => Math.min
      case "Max"   => Math.max
      case "sum"   => (x: InternalDataType, y: InternalDataType) => x + y
      case "count" => (x: InternalDataType, y: InternalDataType) => x + y
      case "min"   => Math.min
      case "max"   => Math.max
      case "*"     => (x: InternalDataType, y: InternalDataType) => x * y
      case "+"     => (x: InternalDataType, y: InternalDataType) => x + y
    }
  }

  def genMultiplyFunc(
      funcExpr: String,
      tupleAttributeOrder: Seq[String]
  ): InternalRow => InternalDataType = {

    val attrExtractRegex = "\\w+".r
    val opExtractRegex = "[\\*|+]".r

    val attributes = attrExtractRegex.findAllIn(funcExpr).toArray
    val pos = attributes.map(tupleAttributeOrder.indexOf)
    val opStrings = opExtractRegex.findAllIn(funcExpr).toArray
    val opFuncs = opStrings.map(genSumFunc)

    val tupleSize: Int = attributes.size

    if (opFuncs.nonEmpty) {
      if (opStrings(0) == "*" && attributes.isEmpty) { (row: InternalRow) =>
        {
          1.0
        }
      } else { (row: InternalRow) =>
        {
          var i = 1
          var res = row(pos(0))
          while (i < tupleSize) {
            val v = row(pos(i))
            res = opFuncs(i - 1)(res, v)
            i += 1
          }
          res
        }
      }
    } else {
      assert(
        pos.size == 1,
        s"wrong numbers of attributes:${attributes} in multiply func"
      )
      (row: InternalRow) => {
        row(pos(0))
      }
    }
  }

  def genTransformFunc(
      funcExpr: String,
      tupleAttributeOrder: Seq[String]
  ): InternalRow => InternalDataType = {

    val attrExtractRegex = "(\\w|\\.)+".r
    val constantExtractRegex = "\\w+\\.\\w+".r
    val opExtractRegex = "[\\*|+]".r

    val attributes = attrExtractRegex.findAllIn(funcExpr).toArray
    val constants =
      constantExtractRegex.findAllIn(funcExpr).toList.map(_.toDouble).toArray
    val pos = attributes.map { attr =>
      if (tupleAttributeOrder.contains(attr)) {
        tupleAttributeOrder.indexOf(attr)
      } else if (constants.contains(attr.toDouble)) {
        -(constants.indexOf(attr.toDouble) + 1)
      } else {
        throw new Exception(s"funcExpr:${funcExpr} error")
      }
    }

    val opStrings = opExtractRegex.findAllIn(funcExpr).toArray
    val opFuncs = opStrings.map(genSumFunc)

    val tupleSize = attributes.length

    if (opFuncs.nonEmpty) { (row: InternalRow) =>
      {
        var i = 1
        var res = 0.0
        if (pos(0) >= 0) {
          res = row(pos(0))
        } else {
          res = constants(-pos(0) - 1)
        }
        while (i < tupleSize) {
          var v = 0.0
          if (pos(i) >= 0) {
            v = row(pos(i))
          } else {
            v = constants(-pos(i) - 1)
          }
          res = opFuncs(i - 1)(res, v)
          i += 1
        }
        res
      }
    } else {
      assert(
        pos.size == 1,
        s"wrong numbers of attributes:${attributes} in multiply func"
      )
      (row: InternalRow) => {
        var v = 0.0
        if (pos(0) >= 0) {
          v = row(pos(0))
        } else {
          v = constants(-pos(0) + 1)
        }
        v
      }
    }
  }

}
