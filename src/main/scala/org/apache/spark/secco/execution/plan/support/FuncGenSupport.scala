package org.apache.spark.secco.execution.plan.support

import org.apache.spark.secco.execution.{OldInternalDataType, OldInternalRow}

trait FuncGenSupport {

  def genSelectionFunc(
      selectionExprs: Seq[(String, String, String)],
      localAttributeOrder: Seq[String]
  ): OldInternalRow => Boolean = {
    val selectionFuncs = selectionExprs.map { case (l, op, r) =>
      val lPos = localAttributeOrder.indexOf(l)
      val rPos = localAttributeOrder.indexOf(r)

      op match {
        case "="  => (row: OldInternalRow) => row(lPos) == row(rPos)
        case "<"  => (row: OldInternalRow) => row(lPos) < row(rPos)
        case ">"  => (row: OldInternalRow) => row(lPos) > row(rPos)
        case "!=" => (row: OldInternalRow) => row(lPos) != row(rPos)
      }
    }.toArray

    (row: OldInternalRow) => selectionFuncs.forall(func => func(row))
  }

  def genProjectionFunc(
      projectionList: Seq[String],
      inputAttributOrder: Seq[String]
  ): (
      OldInternalRow => Array[OldInternalDataType],
      Array[OldInternalDataType]
  ) = {
    val pos =
      projectionList.map(attr => inputAttributOrder.indexOf(attr)).toArray
    val size = pos.length
    val processedRow = new Array[OldInternalDataType](size)
    var i = 0
    while (i < size) {
      processedRow(i) = Double.MaxValue
      i += 1
    }

    (
      (row: OldInternalRow) => {
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
  ): (OldInternalDataType, OldInternalDataType) => OldInternalDataType = {
    funcName match {
      case "Sum"   => (x: OldInternalDataType, y: OldInternalDataType) => x + y
      case "Count" => (x: OldInternalDataType, y: OldInternalDataType) => x + y
      case "Min"   => Math.min
      case "Max"   => Math.max
      case "sum"   => (x: OldInternalDataType, y: OldInternalDataType) => x + y
      case "count" => (x: OldInternalDataType, y: OldInternalDataType) => x + y
      case "min"   => Math.min
      case "max"   => Math.max
      case "*"     => (x: OldInternalDataType, y: OldInternalDataType) => x * y
      case "+"     => (x: OldInternalDataType, y: OldInternalDataType) => x + y
    }
  }

  def genMultiplyFunc(
      funcExpr: String,
      tupleAttributeOrder: Seq[String]
  ): OldInternalRow => OldInternalDataType = {

    val attrExtractRegex = "\\w+".r
    val opExtractRegex = "[\\*|+]".r

    val attributes = attrExtractRegex.findAllIn(funcExpr).toArray
    val pos = attributes.map(tupleAttributeOrder.indexOf)
    val opStrings = opExtractRegex.findAllIn(funcExpr).toArray
    val opFuncs = opStrings.map(genSumFunc)

    val tupleSize: Int = attributes.size

    if (opFuncs.nonEmpty) {
      if (opStrings(0) == "*" && attributes.isEmpty) { (row: OldInternalRow) =>
        {
          1.0
        }
      } else { (row: OldInternalRow) =>
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
      (row: OldInternalRow) => {
        row(pos(0))
      }
    }
  }

  def genTransformFunc(
      funcExpr: String,
      tupleAttributeOrder: Seq[String]
  ): OldInternalRow => OldInternalDataType = {

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

    if (opFuncs.nonEmpty) { (row: OldInternalRow) =>
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
      (row: OldInternalRow) => {
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
