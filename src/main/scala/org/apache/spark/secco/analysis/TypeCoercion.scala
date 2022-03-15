package org.apache.spark.secco.analysis

import org.apache.spark.secco.types._

abstract class TypeCoercionBase {

//  protected def findTypeForComplex(
//    t1: DataType,
//    t2: DataType,
//    findTypeFunc: (DataType, DataType) => Option[DataType]): Option[DataType] = (t1, t2) match {
//    case (ArrayType(et1, containsNull1), ArrayType(et2, containsNull2)) =>
//      findTypeFunc(et1, et2).map { et =>
//        ArrayType(et, containsNull1 || containsNull2 ||
//          Cast.forceNullable(et1, et) || Cast.forceNullable(et2, et))
//      }
//    case (MapType(kt1, vt1, valueContainsNull1), MapType(kt2, vt2, valueContainsNull2)) =>
//      findTypeFunc(kt1, kt2)
//        .filter { kt => !Cast.forceNullable(kt1, kt) && !Cast.forceNullable(kt2, kt) }
//        .flatMap { kt =>
//          findTypeFunc(vt1, vt2).map { vt =>
//            MapType(kt, vt, valueContainsNull1 || valueContainsNull2 ||
//              Cast.forceNullable(vt1, vt) || Cast.forceNullable(vt2, vt))
//          }
//        }
//    case (StructType(fields1), StructType(fields2)) if fields1.length == fields2.length =>
//      val resolver = SQLConf.get.resolver
//      fields1.zip(fields2).foldLeft(Option(new StructType())) {
//        case (Some(struct), (field1, field2)) if resolver(field1.name, field2.name) =>
//          findTypeFunc(field1.dataType, field2.dataType).map { dt =>
//            struct.add(field1.name, dt, field1.nullable || field2.nullable ||
//              Cast.forceNullable(field1.dataType, dt) || Cast.forceNullable(field2.dataType, dt))
//          }
//        case _ => None
//      }
//    case _ => None
//  }
}

object TypeCoercion extends TypeCoercionBase {

  /**
    * Check whether the given types are equal ignoring nullable, containsNull and valueContainsNull.
    */
  def haveSameType(types: Seq[DataType]): Boolean = {
    if (types.size <= 1) {
      true
    } else {
      val head = types.head
      types.tail.forall(_.sameType(head))
    }
  }

  /**
    * The method finds a common type for data types that differ only in nullable flags, including
    * `nullable`, `containsNull` of [[ArrayType]] and `valueContainsNull` of [[MapType]].
    * If the input types are different besides nullable flags, None is returned.
    */
  def findCommonTypeDifferentOnlyInNullFlags(t1: DataType, t2: DataType): Option[DataType] = {
    if (t1 == t2) {
      Some(t1)
    } else {
      // The throw statement is added by lgh
      throw new NotImplementedError(s"For now, TypeCoersion.findCommonTypeDifferentOnlyInNullFlags" +
        s"requires the two DataType instances ($t1 and $t2) are exactly the same.")
//      findTypeForComplex(t1, t2, findCommonTypeDifferentOnlyInNullFlags)
    }
  }
}
