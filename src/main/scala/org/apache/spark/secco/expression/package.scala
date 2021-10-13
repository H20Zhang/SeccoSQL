package org.apache.spark.secco

import java.util.Locale
import com.google.common.collect.Maps
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.types.{
  DataType,
  NullType,
  StructField,
  StructType
}

package object expression {

  /** A place holder expressions used in code-gen, it does not change the corresponding value
    * in the row.
    */
  case object NoOp extends Expression with Unevaluable {
    override def nullable: Boolean = true
    override def dataType: DataType = NullType
    override def children: Seq[Expression] = Nil
  }

  /** Used as input into expressions whose output does not depend on any input value.
    */
  val EmptyRow: InternalRow = null

  /** Helper functions for working with `Seq[Attribute]`.
    */
  implicit class AttributeSeq(val attrs: Seq[Attribute]) extends Serializable {

    /** Creates a StructType with a schema matching this `Seq[Attribute]`. */
    def toStructType: StructType = {
      StructType(attrs.map(a => StructField(a.name, a.dataType, a.nullable)))
    }

    // It's possible that `attrs` is a linked list, which can lead to bad O(n^2) loops when
    // accessing attributes by their ordinals. To avoid this performance penalty, convert the input
    // to an array.
    @transient private lazy val attrsArray = attrs.toArray

    @transient private lazy val exprIdToOrdinal = {
      val arr = attrsArray
      val map = Maps.newHashMapWithExpectedSize[ExprId, Int](arr.length)
      // Iterate over the array in reverse order so that the final map value is the first attribute
      // with a given expression id.
      var index = arr.length - 1
      while (index >= 0) {
        map.put(arr(index).exprId, index)
        index -= 1
      }
      map
    }

    /** Returns the attribute at the given index.
      */
    def apply(ordinal: Int): Attribute = attrsArray(ordinal)

    /** Returns the index of first attribute with a matching expression id, or -1 if no match exists.
      */
    def indexOf(exprId: ExprId): Int = {
      Option(exprIdToOrdinal.get(exprId)).getOrElse(-1)
    }

    private def unique[T](m: Map[T, Seq[Attribute]]): Map[T, Seq[Attribute]] = {
      m.mapValues(_.distinct).map(identity)
    }

    /** Map to use for direct case insensitive attribute lookups. */
    @transient private lazy val direct: Map[String, Seq[Attribute]] = {
      unique(attrs.groupBy(_.name.toLowerCase(Locale.ROOT)))
    }

    /** Map to use for qualified case insensitive attribute lookups with 2 part key */
    @transient private lazy val qualified
        : Map[(String, String), Seq[Attribute]] = {
      // key is 2 part: table/alias and name
      val grouped = attrs.filter(_.qualifier.nonEmpty).groupBy { a =>
        (
          a.qualifier.last.toLowerCase(Locale.ROOT),
          a.name.toLowerCase(Locale.ROOT)
        )
      }
      unique(grouped)
    }

//    /** Map to use for qualified case insensitive attribute lookups with 3 part key */
//    @transient private val qualified3Part: Map[(String, String, String), Seq[Attribute]] = {
//      // key is 3 part: database name, table name and name
//      val grouped = attrs.filter(_.qualifier.length == 2).groupBy { a =>
//        (a.qualifier.head.toLowerCase(Locale.ROOT),
//          a.qualifier.last.toLowerCase(Locale.ROOT),
//          a.name.toLowerCase(Locale.ROOT))
//      }
//      unique(grouped)
//    }

//    /** Perform attribute resolution given a name and a resolver. */
//    def resolve(nameParts: Seq[String], resolver: Resolver): Option[NamedExpression] = {
//      // Collect matching attributes given a name and a lookup.
//      def collectMatches(name: String, candidates: Option[Seq[Attribute]]): Seq[Attribute] = {
//        candidates.toSeq.flatMap(_.collect {
//          case a if resolver(a.name, name) => a.withName(name)
//        })
//      }
//
//      // Find matches for the given name assuming that the 1st two parts are qualifier
//      // (i.e. database name and table name) and the 3rd part is the actual column name.
//      //
//      // For example, consider an example where "db1" is the database name, "a" is the table name
//      // and "b" is the column name and "c" is the struct field name.
//      // If the name parts is db1.a.b.c, then Attribute will match
//      // Attribute(b, qualifier("db1,"a")) and List("c") will be the second element
//      var matches: (Seq[Attribute], Seq[String]) = nameParts match {
//        case dbPart +: tblPart +: name +: nestedFields =>
//          val key = (dbPart.toLowerCase(Locale.ROOT),
//            tblPart.toLowerCase(Locale.ROOT), name.toLowerCase(Locale.ROOT))
//          val attributes = collectMatches(name, qualified3Part.get(key)).filter {
//            a => (resolver(dbPart, a.qualifier.head) && resolver(tblPart, a.qualifier.last))
//          }
//          (attributes, nestedFields)
//        case all =>
//          (Seq.empty, Seq.empty)
//      }
//
//      // If there are no matches, then find matches for the given name assuming that
//      // the 1st part is a qualifier (i.e. table name, alias, or subquery alias) and the
//      // 2nd part is the actual name. This returns a tuple of
//      // matched attributes and a list of parts that are to be resolved.
//      //
//      // For example, consider an example where "a" is the table name, "b" is the column name,
//      // and "c" is the struct field name, i.e. "a.b.c". In this case, Attribute will be "a.b",
//      // and the second element will be List("c").
//      if (matches._1.isEmpty) {
//        matches = nameParts match {
//          case qualifier +: name +: nestedFields =>
//            val key = (qualifier.toLowerCase(Locale.ROOT), name.toLowerCase(Locale.ROOT))
//            val attributes = collectMatches(name, qualified.get(key)).filter { a =>
//              resolver(qualifier, a.qualifier.last)
//            }
//            (attributes, nestedFields)
//          case all =>
//            (Seq.empty[Attribute], Seq.empty[String])
//        }
//      }
//
//      // If none of attributes match database.table.column pattern or
//      // `table.column` pattern, we try to resolve it as a column.
//      val (candidates, nestedFields) = matches match {
//        case (Seq(), _) =>
//          val name = nameParts.head
//          val attributes = collectMatches(name, direct.get(name.toLowerCase(Locale.ROOT)))
//          (attributes, nameParts.tail)
//        case _ => matches
//      }
//
//      def name = UnresolvedAttribute(nameParts).name
//      candidates match {
//        case Seq(a) if nestedFields.nonEmpty =>
//          // One match, but we also need to extract the requested nested field.
//          // The foldLeft adds ExtractValues for every remaining parts of the identifier,
//          // and aliased it with the last part of the name.
//          // For example, consider "a.b.c", where "a" is resolved to an existing attribute.
//          // Then this will add ExtractValue("c", ExtractValue("b", a)), and alias the final
//          // expression as "c".
//          val fieldExprs = nestedFields.foldLeft(a: Expression) { (e, name) =>
//            ExtractValue(e, Literal(name), resolver)
//          }
//          Some(Alias(fieldExprs, nestedFields.last)())
//
//        case Seq(a) =>
//          // One match, no nested fields, use it.
//          Some(a)
//
//        case Seq() =>
//          // No matches.
//          None
//
//        case ambiguousReferences =>
//          // More than one match.
//          val referenceNames = ambiguousReferences.map(_.qualifiedName).mkString(", ")
//          throw new AnalysisException(s"Reference '$name' is ambiguous, could be: $referenceNames.")
//      }
//    }
  }

}
