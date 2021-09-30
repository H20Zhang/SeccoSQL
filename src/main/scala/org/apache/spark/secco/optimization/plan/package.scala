package org.apache.spark.secco.optimization

package object plan {
  object JoinType extends Enumeration {
    type JoinType = Value
    val Inner, // inner join between two relations
    LeftOuter, // left outer join between two relations
    RightOuter, // right outer join between two relations
    FullOuter, // full outer join between two relations
    Natural, // natural join between relations (possibly more than 2)
    GHD, // natural join inside GHD node
    PKFK, // primary key foreign key natural join
    FKFK, // foreign key foreign key natural join
    GHDFKFK //this join types are subType of natural join
    = Value
  }

}
