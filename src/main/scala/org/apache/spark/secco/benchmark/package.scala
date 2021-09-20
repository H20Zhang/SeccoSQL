package org.apache.spark.secco

package object benchmark {

  object Method extends Enumeration {
    type Method = Value
    val GraphFrame, SparkSQL, GraphX, DBX, Secco = Value
  }

  object Query extends Enumeration {
    type Query = Value

    /**
      * S: subgraph query
      * C: complex subgraph query
      * O: OLAP query
      * I: iterative graph query
      * GxIx: graph generation query with graph analytic.
      */
    val S1, S2, S3, S4, C1, C2, C3, C4, O1, O2, O3, O4, O5, O6, O7, O8, O9, O10,
        O11, O12, I1, I2, I3, G1I1, G1I2, G1I3, G2I1, G2I2, G2I3, DEBUG,
        DEBUG_I, DEBUG1, DEBUG2, S1New, S2New, S3New, S4New, DEBUG3 =
      Value
  }

}
