package org.apache.spark.secco.optimization.util

import cern.colt.matrix.impl.DenseDoubleMatrix2D
import com.joptimizer.optimizers.{LPOptimizationRequest, LPPrimalDualMethod}

import scala.math.BigDecimal.RoundingMode

/** A helper class for computing the width (fractional edge cover number) of the graph. */
object FractionalEdgeCoverNumberCalculator {

  /** Compute the fractional width of the graph. */
  def width[V <: Node, E <: Edge[V]](g: Graph[V, E]) = {

    val edges = g.edges
    val nodes = g.nodes

    val c = edges.map(e => 1.0).toArray
    val G = new DenseDoubleMatrix2D(nodes.size, c.size)

    for (i <- 0 until nodes.size) {
      for (j <- 0 until edges.size) {
        if (edges(j).nodeSet.contains(nodes(i))) {
          G.set(i, j, -1.0)
        } else {
          G.set(i, j, 0.0)
        }
      }
    }

    val h = Array.fill(nodes.size)(-1.0)
    val lb = Array.fill(c.size)(0.0)
    val ub = Array.fill(c.size)(1.01)

    val or = new LPOptimizationRequest

    val opt = new LPPrimalDualMethod
    or.setC(c)
    or.setG(G)
    or.setH(h)
    or.setLb(lb)
    or.setUb(ub)
    or.setDumpProblem(false)
    opt.setLPOptimizationRequest(or)
    opt.optimize()

    val rawsol = opt.getOptimizationResponse.getSolution

    val sol = rawsol.map(f =>
      BigDecimal.valueOf(f).setScale(3, RoundingMode.HALF_EVEN).doubleValue()
    )

    sol.sum
  }
}
