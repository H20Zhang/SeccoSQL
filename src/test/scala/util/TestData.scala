package util

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.execution.sources.DataLoader

object TestData {

  val prefix = "./datasets"

  def loadUndirectedGraphEdge(input: String) = {
    val loader = new DataLoader()
    val edge = loader
      .csv(input, "\\s")
      .filter(f => f != null)
      .map(f => (f(0), f(1)))
      .filter(f => f._1 != f._2)
      .flatMap(f => Iterator(f, f.swap))
      .distinct()
      .map(f => Array(f._1, f._2))
      .persist(SeccoSession.currentSession.sessionState.conf.rddCacheLevel)

    edge.count()

    edge
  }

  def data(dataName: String) =
    dataName match {
      case "eu"    => loadUndirectedGraphEdge(s"$prefix/eu.txt")
      case "wiki"  => loadUndirectedGraphEdge(s"$prefix/wikiV.txt")
      case "debug" => loadUndirectedGraphEdge(s"$prefix/debug.txt")
    }

}
