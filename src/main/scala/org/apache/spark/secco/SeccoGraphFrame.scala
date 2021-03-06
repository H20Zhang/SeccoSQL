package org.apache.spark.secco

import org.apache.spark.secco.analysis.{
  UnresolvedAttribute,
  UnresolvedPattern,
  UnresolvedSubgraphQuery
}
import org.apache.spark.secco.execution.QueryExecution
import org.apache.spark.secco.expression.NamedExpression
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.optimization.plan.{
  EdgeRelation,
  Graph,
  GraphRelation,
  MessagePassing,
  NodeRelation,
  Recursion
}
import org.apache.spark.secco.types.{DataTypes, StructField, StructType}
import org.apache.spark.secco.util.counter.CounterManager

import scala.util.Try

/** A GraphFrame stores a graph using graph relation.
  *
  * For graph relation, logically, it is a relation of form
  *
  *   (src_id, dst_id, src_node_label, dst_node_label, edge_label, src_node_properties, dst_node_properties, edge_properties)
  *
  * Physically, it is consists of node relation and edge relation.
  *   The node relation specifies the id, node_label, and node properties of the graph.
  *   The edge relation specifies the src_id, dst_id, edge-label, and edge properties of the graph.
  *
  * Both relational operators (inherit from Dataframe) and graph operators can be applied on it.
  *
  * The additional graph operators are:
  *  - node: return a dataframe that stores nodes of the graph.
  *  - edge: return a dataframe that stores edges of the graph.
  *  - subgraph: return a subgraph of the graph.
  *  - pattern(p): return a dataframe that stores subgraph matching results of p on the graph.
  *  - messagePassing(msg, mergeFunc, updateFunc): perform graph analysis on the graph and return a dataframe that
  *    represents the results.
  */
class SeccoGraphFrame(
    @transient seccoSession: SeccoSession,
    @transient queryExecution: QueryExecution
) extends SeccoDataFrame(seccoSession, queryExecution) {

  /* == graph operations == */

  /** Return nodes of the graph. */
  def node(): SeccoDataFrame = ???

  /** Return edges of the graph. */
  def edge(): SeccoDataFrame = ???

  /** Return a new graph that consists of filtered nodes and filtered edges.
    *
    * @param vFilter predicates to filter the nodes
    * @param eFilter predicates to filter the edges
    * @return a new [[SeccoGraphFrame]]
    */
  def subgraph(
      vFilter: Option[String],
      eFilter: Option[String]
  ): SeccoGraphFrame =
    ???

  /** Perform subgraph matching on the graph using query p.
    *
    * The syntax of query p is similar to syntax of the  `Match` clause in Cypher.
    *
    * For example, it is of following forms: //TODO: make the explanation easier to understand.
    *
    * (a:NodeLabel:..., {key1:value1, ...})->[b:EdgeLabel:..., {key2:value2, ...}]->()->[]->...
    *
    * @param p the pattern of the subgraph to match
    * @return a [[SeccoDataFrame]] that contains the matching results.
    */
  def pattern(p: String) = {
    val patternExpression = Try {
      seccoSession.sessionState.sqlParser
        .parsePatternExpression(p)
        .asInstanceOf[UnresolvedPattern]
    }.getOrElse(throw new Exception(s"${p} is invalid cypher pattern"))

    SeccoDataFrame(
      seccoSession,
      UnresolvedSubgraphQuery(
        queryExecution.logical.asInstanceOf[GraphRelation],
        patternExpression
      )
    )
  }

  /** Perform an single pass of message passing.
    *
    * @param message messages generated per edge [available attributes: state, vLabel, eLabel, vProperties, eProperties].
    * @param mergeFunction function to merge the messages together [available attributes: newState]
    * @param updateFunction function to update the state of the nodes [available attributes: state, newState]
    * @param initialMessage initial state of the nodes [available attributes: vLabel, vProperties]
    * @return a [[SeccoDataFrame]] that contains states of the nodes.
    */
  def messagePassing(
      message: String,
      mergeFunction: String,
      updateFunction: String,
      initialMessage: Option[String] = None,
      round: Int = 1
  ): SeccoDataFrame = {

    val parser = seccoSession.sessionState.sqlParser

    assert(
      queryExecution.logical.isInstanceOf[Graph],
      "`messagePassing` could only be used on Graph-Like Dataset."
    )

    assert(round >= 1, "number of round should be greater or equal than 1.")

    val msgPassing = MessagePassing(
      queryExecution.logical.asInstanceOf[LogicalPlan with Graph],
      initialMessage.map(msg =>
        parser.parseNamedExpression(msg).asInstanceOf[NamedExpression]
      ),
      parser.parseNamedExpression(message).asInstanceOf[NamedExpression],
      parser
        .parseNamedExpression(mergeFunction)
        .asInstanceOf[NamedExpression],
      parser
        .parseNamedExpression(updateFunction)
        .asInstanceOf[NamedExpression]
    )

    if (round == 1) {
      SeccoDataFrame(
        seccoSession,
        msgPassing
      )
    } else {
      SeccoDataFrame(
        seccoSession,
        Recursion(msgPassing, round)
      )
    }
  }
}

object SeccoGraphFrame {

  case class NodeMetaData(
      id: String = "id",
      vLabel: Option[String] = None,
      properties: Seq[String] = Seq()
  )

  case class EdgeMetaData(
      src: String = "src",
      dst: String = "dst",
      eLabel: Option[String] = None,
      edgeProperties: Seq[String] = Seq()
  )

  /** Create an instance of [[SeccoGraphFrame]].
    *
    * @param seccoSession   the [[SeccoSession]] to create the dataset.
    * @param queryExecution the query execution of the [[SeccoGraphFrame]]
    * @return a new [[SeccoGraphFrame]]
    */
  def apply(
      seccoSession: SeccoSession,
      queryExecution: QueryExecution
  ): SeccoGraphFrame = new SeccoGraphFrame(seccoSession, queryExecution)

  /** Create an instance of [[SeccoDataFrame]]
    *
    * @param seSession the [[SeccoSession]] to create the dataset
    * @param logicalPlan    the logical plan of the dataset
    * @return a new [[SeccoDataFrame]]
    */
  def apply(
      seSession: SeccoSession,
      logicalPlan: LogicalPlan
  ): SeccoGraphFrame = {
    assert(
      logicalPlan.isInstanceOf[GraphRelation],
      s"logicalPlan:${logicalPlan} is not GraphRelation, and cannot be used to instantiate GraphFrame"
    )
    val qe = new QueryExecution(seSession, logicalPlan)
    new SeccoGraphFrame(seSession, qe)
  }

  /** Create an instance of [[SeccoDataFrame]] using a node table and an edge table.
    *
    * @param edgeDS the edge table of the graph
    * @param nodeDS the node table of the graph
    * @param nodeMetaData the meta data for node table
    * @param edgeMetaData the meta data for edge table
    * @return
    */
  def apply(
      nodeDS: SeccoDataFrame,
      nodeMetaData: NodeMetaData,
      edgeDS: SeccoDataFrame,
      edgeMetaData: EdgeMetaData
  ): SeccoGraphFrame = {

    val edge = EdgeRelation(
      edgeDS.queryExecution.logical,
      UnresolvedAttribute(edgeMetaData.src :: Nil),
      UnresolvedAttribute(edgeMetaData.dst :: Nil),
      edgeMetaData.eLabel.map(f => UnresolvedAttribute(f :: Nil)),
      edgeMetaData.edgeProperties.map(f => UnresolvedAttribute(f :: Nil))
    )

    val node = NodeRelation(
      nodeDS.queryExecution.logical,
      UnresolvedAttribute(nodeMetaData.id :: Nil),
      nodeMetaData.vLabel.map(f => UnresolvedAttribute(f :: Nil)),
      nodeMetaData.properties.map(f => UnresolvedAttribute(f :: Nil))
    )

    assert(
      nodeDS.seccoSession.eq(edgeDS.seccoSession),
      "The seccoSession of nodeDS and edgeDS should be the same."
    )

    SeccoGraphFrame(
      edgeDS.seccoSession,
      GraphRelation(
        node,
        edge
      )
    )
  }

  /** Create an instance of [[SeccoDataFrame]] using only edge table.
    *
    * @param edgeDS the edge table of the graph
    * @param edgeMetaData the meta data for edge table
    * @return
    */
  def apply(
      edgeDS: SeccoDataFrame,
      edgeMetaData: EdgeMetaData
  ): SeccoGraphFrame = {

    // create the nodeDS from edge
    val nodeDS = edgeDS
      .select(s"${edgeMetaData.src}")
      .unionAll(edgeDS.select(s"${edgeMetaData.dst}"))
    val nodeMetaData = NodeMetaData()

    SeccoGraphFrame(nodeDS, nodeMetaData, edgeDS, edgeMetaData)
  }

  /** Create an instance of [[SeccoDataFrame]] using only node table.
    *
    * @param nodeDS the node table of the graph
    * @param nodeMetaData the meta data for node table
    * @return
    */
  def apply(
      nodeDS: SeccoDataFrame,
      nodeMetaData: NodeMetaData
  ): SeccoGraphFrame = {

    val ECounter =
      CounterManager.globalCounterManager.getOrCreateCounter("TemporaryEdge")
    ECounter.increment()

    // create empty edge table.
    val edgeDS = SeccoDataFrame.empty(
      StructType(
        Seq(
          StructField("src", DataTypes.LongType),
          StructField("dst", DataTypes.LongType)
        )
      )
    )

    val edgeMetaData = EdgeMetaData()

    SeccoGraphFrame(nodeDS, nodeMetaData, edgeDS, edgeMetaData)
  }
}
