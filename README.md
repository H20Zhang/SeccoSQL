# SeccoSQL v.0.1-alpha

SeccoSQL (**Se**parate **c**ommunication from **co**mputation) is an experimental distributed SQL engine on **Spark** for processing complex SQL/Graph queries. It adopts the new communication/computation separated optimization framework proposed in "Parallel Query Processing: To Separate Communication from Computation (SIGMOD 2022)", and many other state-of-the-art query optimization/execution techniques (e.g., GHD-based join optimization, multi-way shuffle, multi-way join, etc).



## Table of Contents

- Overview
- QuickStart
- Dependency
- Installation
- Reference
- Contact



## Overview

SeccoSQL is a new distributed SQL engine for processing complex SQL/Graph queries. A full description is in our [manuscript](https://dl.acm.org/doi/10.1145/3514221.3526164). A brief overview is as follows.

In the existing SQL engine , it optimizes an SQL query by arranging relational algebra operators to reduce the total cost, where, for each operator, it involves (i) distribution of data partitioned to computing nodes by communication, and (ii) computation on computing nodes locally. That kinds of parallelization is also called [intra-operator parallelism](https://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf), where the communication and computation are dealt with inside an operator and are not separable. It is worth noting that the optimizer reason about communication and computation implicitly, and it is difficult to avoid large intermediate results and hence reduce the communication cost. 

In SeccoSQL, we deal with communication and computation explicitly. To do so, we separate communication from computation using several new operators proposed in this paper. 

1. pair operator (⊗): pair the partitions of a relation 𝑅 with the partitions of a relation 𝑆, where a partition is specified by a hash function. 
2. merge operator ($\tilde{\cup}$): collect all partial results from computing nodes as they are.
3. local computation operator ($\tilde{op}$):  does local computation for pairs on a computing node as $\tilde{op}$ for any RA (relational algebra) operator op.

With the pair operator defined, we can explicitly deal with communication to deliver pairs of partitions to computing nodes. With local computation defined, we can perform local computation on pairs. 

In short, with pair, merge and local computation, we are able to explicitly specify communication and computation for RA operators. And, with communication and computation being explicitly specified, we make it possible to “move” the communication and computation inside the RA plan in a very flexible way.

Beyond that SeccoSQL leverages latest research on query optimization and query execution, such as [GHD-based join optimization](https://www.google.com.hk/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwjh2o-_xbH4AhW4jdgFHUawD9YQFnoECAcQAQ&url=https%3A%2F%2Farxiv.org%2Fabs%2F1503.02368&usg=AOvVaw0Wv9Gm97hvKV3BRw80ppQd), [Multiway Shuffle](https://ieeexplore.ieee.org/document/5710932),  [Multiway Join](https://arxiv.org/abs/1203.1952), etc.

SeccoSQL is built on Spark and is designed to run as a library just like SparkSQL, but intended for complex SQL/Graph queries, where SparkSQL cannot perform well. Behind the scenes, SeccoSQL have a very similar structure to SparkSQL. 

1. Parser: able to parse SQL, Cypher, DataFrame queries
2. Expression: deal with expression
3. Catalog: records meta data
4. Codegen: do the code generation
5. Optimizer: 
6. Planner: translate logical plan into optimized physical plan
7. Storage

SeccoSQL support multiple ways to query. It support SQL and Cypher to query the data in a declarive ways. Also, it support DataFrame, which allow user to construct the query gradually. Note SeccoSQL is mainly used as a experimental platform for testing new ideas, the languages support for SQL and Cypher are limited. Only basic SQL and Cypher support are expected. With the parser query, SeccoSQL optimizes query in two steps, it first optimize the query as an RA expression, then on top of that, it perform the separation, and does the reordering of communication and computation to further optimize the query.

**Note: SeccoSQL is very unstable and contains many bugs right now. It will improve gradually. For benchmark purpose, you can check [Secco-SIGMOD](https://github.com/H20Zhang/Secco-SIGMOD), which is the Secco version that with limited functionality but can be used for benchmarking.**

## QuickStart

The main object in Secco to manipulate is `Dataset`, which just like the `Dataset` in `SparkSQL`. In `Dataset`, it defines relational algebra operators (e.g., select, project, join) that transforms the dataset.

The main entry of Secco is SeccoSession, where you can create the `Dataset` , register `Dataset` in `Catalog`, get `Dataset` from `Catalog`, and issuse `SQL` query.

An example is shown below.

```scala
// Obtain SeccoSession via singleton.
    val dlSession = SeccoSession.currentSession

    // Create datasets.
    val seq1 = Seq(Array(1.0, 2.0), Array(2.0, 2.0))
    val tableName = "R1"
    val schema = Seq("A", "B")
    val ds1 =
      dlSession.createDatasetFromSeq(seq1, Some(tableName), Some(schema))

    // Construct RA expression via relational algebra like API.
    val ds2 = ds1.select("A < B")

    // Explain the query execution of ds1 and ds2. It will show parsed plan, analyzed plan, optimized plan, execution plan.
    ds1.explain()
    ds2.explain()
```

For more usage, please check class  `org.apache.spark.secco.SeccoSession` and `org.apache.spark.secco.Dataset`, there contains comments for guiding you using the system.  We recommand you using the `Dataset` api instead of `SQL` api, as it currently have some bugs, and we disable it for now.

## Dependency

The dependency are handled by the build.sbt in SeccoSQL. If you want to add SeccoSQL to your own project, you may need to include following lines to your SBT file.

```
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7" % "provided"
//libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"
// Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3" % "test"
// Util
libraryDependencies += "net.sf.trove4j" % "trove4j" % "3.0.3"
libraryDependencies += "it.unimi.dsi" % "fastutil" % "8.1.0"
// Math Optimizer
libraryDependencies += "com.joptimizer" % "joptimizer" % "5.0.0"
// Graph Processing
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.3.0"
// Args Parsing
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
// Configuration
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
// Better Printing
libraryDependencies += "com.lihaoyi" %% "pprint" % "0.5.4"
// Metering
libraryDependencies += "com.storm-enroute" %% "scalameter-core" % "0.7"
```

## Installation

Currently, you can download the project, add the SeccoSQL to your project, and add dependency in your SBT file.

In the future, SeccoSQL will be published to maven, which allows you do import SeccoSQL by one line such as `libraryDependencies += "secco" %% "SeccoSQL" % "0.1-alpha"`

## Reference

We give a reference list of new query optimization and query execution techniques implemented in SeccoSQL.

### Query Optimization

[Communication/Computation Separated Optimization Framework](https://dl.acm.org/doi/10.1145/3514221.3526164)

[GHD(Generalized HyperTree Decomposition)-Based Join Optimization](https://www.google.com.hk/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&ved=2ahUKEwjh2o-_xbH4AhW4jdgFHUawD9YQFnoECAcQAQ&url=https%3A%2F%2Farxiv.org%2Fabs%2F1503.02368&usg=AOvVaw0Wv9Gm97hvKV3BRw80ppQd)

[Aggregation Push-Down over GHD](https://arxiv.org/abs/1508.07532)

### Query Execution

[Worst-case Optimal Join](https://arxiv.org/abs/1203.1952)

[Cached LeapFrog Join](https://arxiv.org/abs/1602.08721)

[HyperCube Shuffle](https://ieeexplore.ieee.org/document/5710932)



## Road Map

1. Fix bugs in the optimizer when handling equi-joins
2. Fix the bugs in codegen
3. Publish SeccoSQL to maven

## Contact

[Hao Zhang](hzhang@se.cuhk.edu.hk)
