# SeccoSQL v.0.1

SeccoSQL (**Se**parate **c**ommunication from **co**mputation) is an experimental distributed SQL engine on **Spark** for processing complex SQL/Graph queries. It adopts the new communication/computation separated optimization framework proposed in "Parallel Query Processing: To Separate Communication from Computation", and many other state-of-the-art query optimization/execution techniques (e.g., GHD-based join optimization, multi-way shuffle, multi-way join, etc).



## Table of Contents

- Overview
- QuickStart
- Tutorial
- Dependency
- Reference
- Reproducibility
- Contact



## Overview

(TODO)

SeccoSQL is a new distributed analytical database for data beyond Table, such as Graph and Matrix. A brief overview is as follows.

In modern analytic pipeline, most workflows involve ingestion and manipulation of multi-model data. For example, it is common for a pipeline to build a graph from existing data, likely in a relational format, then running graph pattern query or graph analytic algorithms on it, likely to extract features for later machine learning, then apply machine learning algorithm on the extracted graph and features. In exsiting system,  such pipeline usually requires colloboration of multiple systems, which results in complexity in maintainece, high cost of transferring data between systems, and loss of inter-system query optimizations opportunities. 

To solve above problem while maintaining the flexibilty of different data model.The design fo SeccoSQL adopt a two layer principle. In front-end, it maintains flexibility and strength of each data model,  which means using Table model for SQL query, using Graph model for graph pattern query and graph analytic query, using Matrix Model for LA (Linear Algebra) and machine learning, by providing domain-specific API for each data model, such that developer can use the best tool for best dishes. In the backend, the storage, execution, optimization of Relational Data, Graph Data, and Matrix Data is unified. In other word, we adopt (1) a unified storage for Relational Data, Graph Data, and Matrix Data, a unfied (IR) intermediate representation form for SQL query, Graph Query, and LA query, (2) a unfied optimizer to optimize IR such that cross model queries can be optimized, (3) a machine learning based cost estimator for estiming the cost of query plan consists of IR, and (4) a unified query executor for executing IR such that no specialized operator for each data model is needed.

To achive such goal, we relies on a set of new techniques that is proposed by the community and our own. The detail is listed below.



## QuickStart

(TODO)

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

## Tutorial

(TODO)

## Dependency 

(TODO)



## Reference

(TODO)

We give a reference list of new query optimization and query execution techniques implemented in SeccoSQL.

### Query Optimization

Communication/Computation Separated Optimization Framework

GHD(Generalized HyperTree Decomposition)-Based Join Optimization

Aggregation Push-Down over GHD

### Query Execution

Worst-case Optimal Join

Cached LeapFrog Join

HyperCube Shuffle



## Reproducibility

The SeccoSQL in this repo may differs in terms of performance reported in the paper "Parallel Query Processing: To Separate Communication from Computation". For reproducing the results reported in the paper, please use the original code, which is avaiable at [secco-sigmod](../SIGMOD). The guide to reproduce the results is at [Reproducibility](../tutorial/Reproducibility )

## Contact

[Hao Zhang](hzhang@se.cuhk.edu.hk)
