//package org.apache.spark.secco.benchmark.testcases
//
//import org.apache.spark.secco.Dataset
//import org.apache.spark.secco.benchmark.GraphAnalyticBenchmark
//import org.apache.spark.secco.benchmark.testcases.I2.dlSession
//import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
//
///* == Graph Analytic Queries == */
//
///** PageRank Query */
//object I1 extends GraphAnalyticBenchmark {
//
//  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "G",
//        Seq(CatalogColumn("src"), CatalogColumn("dst"))
//      ) -> "directed"
//    )
//
//  /** preprocess the data */
//  override protected def preprocess(
//      inputData: Map[String, Dataset]
//  ): Map[String, Dataset] = {
//    val g = inputData("G")
//
//    val degreeRDD = g
//      .rdd()
//      .map(f => (f(1), 1.0))
//      .reduceByKey(_ + _)
//      .map(f => Array(f._1, f._2))
//      .cache()
//
//    val weightRDD = degreeRDD.map(f => Array(f(0), 1.0)).cache()
//
//    val degreeDS = dlSession.createDatasetFromRDD(
//      degreeRDD,
//      Some("Degree"),
//      Some(Seq("dst", "degree")),
//      Some(Seq("dst"))
//    )
//    val weightDS =
//      dlSession.createDatasetFromRDD(
//        weightRDD,
//        Some("W"),
//        Some(Seq("dst", "weight")),
//        Some(Seq("dst"))
//      )
//
//    Map(
//      "G" -> g,
//      "W" -> weightDS,
//      "Degree" -> degreeDS
//    )
//  }
//
//  /** generate the [[Dataset]] */
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val g = inputData("G")
//    val W = inputData("W")
//    val Degree = inputData("Degree")
//
//    W.join(Degree)
//      .transform("dst, weight*degree")
//      .alias("dst, weight1")
//      .naturalJoin(g)
//      .aggregate("sum(weight1) by src")
//      .alias("src, weight2")
//      .transform("src, 0.85*weight2+0.15")
//      .assign("W")
//      .withRecursive("W")
//  }
//}
//
///** Weakly Connected Component Query */
//object I2 extends GraphAnalyticBenchmark {
//
//  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "G",
//        Seq(CatalogColumn("src"), CatalogColumn("dst"))
//      ) -> "directed"
//    )
//
//  /** preprocess the data */
//  override protected def preprocess(
//      inputData: Map[String, Dataset]
//  ): Map[String, Dataset] = {
//
//    val g = inputData("G")
//
//    val deltaWRDD = g
//      .rdd()
//      .map(f => (f(0)))
//      .distinct()
//      .map(f => Array(f, f))
//      .cache()
//
//    val deltaWDS = dlSession.createDatasetFromRDD(
//      deltaWRDD,
//      Some("DeltaW"),
//      Some(Seq("dst", "weight")),
//      Some(Seq("dst"))
//    )
//
//    Map(
//      "G" -> g,
//      "DeltaW" -> deltaWDS
//    )
//  }
//
//  /** generate the [[Dataset]] */
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val g = inputData("G")
//    val DeltaW = inputData("DeltaW")
//
//    //create relation W in the catalog
//    val W = dlSession.createEmptyDataset("W", Seq("src", "weight"))
//
//    DeltaW
//      .join(g)
//      .aggregate("min(weight) by src")
//      .alias("src, weight")
//      .update("W", "DeltaW", Seq("src"))
//      .withRecursive("W")
//  }
//}
//
///** Single Source Shortest Path Query */
//object I3 extends GraphAnalyticBenchmark {
//
//  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "G",
//        Seq(CatalogColumn("src"), CatalogColumn("dst"))
//      ) -> "directed"
//    )
//
//  /** preprocess the data */
//  override protected def preprocess(
//      inputData: Map[String, Dataset]
//  ): Map[String, Dataset] = {
//
//    val sc = dlSession.sparkContext
//
//    val g = inputData("G")
//
//    val deltaWRDD =
//      sc
//        .parallelize(Seq(dlSession.sessionState.conf.landmark))
//        .map(f => Array(f, 0.0))
//        .cache()
//
//    val deltaWDS = dlSession.createDatasetFromRDD(
//      deltaWRDD,
//      Some("DeltaW"),
//      Some(Seq("dst", "weight")),
//      Some(Seq("dst"))
//    )
//
//    Map(
//      "G" -> g,
//      "DeltaW" -> deltaWDS
//    )
//  }
//
//  /** generate the [[Dataset]] */
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val g = inputData("G")
//    val DeltaW = inputData("DeltaW")
//
//    //create relation W in the catalog
//    val W = dlSession.createEmptyDataset("W", Seq("src", "weight"))
//
//    DeltaW
//      .join(g)
//      .transform("src, weight+1.0")
//      .alias("src, weight2")
//      .aggregate("min(weight2) by src")
//      .alias("src, weight")
//      .update("W", "DeltaW", Seq("src"))
//      .withRecursive("W")
//  }
//}
//
//object G1I1 extends GraphAnalyticBenchmark {
//
//  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "G1",
//        Seq(CatalogColumn("src"), CatalogColumn("mid"))
//      ) -> "directed",
//      CatalogTable(
//        "G2",
//        Seq(CatalogColumn("mid"), CatalogColumn("dst"))
//      ) -> "directed"
//    )
//
//  /** preprocess the data */
//  override protected def preprocess(
//      inputData: Map[String, Dataset]
//  ): Map[String, Dataset] = {
//    val g = inputData("G1")
//
//    val degreeRDD = g
//      .rdd()
//      .map(f => (f(1), 1.0))
//      .reduceByKey(_ + _)
//      .map(f => Array(f._1, f._2))
//      .cache()
//
//    val weightRDD = degreeRDD.map(f => Array(f(0), 1.0)).cache()
//
//    val degreeDS = dlSession.createDatasetFromRDD(
//      degreeRDD,
//      Some("Degree"),
//      Some(Seq("dst", "degree")),
//      Some(Seq("dst"))
//    )
//    val weightDS =
//      dlSession.createDatasetFromRDD(
//        weightRDD,
//        Some("W"),
//        Some(Seq("dst", "weight")),
//        Some(Seq("dst"))
//      )
//
//    inputData ++ Seq("W" -> weightDS, "Degree" -> degreeDS)
//  }
//
//  /** generate the [[Dataset]] */
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val g1 = inputData("G1")
//    val g2 = inputData("G2")
//    val g = g1.join(g2)
//    val W = inputData("W")
//    val Degree = inputData("Degree")
//
//    W.join(Degree)
//      .transform("dst, weight*degree")
//      .alias("dst, weight1")
//      .naturalJoin(g)
//      .aggregate("sum(weight1) by src")
//      .alias("src, weight2")
//      .transform("src, 0.85*weight2+0.15")
//      .assign("W")
//      .withRecursive("W")
//  }
//}
//
//object G2I1 extends GraphAnalyticBenchmark {
//
//  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "G1",
//        Seq(CatalogColumn("src"), CatalogColumn("mid"))
//      ) -> "directed",
//      CatalogTable(
//        "G2",
//        Seq(CatalogColumn("mid"), CatalogColumn("dst"))
//      ) -> "directed"
//    )
//
//  /** preprocess the data */
//  override protected def preprocess(
//      inputData: Map[String, Dataset]
//  ): Map[String, Dataset] = {
//    val g = inputData("G1")
//
//    val degreeRDD = g
//      .rdd()
//      .map(f => (f(1), 1.0))
//      .reduceByKey(_ + _)
//      .map(f => Array(f._1, f._2))
//      .cache()
//
//    val weightRDD = degreeRDD.map(f => Array(f(0), 1.0)).cache()
//
//    val degreeDS = dlSession.createDatasetFromRDD(
//      degreeRDD,
//      Some("Degree"),
//      Some(Seq("dst", "degree")),
//      Some(Seq("dst"))
//    )
//    val weightDS =
//      dlSession.createDatasetFromRDD(
//        weightRDD,
//        Some("W"),
//        Some(Seq("dst", "weight")),
//        Some(Seq("dst"))
//      )
//
//    inputData ++ Seq("W" -> weightDS, "Degree" -> degreeDS)
//  }
//
//  /** generate the [[Dataset]] */
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val g1 = inputData("G1")
//    val g2 = inputData("G2")
//    val g = g1.join(g2).project("src, dst")
//    val W = inputData("W")
//    val Degree = inputData("Degree")
//
//    W.join(Degree)
//      .transform("dst, weight*degree")
//      .alias("dst, weight1")
//      .naturalJoin(g)
//      .aggregate("sum(weight1) by src")
//      .alias("src, weight2")
//      .transform("src, 0.85*weight2+0.15")
//      .assign("W")
//      .withRecursive("W")
//  }
//}
//
//object G1I2 extends GraphAnalyticBenchmark {
//
//  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "G1",
//        Seq(CatalogColumn("src"), CatalogColumn("mid"))
//      ) -> "directed",
//      CatalogTable(
//        "G2",
//        Seq(CatalogColumn("mid"), CatalogColumn("dst"))
//      ) -> "directed"
//    )
//
//  /** preprocess the data */
//  override protected def preprocess(
//      inputData: Map[String, Dataset]
//  ): Map[String, Dataset] = {
//
//    val g = inputData("G1")
//
//    val deltaWRDD = g
//      .rdd()
//      .map(f => (f(0)))
//      .distinct()
//      .map(f => Array(f, f))
//      .cache()
//
//    val deltaWDS = dlSession.createDatasetFromRDD(
//      deltaWRDD,
//      Some("DeltaW"),
//      Some(Seq("dst", "weight")),
//      Some(Seq("dst"))
//    )
//
//    inputData ++ Seq("DeltaW" -> deltaWDS)
//  }
//
//  /** generate the [[Dataset]] */
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val g1 = inputData("G1")
//    val g2 = inputData("G2")
//    val g = g1.join(g2)
//    val DeltaW = inputData("DeltaW")
//    //create relation W in the catalog
//    val W = dlSession.createEmptyDataset("W", Seq("src", "weight"))
//
//    DeltaW
//      .join(g)
//      .aggregate("min(weight) by src")
//      .alias("src, weight")
//      .update("W", "DeltaW", Seq("src"))
//      .withRecursive("W")
//  }
//}
//
//object G2I2 extends GraphAnalyticBenchmark {
//
//  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "G1",
//        Seq(CatalogColumn("src"), CatalogColumn("mid"))
//      ) -> "directed",
//      CatalogTable(
//        "G2",
//        Seq(CatalogColumn("mid"), CatalogColumn("dst"))
//      ) -> "directed"
//    )
//
//  /** preprocess the data */
//  override protected def preprocess(
//      inputData: Map[String, Dataset]
//  ): Map[String, Dataset] = {
//
//    val g = inputData("G1")
//
//    val deltaWRDD = g
//      .rdd()
//      .map(f => (f(0)))
//      .distinct()
//      .map(f => Array(f, f))
//      .cache()
//
//    val deltaWDS = dlSession.createDatasetFromRDD(
//      deltaWRDD,
//      Some("DeltaW"),
//      Some(Seq("dst", "weight")),
//      Some(Seq("dst"))
//    )
//
//    inputData ++ Seq("DeltaW" -> deltaWDS)
//  }
//
//  /** generate the [[Dataset]] */
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val g1 = inputData("G1")
//    val g2 = inputData("G2")
//    val g = g1.join(g2).project("src, dst")
//    val DeltaW = inputData("DeltaW")
//    //create relation W in the catalog
//    val W = dlSession.createEmptyDataset("W", Seq("src", "weight"))
//
//    DeltaW
//      .join(g)
//      .aggregate("min(weight) by src")
//      .alias("src, weight")
//      .update("W", "DeltaW", Seq("src"))
//      .withRecursive("W")
//  }
//}
//
//object G1I3 extends GraphAnalyticBenchmark {
//
//  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "G1",
//        Seq(CatalogColumn("src"), CatalogColumn("mid"))
//      ) -> "directed",
//      CatalogTable(
//        "G2",
//        Seq(CatalogColumn("mid"), CatalogColumn("dst"))
//      ) -> "directed"
//    )
//
//  /** preprocess the data */
//  override protected def preprocess(
//      inputData: Map[String, Dataset]
//  ): Map[String, Dataset] = {
//
//    val sc = dlSession.sparkContext
//
//    val g = inputData("G1")
//
//    val deltaWRDD =
//      sc
//        .parallelize(Seq(dlSession.sessionState.conf.landmark))
//        .map(f => Array(f, 0.0))
//        .cache()
//
//    val deltaWDS = dlSession.createDatasetFromRDD(
//      deltaWRDD,
//      Some("DeltaW"),
//      Some(Seq("dst", "weight")),
//      Some(Seq("dst"))
//    )
//
//    inputData ++ Seq("DeltaW" -> deltaWDS)
//  }
//
//  /** generate the [[Dataset]] */
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val g1 = inputData("G1")
//    val g2 = inputData("G2")
//    val g = g1.join(g2)
//    val DeltaW = inputData("DeltaW")
//    //create relation W in the catalog
//    val W = dlSession.createEmptyDataset("W", Seq("src", "weight"))
//
//    DeltaW
//      .join(g)
//      .transform("src, weight+1.0")
//      .alias("src, weight2")
//      .aggregate("min(weight2) by src")
//      .alias("src, weight")
//      .update("W", "DeltaW", Seq("src"))
//      .withRecursive("W")
//  }
//}
//
//object G2I3 extends GraphAnalyticBenchmark {
//
//  /** Returns a Map that contains schemas of relation to be loaded, and their data's relative address */
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "G1",
//        Seq(CatalogColumn("src"), CatalogColumn("mid"))
//      ) -> "directed",
//      CatalogTable(
//        "G2",
//        Seq(CatalogColumn("mid"), CatalogColumn("dst"))
//      ) -> "directed"
//    )
//
//  /** preprocess the data */
//  override protected def preprocess(
//      inputData: Map[String, Dataset]
//  ): Map[String, Dataset] = {
//
//    val sc = dlSession.sparkContext
//
//    val g = inputData("G1")
//
//    val deltaWRDD =
//      sc
//        .parallelize(Seq(dlSession.sessionState.conf.landmark))
//        .map(f => Array(f, 0.0))
//        .cache()
//
//    val deltaWDS = dlSession.createDatasetFromRDD(
//      deltaWRDD,
//      Some("DeltaW"),
//      Some(Seq("dst", "weight")),
//      Some(Seq("dst"))
//    )
//
//    inputData ++ Seq("DeltaW" -> deltaWDS)
//  }
//
//  /** generate the [[Dataset]] */
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val g1 = inputData("G1")
//    val g2 = inputData("G2")
//    val g = g1.join(g2).project("src, dst")
//    val DeltaW = inputData("DeltaW")
//    //create relation W in the catalog
//    val W = dlSession.createEmptyDataset("W", Seq("src", "weight"))
//
//    DeltaW
//      .join(g)
//      .transform("src, weight+1.0")
//      .alias("src, weight2")
//      .aggregate("min(weight2) by src")
//      .alias("src, weight")
//      .update("W", "DeltaW", Seq("src"))
//      .withRecursive("W")
//  }
//}
