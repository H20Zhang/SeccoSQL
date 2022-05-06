//package org.apache.spark.secco.benchmark.testcases
//
//import org.apache.spark.secco.Dataset
//import org.apache.spark.secco.benchmark.SQLBenchmark
//import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
//
///* == SQL Queries == */
//
//object O1 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "cast_info",
//        Seq(CatalogColumn("person_id"), CatalogColumn("movie_id"))
//      ) -> "cast_info"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val cast_infoDS = inputData("cast_info")
//    cast_infoDS.aggregate("count(*) by movie_id")
//  }
//
//}
//
//object O2 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "cast_info",
//        Seq(CatalogColumn("person_id"), CatalogColumn("movie_id"))
//      ) -> "cast_info",
//      CatalogTable(
//        "title",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("movie_name")),
//        Seq(CatalogColumn("movie_id"))
//      ) -> "title"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val cast_infoDS = inputData("cast_info")
//    val titleDS = inputData("title")
//    cast_infoDS.join(titleDS)
//  }
//}
//
//object O3 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "cast_info",
//        Seq(CatalogColumn("person_id"), CatalogColumn("movie_id"))
//      ) -> "cast_info",
//      CatalogTable(
//        "title",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("movie_name")),
//        Seq(CatalogColumn("movie_id"))
//      ) -> "title"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val cast_infoDS = inputData("cast_info")
//    val titleDS = inputData("title")
//    cast_infoDS.join(titleDS).aggregate("count(*) by movie_name")
//  }
//}
//
//object O4 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "cast_info",
//        Seq(CatalogColumn("person_id"), CatalogColumn("movie_id"))
//      ) -> "cast_info",
//      CatalogTable(
//        "title",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("movie_name")),
//        Seq(CatalogColumn("movie_id"))
//      ) -> "title",
//      CatalogTable(
//        "movie_keyword",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("keyword_id"))
//      ) -> "movie_keyword"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val cast_infoDS = inputData("cast_info")
//    val titleDS = inputData("title")
//    val movie_keyword = inputData("movie_keyword")
//
//    cast_infoDS.join(titleDS).join(movie_keyword)
//  }
//}
//
//object O5 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "cast_info",
//        Seq(CatalogColumn("person_id"), CatalogColumn("movie_id"))
//      ) -> "cast_info",
//      CatalogTable(
//        "title",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("movie_name")),
//        Seq(CatalogColumn("movie_id"))
//      ) -> "title",
//      CatalogTable(
//        "movie_keyword",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("keyword_id"))
//      ) -> "movie_keyword"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val cast_infoDS = inputData("cast_info")
//    val titleDS = inputData("title")
//    val movie_keyword = inputData("movie_keyword")
//
//    cast_infoDS
//      .join(titleDS)
//      .join(movie_keyword)
//      .aggregate("count(*) by movie_name")
//  }
//}
//
//object O6 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "cast_info",
//        Seq(CatalogColumn("person_id"), CatalogColumn("movie_id"))
//      ) -> "cast_info",
//      CatalogTable(
//        "title",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("movie_name")),
//        Seq(CatalogColumn("movie_id"))
//      ) -> "title",
//      CatalogTable(
//        "movie_keyword",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("keyword_id"))
//      ) -> "movie_keyword",
//      CatalogTable(
//        "name",
//        Seq(CatalogColumn("person_id"), CatalogColumn("name")),
//        Seq(CatalogColumn("person_id"))
//      ) -> "name"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val cast_infoDS = inputData("cast_info")
//    val titleDS = inputData("title")
//    val movie_keyword = inputData("movie_keyword")
//    val name = inputData("name")
//
//    cast_infoDS
//      .join(titleDS)
//      .join(movie_keyword)
//      .join(name)
//      .aggregate("count(*) by movie_name")
//  }
//}
//
//object O7 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "title",
//        Seq(CatalogColumn("movie_id2"), CatalogColumn("movie_name")),
//        Seq(CatalogColumn("movie_id2"))
//      ) -> "title",
//      CatalogTable(
//        "movie_keyword",
//        Seq(CatalogColumn("movie_id1"), CatalogColumn("keyword_id"))
//      ) -> "movie_keyword"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val titleDS = inputData("title")
//    val movie_keyword = inputData("movie_keyword")
//
//    movie_keyword.thetaJoin("movie_id1 < movie_id2", titleDS)
//  }
//}
//
//object O8 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "title",
//        Seq(CatalogColumn("movie_id2"), CatalogColumn("movie_name")),
//        Seq(CatalogColumn("movie_id2"))
//      ) -> "title",
//      CatalogTable(
//        "movie_keyword",
//        Seq(CatalogColumn("movie_id1"), CatalogColumn("keyword_id"))
//      ) -> "movie_keyword"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val titleDS = inputData("title")
//    val movie_keyword = inputData("movie_keyword")
//
//    movie_keyword
//      .thetaJoin("movie_id1 < movie_id2", titleDS)
//      .aggregate("count(*) by movie_name")
//  }
//}
//
//object O9 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "title",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("movie_name")),
//        Seq(CatalogColumn("movie_id"))
//      ) -> "title",
//      CatalogTable(
//        "movie_keyword",
//        Seq(CatalogColumn("movie_id"), CatalogColumn("keyword_id"))
//      ) -> "movie_keyword",
//      CatalogTable(
//        "name",
//        Seq(CatalogColumn("person_id"), CatalogColumn("name"))
//      ) -> "name"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val titleDS = inputData("title")
//    val movie_keyword = inputData("movie_keyword")
//    val name = inputData("name")
//
//    movie_keyword
//      .join(titleDS)
//      .thetaJoin("person_id < movie_id", name)
//  }
//}
//
//object O10 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "cast_info1",
//        Seq(CatalogColumn("person_id"), CatalogColumn("movie_id1"))
//      ) -> "cast_info",
//      CatalogTable(
//        "cast_info2",
//        Seq(CatalogColumn("person_id"), CatalogColumn("movie_id2"))
//      ) -> "cast_info",
//      CatalogTable(
//        "title",
//        Seq(CatalogColumn("movie_id1"), CatalogColumn("movie_name")),
//        Seq(CatalogColumn("movie_id1"))
//      ) -> "title"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val cast_infoDS1 = inputData("cast_info1")
//    val cast_infoDS2 = inputData("cast_info2")
//    val titleDS = inputData("title")
//    cast_infoDS1
//      .join(cast_infoDS2)
//      .join(titleDS)
//      .aggregate("count(*) by movie_name")
//  }
//}
//
//object O11 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] =
//    Map(
//      CatalogTable(
//        "cast_info1",
//        Seq(CatalogColumn("person_id1"), CatalogColumn("movie_id1"))
//      ) -> "cast_info",
//      CatalogTable(
//        "cast_info2",
//        Seq(CatalogColumn("person_id2"), CatalogColumn("movie_id1"))
//      ) -> "cast_info",
//      CatalogTable(
//        "cast_info3",
//        Seq(CatalogColumn("person_id2"), CatalogColumn("movie_id2"))
//      ) -> "cast_info",
//      CatalogTable(
//        "name",
//        Seq(CatalogColumn("person_id1"), CatalogColumn("name")),
//        Seq(CatalogColumn("person_id1"))
//      ) -> "name"
//    )
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val cast_infoDS1 = inputData("cast_info1")
//    val cast_infoDS2 = inputData("cast_info2")
//    val cast_infoDS3 = inputData("cast_info3")
//    val name = inputData("name")
//
//    cast_infoDS1
//      .join(cast_infoDS2)
//      .join(cast_infoDS3)
//      .join(name)
//      .aggregate("count(*) by name")
//  }
//}
//
//object O12 extends SQLBenchmark {
//
//  override def relationsWithAddress: Map[CatalogTable, String] = {
//
//    val conf = dlSession.sessionState.conf
//
//    //fixme: below is a temporary fix for the bug, if delay strategy is not heuristic
//    if (conf.delayStrategy != "Heuristic") {
//      Map(
//        CatalogTable(
//          "cast_info1",
//          Seq(CatalogColumn("person_id"), CatalogColumn("movie_id1"))
//        ) -> "cast_info",
//        CatalogTable(
//          "cast_info2",
//          Seq(CatalogColumn("person_id"), CatalogColumn("movie_id2"))
//        ) -> "cast_info",
//        CatalogTable(
//          "movie_keyword1",
//          Seq(CatalogColumn("movie_id1"), CatalogColumn("keyword_id"))
//        ) -> "movie_keyword",
//        CatalogTable(
//          "movie_keyword2",
//          Seq(CatalogColumn("movie_id2"), CatalogColumn("keyword_id"))
//        ) -> "movie_keyword",
//        CatalogTable(
//          "name",
//          Seq(CatalogColumn("person_id"), CatalogColumn("name"))
//        ) -> "name"
//      )
//    } else {
//      Map(
//        CatalogTable(
//          "cast_info1",
//          Seq(CatalogColumn("person_id"), CatalogColumn("movie_id1"))
//        ) -> "cast_info",
//        CatalogTable(
//          "cast_info2",
//          Seq(CatalogColumn("person_id"), CatalogColumn("movie_id2"))
//        ) -> "cast_info",
//        CatalogTable(
//          "movie_keyword1",
//          Seq(CatalogColumn("movie_id1"), CatalogColumn("keyword_id"))
//        ) -> "movie_keyword",
//        CatalogTable(
//          "movie_keyword2",
//          Seq(CatalogColumn("movie_id2"), CatalogColumn("keyword_id"))
//        ) -> "movie_keyword",
//        CatalogTable(
//          "name",
//          Seq(CatalogColumn("person_id"), CatalogColumn("name")),
//          Seq(CatalogColumn("person_id"))
//        ) -> "name"
//      )
//    }
//  }
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val cast_infoDS1 = inputData("cast_info1")
//    val cast_infoDS2 = inputData("cast_info2")
//    val movie_keyword1 = inputData("movie_keyword1")
//    val movie_keyword2 = inputData("movie_keyword2")
//    val name = inputData("name")
//
//    cast_infoDS1
//      .join(cast_infoDS2)
//      .join(movie_keyword1)
//      .join(movie_keyword2)
//      .join(name)
//      .aggregate("count(*) by name")
//  }
//}
