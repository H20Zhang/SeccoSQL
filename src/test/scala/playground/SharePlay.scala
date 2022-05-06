//package playground
//
//import org.apache.spark.secco.execution.plan.communication.EnumShareComputer
//import util.SeccoFunSuite
//
//class SharePlay extends SeccoFunSuite {
//
//  //    val graphSize = 0.355049536 * 1000 toLong
//  //    val nodeSize = 0.02714264 * 1000 toLong
//
//  //WB
//  //    6.852E+05
//  //    1.330E+07
////  val graphSize = 13300000
////  val nodeSize = 685200
//
//  //AS
//  //1,696,415
//  //22,190,596
////  val graphSize = 22190596
////  val nodeSize = 1696415
//
//  //LJ
//  //    4.848E+06
//  //    8.570E+07
////  val graphSize = 85702474
////  val nodeSize = 4847571
////
//  val graphSize = 1000
//  val nodeSize = 1000
//
//  test("S1-S4") {
//
//    val triangleSchema = Seq(Seq("A", "B"), Seq("B", "C"), Seq("A", "C"))
//    val S1Schema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("A", "E"),
//      Seq("B", "E")
//    )
//
//    val S2Schema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("A", "E"),
//      Seq("B", "E"),
//      Seq("C", "E")
//    )
//
//    val S3Schema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("A", "E"),
//      Seq("A", "C"),
//      Seq("B", "D"),
//      Seq("C", "E")
//    )
//
//    val S4Schema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("A", "E"),
//      Seq("B", "D"),
//      Seq("B", "E"),
//      Seq("C", "E")
//    )
//
//    val TwoEdgeSchema = Seq(Seq("A", "B"), Seq("B", "C"))
//    val ThreeEdgeSchema = Seq(Seq("A", "B"), Seq("B", "C"), Seq("C", "D"))
//    val FourEdgeSchema =
//      Seq(Seq("A", "B"), Seq("B", "C"), Seq("C", "D"), Seq("D", "E"))
//    val FiveEdgeSchema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("E", "F")
//    )
//    val SixEdgeSchema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("E", "F"),
//      Seq("F", "G")
//    )
//
//    val SevenEdgeSchema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("E", "F"),
//      Seq("F", "G"),
//      Seq("G", "I")
//    )
//
//    val triangleEdge =
//      Seq(Seq("A", "B"), Seq("B", "C"), Seq("C", "A"), Seq("A", "D"))
//
//    val chordalSquare = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "A"),
//      Seq("A", "C")
//    )
//
//    val TwoEdgeConstraint = Map("B" -> 1)
//    val ThreeEdgeConstraint = Map("B" -> 1, "C" -> 1)
//    val FourEdgeConstraint = Map("B" -> 1, "C" -> 1, "D" -> 1)
//    val FiveEdgeConstraint = Map("B" -> 1, "C" -> 1, "D" -> 1, "E" -> 1)
//    val SixEdgeConstraint =
//      Map("B" -> 1, "C" -> 1, "D" -> 1, "E" -> 1, "F" -> 1)
//
//    val tasks = Seq(
////      S1Schema,
////      S2Schema,
////      S3Schema,
////      S4Schema,
////      triangleEdge,
////      chordalSquare,
//      TwoEdgeSchema,
//      ThreeEdgeSchema,
//      FourEdgeSchema,
//      FiveEdgeSchema,
//      SixEdgeSchema
////      SevenEdgeSchema
//    ).zip(
//      Seq(
//        TwoEdgeConstraint,
//        ThreeEdgeConstraint,
//        FourEdgeConstraint,
//        FiveEdgeConstraint,
//        SixEdgeConstraint
//      )
//    )
//
//    val costs = tasks.map {
//      case (taskSchema, taskConstraints) =>
//        val schemas = taskSchema
//        val constraint: Map[String, Int] = taskConstraints
//        val tasks = 192
//        val statisticMap: Map[Seq[String], Long] =
//          schemas
//            .map(f => (f, graphSize.toLong))
//            .toMap
//
//        val shareComputer =
//          new EnumShareComputer(schemas, constraint, tasks, statisticMap)
//
//        shareComputer.optimalShareWithBudget()
//    }
//
//    costs.foreach { cost =>
//      println(s"cost:${cost}")
//    }
//
//  }
//
//  test("C1-C2") {
//
//    val triangleSchema = Seq(Seq("A", "B"), Seq("B", "C"), Seq("A", "C"))
//    val C1Schema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("A", "E"),
//      Seq("B", "E")
//    )
//
//    val C2Schema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("A", "E"),
//      Seq("B", "E"),
//      Seq("C", "E")
//    )
//
//    val C1Constraint = Map("A" -> 1, "C" -> 1, "E" -> 1, "D" -> 1)
//    val C2Constraint = Map("A" -> 1, "D" -> 1, "E" -> 1, "C" -> 1)
//
//    val tasks = Seq(C1Schema, C2Schema).zip(Seq(C1Constraint, C2Constraint))
//
//    val costs = tasks.map {
//      case (taskSchema, taskConstraints) =>
//        val schemas = taskSchema
//        val constraint: Map[String, Int] = taskConstraints
//        val tasks = 192
//        val statisticMap: Map[Seq[String], Long] =
//          schemas
//            .map(f => (f, graphSize.toLong))
//            .toMap
//
//        val shareComputer =
//          new EnumShareComputer(schemas, constraint, tasks, statisticMap)
//
//        shareComputer.optimalShareWithBudget()
//    }
//
//    costs.foreach { cost =>
//      println(s"cost:${cost}")
//    }
//  }
//
//  test("C3-C4") {
//
//    val triangleSchema = Seq(Seq("A", "B"), Seq("B", "C"), Seq("A", "C"))
//    val S1Schema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("A", "E"),
//      Seq("B", "E"),
//      Seq("D", "F")
//    )
//
//    val S2Schema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "E"),
//      Seq("A", "E"),
//      Seq("B", "E"),
//      Seq("C", "E"),
//      Seq("D", "F")
//    )
//
//    val S1Constraint = Map("B" -> 1, "C" -> 1, "E" -> 1, "D" -> 1, "F" -> 1)
//    val S2Constraint = Map("B" -> 1, "D" -> 1, "E" -> 1, "C" -> 1, "F" -> 1)
//
//    val tasks = Seq(S1Schema, S2Schema).zip(Seq(S1Constraint, S2Constraint))
//
////    val tasks = Seq(S1Schema, S2Schema)
//
//    val costs = tasks.map {
//      case (taskSchema, taskConstraints) =>
//        val schemas = taskSchema
//        val constraint: Map[String, Int] = taskConstraints
//        val tasks = 192
//        val statisticMap: Map[Seq[String], Long] =
//          schemas.map { f =>
//            if (f != Seq("D", "F")) {
//              (f, graphSize.toLong)
//            } else {
//              (f, nodeSize.toLong)
//            }
//          }.toMap
//
//        val shareComputer =
//          new EnumShareComputer(schemas, constraint, tasks, statisticMap)
//
//        shareComputer.optimalShareWithBudget()
//    }
//
//    costs.foreach { cost =>
//      println(s"cost:${cost}")
//    }
//
//  }
//
//  test("secco") {
//
//    val triangleSchema = Seq(Seq("A", "B"), Seq("B", "C"), Seq("A", "C"))
//    val squareSchema =
//      Seq(Seq("A", "B"), Seq("B", "C"), Seq("C", "D"), Seq("D", "A"))
//    val fourCliqueSchema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("C", "D"),
//      Seq("D", "A"),
//      Seq("A", "C"),
//      Seq("B", "D")
//    )
//    val trianglePlusEdgeSchema =
//      Seq(Seq("A", "B"), Seq("B", "C"), Seq("A", "C"), Seq("B", "A"))
//    val trianglePlusTwoEdgeWithConstraintSchema = Seq(
//      Seq("A", "B"),
//      Seq("B", "C"),
//      Seq("A", "C"),
//      Seq("B", "A"),
//      Seq("C", "A")
//    )
//    val squarePlusEdgeWithConstraintSchema =
//      Seq(
//        Seq("A", "B"),
//        Seq("B", "C"),
//        Seq("C", "D"),
//        Seq("D", "A"),
//        Seq("B", "A")
//      )
//
//    val triangleAddEdge = Seq(Seq("A", "B"), Seq("B", "C", "F"), Seq("A", "C"))
//    val squareAddEdge =
//      Seq(Seq("A", "B"), Seq("B", "C", "F"), Seq("C", "D"), Seq("D", "A"))
//
//    val trianglePlusTwoEdgeWithConstraint = Map("A" -> 1)
//    val squarePlusEdgeWithConstraint = Map("A" -> 1, "C" -> 1)
//
//    val tasks = Seq(
//      triangleSchema,
//      squareSchema,
//      fourCliqueSchema,
//      trianglePlusEdgeSchema,
//      triangleAddEdge,
//      squareAddEdge,
//      trianglePlusTwoEdgeWithConstraintSchema,
//      squarePlusEdgeWithConstraintSchema
//    ).zip(
//      Seq(
//        Map[String, Int](),
//        Map[String, Int](),
//        Map[String, Int](),
//        Map[String, Int](),
//        Map[String, Int](),
//        Map[String, Int](),
//        trianglePlusTwoEdgeWithConstraint,
//        squarePlusEdgeWithConstraint
//      )
//    )
//
//    val costs = tasks.map {
//      case (taskSchema, taskConstraints) =>
//        val schemas = taskSchema
//        val constraint: Map[String, Int] = taskConstraints
//        val tasks = 192
//        val statisticMap: Map[Seq[String], Long] =
//          schemas.map { f =>
//            if (f == Seq("B", "C", "F")) {
//              val newGraphSize = (graphSize * 1.5).toLong
//              (f, newGraphSize.toLong)
//            } else {
//              (f, graphSize.toLong)
//            }
//
//          }.toMap
//
//        val shareComputer =
//          new EnumShareComputer(schemas, constraint, tasks, statisticMap)
//
//        shareComputer.optimalShareWithBudget()
//    }
//
//    costs.foreach { cost =>
//      println(s"cost:${cost}")
//    }
//  }
//
//  test("imdb") {
//
//    val Q3 = Seq(Seq("pid", "mid"), Seq("mid", "mname"))
//    val Q4 = Seq(Seq("pid", "mid"), Seq("mid", "mk"), Seq("mid", "mname"))
//    val Q5 = Seq(
//      Seq("pid", "mid"),
//      Seq("mid", "mk"),
//      Seq("mid", "mname")
//    )
//    val Q6 = Seq(
//      Seq("pid", "mid"),
//      Seq("mid", "mk"),
//      Seq("mid", "mname"),
//      Seq("pid", "name")
//    )
//
//    val Q10 = Seq(Seq("pid", "mid1"), Seq("mid2", "mk"), Seq("mid1", "mname"))
//    val Q11 = Seq(
//      Seq("pid1", "mid"),
//      Seq("pid2", "mid"),
//      Seq("pid2", "mid2"),
//      Seq("pid1", "name")
//    )
//    val Q12 = Seq(
//      Seq("pid", "mid1"),
//      Seq("pid", "mid2"),
//      Seq("mid1", "mk"),
//      Seq("mid2", "mk"),
//      Seq("pid1", "name")
//    )
//
//    val debug = Seq(
//      Seq("pid", "name"),
//      Seq("mid", "mk"),
//      Seq("mid", "mname")
//    )
//
//    val numCI = 36244344
//    val numMK = 4523930
//    val numT = 2528312
//    val numNI = 4167491
//
//    val sizeMap = Map(
//      Seq("pid", "mid") -> numCI,
//      Seq("mid", "mk") -> numMK,
//      Seq("mid", "mname") -> numT,
//      Seq("pid", "name") -> numNI,
//      Seq("pid", "mid1") -> numCI,
//      Seq("mid2", "mk") -> numMK,
//      Seq("mid1", "mname") -> numT,
//      Seq("pid1", "mid") -> numCI,
//      Seq("pid2", "mid") -> numCI,
//      Seq("pid2", "mid2") -> numCI,
//      Seq("pid1", "name") -> numNI,
//      Seq("pid", "mid1") -> numCI,
//      Seq("pid", "mid2") -> numCI,
//      Seq("mid1", "mk") -> numMK,
//      Seq("mid2", "mk") -> numMK,
//      Seq("pid1", "name") -> numNI
//    )
//
//    val Q3Constraint = Map("pid" -> 1, "mid" -> 1)
//    val Q4Constraint = Map[String, Int]()
//    val Q5Constraint = Map("pid" -> 1, "mid" -> 1, "mk" -> 1)
//    val Q6Constraint = Map("pid" -> 1, "mid" -> 1, "mk" -> 1, "name" -> 1)
//    val Q10Constraint = Map("pid" -> 1, "mid1" -> 1, "mid1" -> 2)
//    val Q11Constraint =
//      Map("pid" -> 1, "mid" -> 1, "mid2" -> 1, "pid1" -> 1, "pid2" -> 1)
//    val Q12Constraint = Map(
//      "pid" -> 1,
//      "mid" -> 1,
//      "mid2" -> 1,
//      "pid1" -> 1,
//      "pid2" -> 1,
//      "mk" -> 1
//    )
//
//    val tasks = Seq(
//      Q3,
//      Q4,
//      Q5,
//      Q6,
//      Q10,
//      Q11,
//      Q12,
//      debug
//    ).zip(
//      Seq(
//        Q3Constraint,
//        Q4Constraint,
//        Q5Constraint,
//        Q6Constraint,
//        Q10Constraint,
//        Q11Constraint,
//        Q12Constraint,
//        Map[String, Int]()
//      )
//    )
//
//    val costs = tasks.map {
//      case (taskSchema, taskConstraints) =>
//        val schemas = taskSchema
//        val constraint: Map[String, Int] = taskConstraints
//        val tasks = 192
//        val statisticMap: Map[Seq[String], Long] =
//          schemas.map { f =>
//            val size = sizeMap(f)
//            (f, size.toLong)
//          }.toMap
//
//        val shareComputer =
//          new EnumShareComputer(schemas, constraint, tasks, statisticMap)
//
//        shareComputer.optimalShareWithBudget()
//    }
//
//    costs.foreach { cost =>
//      println(s"cost:${cost}")
//    }
//  }
//
//}
