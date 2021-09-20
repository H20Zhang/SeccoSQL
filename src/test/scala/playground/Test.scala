//package playground
//
//import dbx.deprecated.Builder
//
//object Test extends App {
//  val builder = Builder.builder
//  //  val builder = SparkSession.builder
//  val spark = builder
//    .master("local[1]")
//    .appName("test")
//    .getOrCreate()
//  import spark.sqlContext.implicits._
//  spark.sparkContext.setLogLevel("ERROR")
//  case class T(i: Int)
//  spark.sparkContext
//    .parallelize(1 to 10)
//    .map(T(_))
//    .toDF
//    .createOrReplaceTempView("T")
//  case class H(ID: Int, h: Int, a: Int)
//  spark.sparkContext
//    .parallelize(1 to 10)
//    .map(x => H(x, x, x))
//    .toDF
//    .createOrReplaceTempView("H")
//  case class L(L: Int)
//  case class V(ID: Int)
//  case class D(ID: Int, vw: Int)
//  case class E(F: Int, T: Int, ew: Int)
//  spark.sparkContext
//    .parallelize(1 to 10)
//    .map(V(_))
//    .toDF
//    .createOrReplaceTempView("V")
//  spark.sparkContext
//    .parallelize(1 to 10)
//    .map(x => E(x, x, x))
//    .toDF
//    .createOrReplaceTempView("E")
//  spark.sparkContext
//    .parallelize(1 to 10)
//    .map(x => D(x, x))
//    .toDF
//    .createOrReplaceTempView("D")
//  spark.sparkContext
//    .parallelize(1 to 10)
//    .map(x => H(x, x, x))
//    .toDF
//    .createOrReplaceTempView("RA")
//  spark.sparkContext
//    .parallelize(0 to 0)
//    .map(L(_))
//    .toDF
//    .createOrReplaceTempView("L")
//  //    val in = """  WITH Hh AS (SELECT ID, h FROM H),
//  //                        Ra AS (SELECT E.F AS ID, sum(h * ew) AS a FROM Hh, E WHERE Hh.ID = E.T GROUP BY E.F),
//  //                        Rh AS (SELECT E.T AS ID, sum(a * ew) AS h FROM Ra, E WHERE Ra.ID = E.F GROUP BY E.T),
//  //                        Rha AS (SELECT Ra.ID, h, a FROM Ra, Rh WHERE Ra.ID = Rh.ID),
//  //                        Rn AS (SELECT sum(h * h) as nh, sum(a * a) as na FROM Rha)
//  //                   SELECT ID, h/sqrt(nh) AS A, a/sqrt(na) AS B FROM Rha, Rn
//  //                """
//  //    val edge = spark.sparkContext.textFile("graph.txt").map { x => x.split("\t").map { x => x.toInt } }
//  //    edge.map { a => E(a(0), a(1), 1) }.repartition(1).cache.toDF.createOrReplaceTempView("E")
//  //    edge.flatMap { a => Seq(a(0), a(1)) }.distinct.repartition(1).cache.map { x => V(x) }.toDF.createOrReplaceTempView("V")
//  val in_ind =
//    """
//    with recursive(10) ind_set(ID) as(
//      (with ind_ini(ID, value) as (select ID, rand() from V),
//            ind_node_ini(ID, value, nei_value) as (select tmp1.ID, tmp1.value, min(tmp2.value) from ind_ini as tmp1, E, ind_ini as tmp2 where tmp1.ID = E.F and tmp2.ID = E.T group by tmp1.ID, tmp1.value)
//       select ID from ind_node_ini where value<nei_value
//      )
//      union all
//      (with tmp_node(ID) as (select V.ID from V left outer join ind_set on V.ID = ind_set.ID where ind_set.ID is null ),
//            tmp_node2(ID) as (select tmp_node.ID from tmp_node left outer join (select E.T from ind_set, E where ind_set.ID = E.F) as E1 on tmp_node.ID = E1.T where E1.T is null),
//            ind_rec(ID, value) as (select ID, rand() from tmp_node2),
//            ind_node_rec(ID, value, nei_value) as (select tmp1.ID, tmp1.value, min(tmp2.value) from ind_rec as tmp1, E, ind_rec as tmp2 where tmp1.ID=E.F and tmp2.ID = E.T group by tmp1.ID, tmp1.value)
//       select ID from ind_node_rec where value < nei_value
//      )
//    ) select * from ind_set;
//    """
//
//  val in_ind_notin =
//    """
//    with recursive(10) ind_set(ID) as(
//      (with ind_ini(ID, value) as (select ID, rand() from V),
//            ind_node_ini(ID, value, nei_value) as (select tmp1.ID, tmp1.value, min(tmp2.value) from ind_ini as tmp1, E, ind_ini as tmp2 where tmp1.ID = E.F and tmp2.ID = E.T group by tmp1.ID, tmp1.value)
//       select ID from ind_node_ini where value<nei_value
//      )
//      union all
//      (with tmp_node(ID) as (select ID from V where ID not in (select ID from ind_set)),
//            tmp_node2(ID) as (select ID from tmp_node where ID not in (select E.T from ind_set, E where ind_set.ID = E.F)),
//            ind_rec(ID, value) as (select ID, rand() from tmp_node2),
//            ind_node_rec(ID, value, nei_value) as (select tmp1.ID, tmp1.value, min(tmp2.value) from ind_rec as tmp1, E, ind_rec as tmp2 where tmp1.ID=E.F and tmp2.ID = E.T group by tmp1.ID, tmp1.value)
//       select ID from ind_node_rec where value < nei_value
//      )
//    ) select * from ind_set;
//    """
//
//  val in_ind_notexists =
//    """
//    with recursive(10) ind_set(ID) as(
//      (with ind_ini(ID, value) as (select ID, rand() from V),
//            ind_node_ini(ID, value, nei_value) as (select tmp1.ID, tmp1.value, min(tmp2.value) from ind_ini as tmp1, E, ind_ini as tmp2 where tmp1.ID = E.F and tmp2.ID = E.T group by tmp1.ID, tmp1.value)
//       select ID from ind_node_ini where value<nei_value
//      )
//      union all
//      (with tmp_node(ID) as (select ID from V where not exists (select ind_set.ID from ind_set where ind_set.ID = V.ID)),
//            tmp_node2(ID) as (select ID from tmp_node where not exists (select E.T from ind_set, E where ind_set.ID = E.F and E.T = tmp_node.ID)),
//            ind_rec(ID, value) as (select ID, rand() from tmp_node2),
//            ind_node_rec(ID, value, nei_value) as (select tmp1.ID, tmp1.value, min(tmp2.value) from ind_rec as tmp1, E, ind_rec as tmp2 where tmp1.ID=E.F and tmp2.ID = E.T group by tmp1.ID, tmp1.value)
//       select ID from ind_node_rec where value < nei_value
//      )
//    ) select * from ind_set;
//    """
//  //  val in = " select count(*) from V b where 1 = (select count(*)  from V a where a.ID=b.ID)"
//  //      val in = """
//  //                          with recursive(10) H(ID, h, a) as (
//  //                            (select ID, 1.0 as h, 1.0 as a from V)
//  //                            union by update ID
//  //                            (with Hh as (select ID, h from H),
//  //                                  Ra(ID, a) as (select E.F, sum(h * ew) from Hh, E where Hh.ID = E.T group by E.F),
//  //                                  Rh(ID, h) as (select E.T, sum(a * ew) from Ra, E where Ra.ID = E.F group by E.T),
//  //                                  Rha(ID, h, a) as ((select ID, 0 as h, 0 as a from V)
//  //                                                    union by update ID
//  //                                                    (select Ra.ID, h, a from Ra, Rh where Ra.ID = Rh.ID)),
//  //                                  Rn(nh, na) as (select sum(h * h), sum(a * a) from Rha)
//  //                             select ID, h/sqrt(nh) as h, a/sqrt(na) as a from Rha, Rn)
//  //                          ) select * from H;
//  //                          """
//  //val in ="""
//  //with recursive KCORE(ID, deg) as (
//  //(select E.T as ID, count(V.ID) as deg  from V, E where V.ID = E.F group by E.T)
//  //union by update ID
//  //( with CR(F, T) as (select F, T from KCORE as KC1 , E, KCORE as KC2 where KC1.ID = E.F and KC2.ID = E.T and KC1.deg >= 2 and KC2.deg >= 2)
//  //select CR.T as ID, count(KCORE.ID) as deg from KCORE, CR where KCORE.ID = CR.F group by CR.T)
//  //) select * from KCORE ;
//  //"""
//  val in =
//    """select ID*ID from (select ID+1  as ID from V) t"""
//  //  val in ="with recursive(3) H(ID, h, a) as ((select * from H) union all ( select ID+10,h,a from H)) select * from H"
//  //    var in = """        with H(ID, h, a) as (select ID, 1.0 as h, 1.0 as a from V)
//  //                        (select * from H)
//  //                        union by update ID
//  //                        (with Hh as (select ID, h from H),
//  //                              Ra(ID, a) as (select E.F, sum(h * ew) from Hh, E where Hh.ID = E.T group by E.F),
//  //                              Rh(ID, h) as (select E.T, sum(a * ew) from Ra, E where Ra.ID = E.F group by E.T),
//  //                              Rha(ID, h, a) as ((select ID, 0 as h, 0 as a from V)
//  //                                                union by update ID
//  //                                                (select Ra.ID, h, a from Ra, Rh where Ra.ID = Rh.ID)),
//  //                              Rn(nh, na) as (select sum(h * h), sum(a * a) from Rha)
//  //                         select ID, h/sqrt(nh) as h, a/sqrt(na) as a from Rha, Rn)
//  //                      """
//  //    for (i <- 0 until 1) {
//  //      in = s"""         with H(ID, h, a) as ($in)
//  //                        (select * from H)
//  //                        union by update ID
//  //                        (with Hh as (select ID, h from H),
//  //                              Ra(ID, a) as (select E.F, sum(h * ew) from Hh, E where Hh.ID = E.T group by E.F),
//  //                              Rh(ID, h) as (select E.T, sum(a * ew) from Ra, E where Ra.ID = E.F group by E.T),
//  //                              Rha(ID, h, a) as ((select ID, 0 as h, 0 as a from V)
//  //                                                union by update ID
//  //                                                (select Ra.ID, h, a from Ra, Rh where Ra.ID = Rh.ID)),
//  //                              Rn(nh, na) as (select sum(h * h), sum(a * a) from Rha)
//  //                         select ID, h/sqrt(nh) as h, a/sqrt(na) as a from Rha, Rn)
//  //  """
//  //    }
//  //    val in = """
//  //                    with recursive(10) H(ID, h, a) as (
//  //                      (select ID, 0.0 as h, 0.0 as a from V)
//  //                      union by update ID
//  //                      (select ID, h + 1 as h, a + 1 as a from H)
//  //                    ) select * from H;
//  //                    """
//  //  val in = """
//  //                  with recursive(10) H(ID, h, a) as (
//  //                    (select ID, 1.0 as h, 1.0 as a from V)
//  //                    union by update ID
//  //                    (with Hh as (select ID, h from H),
//  //                          Ra(ID, a) as (select E.F, sum(h * ew) from Hh, E where Hh.ID = E.T group by E.F),
//  //                          Rh(ID, h) as (select E.T, sum(a * ew) from Ra, E where Ra.ID = E.F group by E.T),
//  //                          Rha as (select Ra.ID, h, a from Ra, Rh where Ra.ID = Rh.ID),
//  //                          Rn(nh, na) as (select sum(h * h), sum(a * a) from Rha)
//  //                     select ID, h/sqrt(nh) as h, a/sqrt(na) as a from Rha, Rn)
//  //                  ) select * from H;
//  //                  """
//  //   val in = "select ID from ((select * from V) union by update ID (select * from V))  t"
//  //  val in = "with E as (select * from E ), V as ()"
//  //      val in = """
//  //                with recursive(10) H(ID, h, a) as (
//  //                  (select ID, 1.0 as h, 1.0 as a from V)
//  //                  union by update ID
//  //                  (with Hh as (select ID, h from H),
//  //                        Ra(ID, a) as (select E.F, sum(h * ew) as x from Hh, E where ID = E.T group by E.F)
//  //                   select ID, a as h, a from Ra)
//  //                ) select * from H;
//  //    """
//  //  val in ="""      with Ra as (select ID, a from RA)
//  //                   select  ID, a as h, a from Ra"""
//  //
//  //  val in ="""     with Ra(ID,a) as (select ID, h  from H)
//  //                  select ID, a as h, a from Ra"""
//  //  val in ="""     with Ra as (select ID as ID, h as a from (select ID, h  from H) t)
//  //                  select ID, a as h, a from Ra"""
//  //  val in ="""(select * from H) union by update ID (select * from H)"""
//  //  val in = """
//  //              with recursive(10) H(ID, h, a) as (
//  //                (select ID, 1.0 as h, 1.0 as a from V)
//  //                union by update ID
//  //                (select ID, a as h, a from (select F as ID, x as a from (select E.F, sum(h * ew) as x from (select ID, h from H) Hh, E where ID = E.T group by E.F) t) Ra)
//  //              ) select * from H;
//  //  """
//  //  val in = "select sum(ID) from V"
//  //  val in = "select ID, h from H"
//  //  val in = "(select ID, h from H) union (select ID, h from H)"
//  //    val in ="select * from V"
//  //  val in = "with Hh as (select ID, h from H) select * from Hh"
//  //  val in = "select distinct a.i + 1,a.* from T a, T t where a.i > 1 and t.i = a.i group by a.i having a.i > 2"
//  //  val in = "select distinct a.j + 1,a.* from T a, T t"
//  //  val in = "(select i,j from (select  i , i as j from T a) a) union by update i (select  i +1 as j,i from T)"
//  //  val in = "select * from (select distinct a.i + 1 as j, i from T a)  L full outer join  (select  i,i as j from T) R using (i)"
//  //    val in = "select * from T a join T b using (i)"
//
//  val in_APSP =
//    """
//    with recursive APSP(F, T, ew) as (
//      (select F, T, ew from E)
//        union by update F, T
//        (select APSP.F, E.T as T,  min(APSP.ew + E.ew) as ew from APSP, E where APSP.T  = E.F group by  E.T, APSP.F )
//    ) select * from APSP;
//    """
//
//  val in_topo =
//    """
//        |with recursive Topo(ID, L) as (
//        |(select ID, 0 from V  left outer join E on V.ID = E.T where E.T is null  )
//        |union all
//        |( with Ln(L) as (select max(L) + 1 as L from Topo),
//        |
//        |V1(ID) as (select V.ID as ID from V left outer join Topo on V.ID = Topo.ID where Topo.ID is null ),
//        |
//        |E1(F, T) as (select E.F as F , E.T as T  from V1, E where V1.ID = E.F ),
//        |
//        |Tn (ID, L) as (select V1.ID as ID , L from L, V1 left outer join E1 on V1.ID = E1.T where E1.T is null )
//        |
//        |select ID, L from Tn)
//        |) select * from Topo;
//        |
//      """.stripMargin
//
//  val in_topo_notin =
//    """
//          |with recursive Topo(ID, L) as (
//          |(select ID, 0 from V where V.ID not in (select E.T from E)  )
//          |union all
//          |( with Ln(L) as (select max(L) + 1 as L from Topo),
//          |
//          |V1(ID) as (select V.ID as ID from V where V.ID not in (select ID from Topo) ),
//          |
//          |E1(F, T) as (select E.F as F , E.T as T  from V1, E where V1.ID = E.F ),
//          |
//          |Tn (ID, L) as (select V1.ID as ID , L from L, V1 where V1.ID not in (select E1.T from E1))
//          |
//          |select ID, L from Tn)
//          |) select * from Topo;
//          |
//      """.stripMargin
//
//  val in_topo_notexists =
//    """
//          |with recursive Topo(ID, L) as (
//          |(select ID, 0 from V where not exists (select E.T from E where E.T = V.ID)  )
//          |union all
//          |( with Ln(L) as (select max(L) + 1 as L from Topo),
//          |
//          |V1(ID) as (select V.ID as ID from V where not exists (select Topo.ID from Topo where Topo.ID = V.ID) ),
//          |
//          |E1(F, T) as (select E.F as F , E.T as T  from V1, E where V1.ID = E.F ),
//          |
//          |Tn (ID, L) as (select V1.ID as ID , L from L, V1 where not exists (select E1.T from E1 where E1.T = V1.ID))
//          |
//          |select ID, L from Tn)
//          |) select * from Topo;
//          |
//      """.stripMargin
//
//  val in_wcc =
//    """
//        |with recursive WCC(ID, vw) as(
//        |(select ID , ID from V)
//        |union by update ID
//        |(select E.T as ID, min(WCC.vw) as vw from WCC , E where WCC.ID = E.F group by E.T )
//        |) select * from WCC where ID = 1;
//        |
//      """.stripMargin
//
//  val in_pagerank =
//    """
//          |with recursive(10) PageRank(ID , vw) as(
//          |(select ID, 0.0 as vw from V)
//          |union by update ID
//          |(select E.F as ID, 0.85*sum(PageRank.vw * E.ew)+0.15*1/10000 as vw  from PageRank, E where PageRank.ID = E.T group by E.F )
//          |) select vw from PageRank where ID =1 ;
//          |
//      """.stripMargin
//
//  val in_ppr =
//    """
//          |with recursive(10) PPR(ID, vw) as (
//          |(select ID, 0.0 from V)
//          |union by update ID
//          |(select E.F as ID, sum(PPR.vw * E.ew)+ first(E.vw) as vw from (select * from D join E on D.ID = E.F) E, PPR
//          |where PPR.ID = E.T group by E.F)
//          | )select * from PPR;
//          |
//          |
//      """.stripMargin
//
//  val in_ppr2 =
//    """with recursive(10) PPR(ID, vw) as (
//          |(select ID, 0.0 from V)
//          |union by update ID
//          |(select F as ID, TEMP.vw + D.vw from D, (select E.F, sum(PPR.vw * E.ew) as vw  from PPR, E
//          |where PPR.ID = E.T group by E.F ) as TEMP  where D.ID = TEMP.F )
//          |) select * from PPR;
//      """.stripMargin
//
//  val in_anti = "select V.ID from V where V.ID not in (select F from E)"
//  val sql = spark.sql(in_ppr2)
//  //    println(Parser.parsePlan(in))
//  println(sql.queryExecution.logical)
//  println(sql.queryExecution.analyzed)
//  println(sql.queryExecution.optimizedPlan)
//  //  println(sql.queryExecution.logical.children.head.asInstanceOf[Project].projectList.head.getClass)
//  //  sql.explain(true)
//  //   println(sql.rdd.toDebugString)
//  //  sql.toDF.queryExecution.debug.codegen
//
//  //  sql.rdd.foreach { x => println("ans:" + x) }
//  //  val ans = sql.collectAsList
//  //  println(ans.subList(0, Math.min(10, ans.size)))
//  //  import org.apache.spark.sql.execution.debug._
//  //  sql.debugCodegen
//}
