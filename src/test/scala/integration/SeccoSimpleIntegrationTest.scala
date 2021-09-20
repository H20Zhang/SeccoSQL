package integration

import org.apache.spark.secco.SeccoSession
import util.SeccoFunSuite

class SeccoSimpleIntegrationTest extends SeccoFunSuite {

  test("basic") {
    println(s"simple integration test")

    val sqlText =
      """
        |select
        |	avg(l_quantity) +1 as avg_qty,
        |	avg(l_extendedprice) as avg_price,
        |	avg(l_discount) as avg_disc,
        |	count(*) as count_order
        |from
        |	lineitem
        |where
        |	l_shipdate <= 1998
        |group by
        |	l_returnflag,
        |	l_linestatus
        |order by
        |	l_returnflag,
        |	l_linestatus desc
        |limit 100
      """.stripMargin

    val seSession = SeccoSession.currentSession
    val ds = seSession.sql(sqlText)

    print(ds.queryExecution.logical)

  }

}
