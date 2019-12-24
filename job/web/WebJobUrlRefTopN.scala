package com.wangjia.bigdata.core.job.web

import java.sql.Timestamp
import java.util.Properties

import com.wangjia.bigdata.core.bean.info.{AccountInfo, AppInfo}
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.LogType
import com.wangjia.hbase.HBaseTableName
import com.wangjia.utils.HBaseUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SaveMode, _}

import scala.collection.mutable

/**
  * Created by Administrator on 2017/5/11.
  */
case class TopN_UrlBean(appid: String, uuid: String, ref: String, url: String, ip: String, acc: Int)

class WebJobUrlRefTopN extends EveryDaySparkJob {
    //应用信息
    private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx


        //加载广播应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos
        mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.URL_BROWSE_LOG)
        HBaseUtils.createTable(HBaseTableName.EQID_KEYWORD)

        val sc = SparkExtend.ctx

        val linesRdd = sc.textFile(this.inPath)
        val beanRdd = linesRdd.map(BeanHandler.toWebAll)
                .filter(x => x != null && x.logtype == LogType.PAGE && x.ref.length > 0 && x.ref.length < 8192)
                .filter(x => {
                    val appInfo = mapAppInfosBroadcast.value.getOrElse(x.appid, null)
                    if (appInfo == null || appInfo.host == null || appInfo.host.length == 0)
                        false
                    else if (x.ref.indexOf(appInfo.host) != -1)
                        false
                    else
                        true
                })
                .map(x => TopN_UrlBean(x.appid, x.uuid, x.ref, x.url, x.ip, x.acc))

        val spark = SparkSession.builder().appName(this.jobName).getOrCreate()
        import spark.implicits._
        val beanDF = beanRdd.toDF
        beanDF.createOrReplaceTempView("t_TopN_UrlBean")

        //设置公共方法
        spark.udf.register("logDate", () => new java.sql.Date(this.logDate.getTime))
        spark.udf.register("addTime", () => new Timestamp(System.currentTimeMillis()))

        //设置数据库配置
        val prop = new Properties
        prop.setProperty("user", JDBCBasicUtils.getProperty("username"))
        prop.setProperty("password", JDBCBasicUtils.getProperty("password"))


        //统计URL num userNum ipNum
        val sql_url_visit =
            """select appid,
              | ref as url,
              | count(*) as all_visit_num,
              | count(case when acc=1 then 1 else null end) as acc_visit_num,
              | count(case when acc=1 then null else 1 end) as visit_num,
              | count(distinct case when acc=1 then uuid else null end) as acc_user_num,
              | count(distinct case when acc=1 then null else uuid end) as user_num,
              | count(distinct case when acc=1 then ip else null end) as acc_ip_num,
              | count(distinct case when acc=1 then null else ip end) as ip_num
              | from t_TopN_UrlBean group by appid,ref """.stripMargin

        val urlVisitDf: DataFrame = spark.sql(sql_url_visit)
        urlVisitDf.createOrReplaceTempView("urlVisitDf")
        //val aaa = """select * from tb as a where 2 > (select count(*) from tb where appid = a.appid and event_num > a.event_num ) order by a.appid,a.event_num  """

        //按APPID 分类 求访问次数TOPN
        val sql_url_visit_top100 =
            """select appid,
              | url,
              | all_visit_num,
              | acc_visit_num,
              | visit_num,
              | acc_user_num,
              | user_num,
              | acc_ip_num,
              | ip_num,
              | logDate() as day,
              | addTime() as addtime
              | from (select *,row_number() over(partition by appid order by all_visit_num desc) rank from urlVisitDf)
              | where rank <= 500 """.stripMargin

        val urlVisitTopNDf = spark.sql(sql_url_visit_top100)

        urlVisitTopNDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_url_ref_topn_everyday", prop)

        spark.stop()
    }
}

object WebJobUrlRefTopN {
    def main(args: Array[String]) {
        val job = new WebJobUrlRefTopN()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}