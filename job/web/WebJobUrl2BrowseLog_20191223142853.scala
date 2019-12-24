package com.wangjia.bigdata.core.job.web

import java.sql.{Statement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Properties

import com.google.gson.JsonObject
import com.wangjia.bigdata.core.bean.info.{EqidUserInfo, SourceInfo, StrRule}
import com.wangjia.bigdata.core.bean.sparksql.LogSqlBean
import com.wangjia.bigdata.core.handler.{BeanHandler, KeyWordHandler, RuleHandler}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.eqid.EqidConnection
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.{HBaseUtils, MD5Utils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SaveMode, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/5/11.
  */
class WebJobUrl2BrowseLog extends EveryDaySparkJob {

    //来源地址映射
    private var sourceInfosBroadcast: Broadcast[Array[SourceInfo]] = null

    //EQID配置
    private var eqidUserInfosBroadcast: Broadcast[mutable.Map[String, EqidUserInfo]] = null


    override protected def init(): Unit = {
        super.init()
        //加载广播来源地址映射
        val sourceInfos = JAInfoUtils.loadSourceInfos
        sourceInfosBroadcast = SparkExtend.ctx.broadcast(sourceInfos)

        val eqidUserInfos = JAInfoUtils.loadEqidUserInfo()
        eqidUserInfosBroadcast = SparkExtend.ctx.broadcast(eqidUserInfos)
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.URL_BROWSE_LOG)
        HBaseUtils.createTable(HBaseTableName.EQID_KEYWORD)

        val sc = SparkExtend.ctx

        val linesRdd = sc.textFile(this.inPath)
        val beansRdd = linesRdd.map(BeanHandler.toWebAll)
                .filter(_ != null)

        val logSqlBeanRdd = beansRdd.mapPartitions(iterator => {
            initHBase()
            val bs = new ListBuffer[LogSqlBean]
            val conn = new EqidConnection
            iterator.foreach { case bean =>
                val deviceId = bean.dStrInfo("ja_uuid")
                val system = bean.dStrInfo("system")
                val browser = bean.dStrInfo("browser")
                val sw = bean.dIntInfo("screenWidth")
                val sh = bean.dIntInfo("screenHeight")
                val bcookie = bean.dIntInfo("bcookie")
                val bflash = bean.dIntInfo("bflash")

                val info = RuleHandler.getMeetRule(sourceInfosBroadcast.value.asInstanceOf[Array[StrRule]], bean.ref)
                val sourceid: Int = if (info != null) info.asInstanceOf[SourceInfo].id else 0
                val fkw: (String, String) = KeyWordHandler.url2KeyWord(sourceid, bean.ref)
                val kw_field: String = if (fkw == null) "" else fkw._1
                val kw: String = {
                    if (fkw == null)
                        ""
                    else if (!kw_field.equals("eqid"))
                        fkw._2
                    else {
                        val eqidUserInfo = eqidUserInfosBroadcast.value.getOrElse(bean.appid, null)
                        if (eqidUserInfo == null)
                            "eqid:" + fkw._2
                        else {
                            val str = conn.getKeyWord(fkw._2, eqidUserInfo.accesskey, eqidUserInfo.secretkey)
                            if (str != null) str else "eqid:" + fkw._2
                        }
                    }
                }

                val logBean = LogSqlBean(bean.appid,
                    bean.userid,
                    deviceId,
                    bean.uuid,
                    bean.logtype,
                    bean.subtype,
                    bean.time,
                    bean.ip,
                    bean.ref,
                    sourceid,
                    kw_field,
                    kw,
                    bean.url,
                    bean.acc,
                    system,
                    browser,
                    sw,
                    sh,
                    bcookie,
                    bflash)
                bs += logBean
            }
            conn.close()
            bs.toIterator
        })

        //保存到HBASE
        logSqlBeanRdd.foreachPartition(iterator => {
            initHBase()
            val conn = new ExTBConnection
            val tb = conn.getTable(HBaseTableName.URL_BROWSE_LOG)
            val json = new JsonObject
            while (iterator.hasNext) {
                val b = iterator.next()
                val urlMd5 = MD5Utils.MD5(b.url)
                val key = urlMd5 + b.appid + b.time
                json.addProperty("ref", b.ref)
                json.addProperty("url", b.url)
                json.addProperty("time", b.time)
                json.addProperty("ip", b.ip)
                json.addProperty("uuid", b.uuid)
                json.addProperty("kw", b.kw)
                json.addProperty("acc", b.acc)

                val put = new Put(Bytes.toBytes(key))
                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(json.toString))
                tb.addPut(put)
            }
            conn.close()
        })

        //计算URL 访问的TOPN
        val spark = SparkSession.builder().appName(this.jobName).getOrCreate()
        import spark.implicits._
        val beanDF = logSqlBeanRdd.toDF
        beanDF.createOrReplaceTempView("t_webLogPage")

        //设置公共方法
        spark.udf.register("logDate", () => new java.sql.Date(this.logDate.getTime))
        spark.udf.register("addTime", () => new Timestamp(System.currentTimeMillis()))


        //设置数据库配置
        val prop = new Properties
        prop.setProperty("user", JDBCBasicUtils.getProperty("username"))
        prop.setProperty("password", JDBCBasicUtils.getProperty("password"))

        //创建表
        val formatter = new SimpleDateFormat("yyyyMMdd")
        val tableName = "ja_data_url_des_everyday_" + formatter.format(this.logDate)
        val create_table_sql =
            """CREATE TABLE `table_name` (
              |  `id` int(11) NOT NULL AUTO_INCREMENT,
              |  `appid` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `userId` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `cookieId` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `logtype` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `subtype` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `ref` varchar(8192) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `sourceid` int(255) DEFAULT NULL,
              |  `kw_field` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
              |  `kw` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
              |  `url` varchar(8192) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `ip` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `uuid` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `time` bigint(20) DEFAULT NULL,
              |  `acc` int(11) DEFAULT NULL,
              |  `system` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `browser` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
              |  `sw` int(255) DEFAULT NULL,
              |  `sh` int(255) DEFAULT NULL,
              |  `bCookie` int(11) DEFAULT NULL,
              |  `bFlash` int(11) DEFAULT NULL,
              |  PRIMARY KEY (`id`)
              |) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;""".stripMargin
        try {
            val conn = JDBCBasicUtils.getConn(JDBCBasicUtils.getProperty("myurldes.url"),
                JDBCBasicUtils.getProperty("username"),
                JDBCBasicUtils.getProperty("password"))
            val stmt: Statement = conn.createStatement()
            stmt.executeUpdate(create_table_sql.replace("table_name", tableName))
            stmt.close()
            conn.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }

        //全局存入mysql
        val sql_url_des =
            """select appid,
              |userId,
              |cookieId,
              |logType as logtype,
              |sub_type as subtype,
              |ref,
              |sourceid,
              |kw_field,
              |kw,
              |url,
              |ip,
              |uuid,
              |time,
              |acc,
              |system,
              |browser,
              |sw,
              |sh,
              |bCookie,
              |bFlash
              |from t_webLogPage where length(ref)<8192 and length(url)<8192
            """.stripMargin
        val urlDesDf: DataFrame = spark.sql(sql_url_des)
        urlDesDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("myurldes.url"), tableName, prop)

        //统计URL num userNum ipNum
        val sql_url_visit =
            """select appid,
              | url,
              | count(*) as all_visit_num,
              | count(case when acc=1 then 1 else null end) as acc_visit_num,
              | count(case when acc=1 then null else 1 end) as visit_num,
              | count(distinct case when acc=1 then cookieId else null end) as acc_user_num,
              | count(distinct case when acc=1 then null else cookieId end) as user_num,
              | count(distinct case when acc=1 then ip else null end) as acc_ip_num,
              | count(distinct case when acc=1 then null else ip end) as ip_num
              | from t_webLogPage where logType='50' group by appid,url """.stripMargin

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

        urlVisitTopNDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_url_topn_everyday", prop)

        spark.stop()
    }
}

object WebJobUrl2BrowseLog {
    def main(args: Array[String]) {
        val job = new WebJobUrl2BrowseLog()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}

