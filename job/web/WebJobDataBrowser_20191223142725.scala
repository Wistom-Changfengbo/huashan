package com.wangjia.bigdata.core.job.web

import java.sql.Timestamp
import java.util.Properties

import com.wangjia.bigdata.core.bean.sparksql.BrowserSqlBean
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by Cfb on 2017/8/24.
  */
class WebJobDataBrowser extends EveryDaySparkJob {

    private[this] val USERNAME = JDBCBasicUtils.getProperty("username")
    private[this] val PASSWORD = JDBCBasicUtils.getProperty("password")
    private[this] val URL = JDBCBasicUtils.getProperty("url")

    /**
      *
      * 任务入口方法
      *
      * @param args
      */
    override protected def job(args: Array[String]): Unit = {
        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile(this.inPath)

        val beansRdd = linesRdd.map(BeanHandler.toWebAcc).filter(_ != null)
        val spark: SparkSession = SparkSession.builder().appName(this.jobName).getOrCreate()
        import spark.implicits._
        val beanDF: DataFrame = beansRdd.map(x => {
            BrowserSqlBean(x.appid, x.dStrInfo("ja_uuid"), x.ip, x.dStrInfo("system"), x.dStrInfo("browser"))
        }).toDF()

        beanDF.createOrReplaceTempView("app_browser")
        spark.udf.register("logDate", () => new java.sql.Date(this.logDate.getTime))
        spark.udf.register("addTime", () => new Timestamp(System.currentTimeMillis()))
        //按照浏览器和系统 统计pv，uv，ip
        val sqlText =
            """
              |select appid,
              |system,
              |browser,
              |count(distinct ip) as ip_num,
              |count(*) as pv_num,
              |count(distinct cookieId) as uv_num,
              |logDate() as day,
              |addTime() as addtime
              |from app_browser
              |group by appid,system,browser
            """.stripMargin
        val sql: DataFrame = spark.sql(sqlText)
        val prop = new Properties
        prop.setProperty("user", USERNAME)
        prop.setProperty("password", PASSWORD)
        try {
            sql.write.mode(SaveMode.Append).jdbc(URL, "ja_data_browser_everyday", prop)
        } catch {
            case e:Exception => println(e)
        }
        spark.stop()
    }
}

object WebJobDataBrowser {
    def main(args: Array[String]) {
        val job = new WebJobDataBrowser()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
