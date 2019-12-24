package com.wangjia.bigdata.core.job.web

import java.sql.Timestamp
import java.util.Properties

import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * 统计分辨率
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  *
  */
class WebJobScreenSize extends EveryDaySparkJob {

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

        val schema = StructType(List(
            StructField("appid", StringType),
            StructField("uuid", StringType),
            StructField("ip", StringType),
            StructField("system", StringType),
            StructField("browser", StringType),
            StructField("sw", IntegerType),
            StructField("sh", IntegerType)
        ))
        val rowRDD = beansRdd.map(bean => {
            val appid = bean.appid
            val uuid = bean.uuid
            val ip = bean.ip
            val system = bean.dStrInfo("system")
            val browser = bean.dStrInfo("browser")
            val sw = bean.dIntInfo("screenWidth")
            val sh = bean.dIntInfo("screenHeight")
            Row(appid, uuid, ip, system, browser, sw, sh)
        })

        val df: DataFrame = spark.createDataFrame(rowRDD, schema).toDF()
        df.createOrReplaceTempView("tb_base_bean")
        spark.udf.register("logDate", () => new java.sql.Date(this.logDate.getTime))
        spark.udf.register("addTime", () => new Timestamp(System.currentTimeMillis()))
        //按照浏览器和系统 统计pv，uv，ip
        val sqlText =
            """
              |select appid,
              |system,
              |sw,
              |sh,
              |count(distinct ip) as ip_num,
              |count(*) as pv_num,
              |count(distinct uuid) as user_num,
              |logDate() as day,
              |addTime() as addtime
              |from tb_base_bean
              |where sw != 0 and sh != 0
              |group by appid,system,sw,sh
            """.stripMargin
        val resultDf: DataFrame = spark.sql(sqlText)

        val prop = new Properties
        prop.setProperty("user", USERNAME)
        prop.setProperty("password", PASSWORD)
        try {
            resultDf.write.mode(SaveMode.Append).jdbc(URL, "ja_data_flow_everyday_screen_size", prop)
        } catch {
            case e: Exception => println(e)
        }
        spark.stop()
    }
}

object WebJobScreenSize {
    def main(args: Array[String]) {
        val job = new WebJobScreenSize()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}