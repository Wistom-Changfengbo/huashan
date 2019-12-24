package com.wangjia.bigdata.core.job.common

import java.sql.Timestamp
import java.util.Properties

import com.wangjia.bigdata.core.bean.info.{SourceInfo, StrRule}
import com.wangjia.bigdata.core.handler.{BeanHandler, RuleHandler}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{IPUtils, JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.{LogType, Platform}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, _}

/**
  * 统计各个平台的流量信息
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --platform  平台:web,app，weixin
  *
  * Created by Cfb on 2018/2/26.
  */

case class ComFlewBean(appid: String, uuid: String, pageRule: String, time: Long, stayTime: Long, ip: String, channel: String, sourceid: Int)

class ComJobFlow extends EveryDaySparkJob {
    //来源地址映射
    private var sourceInfosBroadcast: Broadcast[Array[SourceInfo]] = null

    override protected def init(): Unit = {
        super.init()
        //加载广播来源地址映射
        val sourceInfos = JAInfoUtils.loadSourceInfos
        sourceInfosBroadcast = SparkExtend.ctx.broadcast(sourceInfos)
      sourceInfos.foreach(println)
    }

    private def loadWeb(inpath: String): RDD[ComFlewBean] = {
        val flowRdd = SparkExtend.ctx.textFile(inpath).map(BeanHandler.toWebAcc)
                .filter(x => x != null && x.logtype == LogType.PAGE)
                .map(x => {
                    val info = RuleHandler.getMeetRule(sourceInfosBroadcast.value.asInstanceOf[Array[StrRule]], x.ref)
                    val sourceid = if (info != null) info.asInstanceOf[SourceInfo].id else 0
                    ComFlewBean(x.appid, x.uuid, x.url, x.time, x.staytime, x.ip, "", sourceid)
                })

        flowRdd
    }

    private def loadApp(inpath: String): RDD[ComFlewBean] = {
        val flowRdd = SparkExtend.ctx.textFile(inpath)
                .map(BeanHandler.toAppPage)
                .filter(_ != null)
                .map(x => {
                    val channel: String = x.dStrInfo("channel", "")
                    ComFlewBean(x.appid, x.uuid, x.pageid, x.time, x.staytime, x.ip, channel, 0)
                })
//      println(SparkExtend.ctx.textFile(inpath).filter(_.contains("1208")).count())
        flowRdd
    }

    private def loadWeiXin(inpath: String): RDD[ComFlewBean] = {
        val flowRdd = SparkExtend.ctx
                .textFile(inpath)
                .map(BeanHandler.toWeiXin)
                .filter(x => x != null && x.logtype == LogType.PAGE)
                .map(x => {
                    val platform: String = {
                        val _platform = x.dStrInfo("platform", "other")
                        if (_platform.length > 0)
                            _platform
                        else
                            "other"
                    }
                    ComFlewBean(x.appid, x.uuid, x.pageid, x.time, x.staytime, x.ip, platform, 0)
                })
      flowRdd.filter(_.appid=="1208").foreach(println)
        flowRdd
    }

    /**
      * 统计APP流量
      *
      * @param spark
      * @param jdbcProp
      */
    private def analyzeAppFlow(spark: SparkSession, jdbcProp: Properties): Unit = {
        //统计总的PV UV IP
        val sql_PvUvIp =
            """select  appid,
              | count(*) as pv_num,
              | count(distinct uuid) as user_num,
              | count(distinct ip) as ip_num,
              | logDate() as day,
              | addTime() as addtime from t_commonLogBase group by appid """.stripMargin

        val pvuvipDf: DataFrame = spark.sql(sql_PvUvIp)

//        pvuvipDf.show(80)
        println(11)
//        pvuvipDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_flow_everyday", jdbcProp)

    }

    /**
      * 统计渠道流量
      *
      * @param spark
      * @param jdbcProp
      */
    private def analyzeChannelFlow(spark: SparkSession, jdbcProp: Properties): Unit = {
        //按渠道统计PV UV IP
        val sql_ChannelPvUvIp =
            """select  appid,
              | channel,
              | count(*) as pv_num,
              | count(distinct uuid) as user_num,
              | count(distinct ip) as ip_num,
              | logDate() as day,
              | addTime() as addtime from t_commonLogBase group by appid,channel """.stripMargin

        val ChannelPvuvipDf: DataFrame = spark.sql(sql_ChannelPvUvIp)
        ChannelPvuvipDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_flow_everyday_source", jdbcProp)
    }

    /**
      * 统计渠道流量
      *
      * @param spark
      * @param jdbcProp
      */
    private def analyzeChannelFlow_Web(spark: SparkSession, jdbcProp: Properties): Unit = {
        //按渠道统计PV UV IP
        val sql_ChannelPvUvIp =
            """select  appid,
              | sourceid,
              | count(*) as pv_num,
              | count(distinct uuid) as user_num,
              | count(distinct ip) as ip_num,
              | logDate() as day,
              | addTime() as addtime from t_commonLogBase where sourceid!=0 group by appid,sourceid """.stripMargin

        val ChannelPvuvipDf: DataFrame = spark.sql(sql_ChannelPvUvIp)
        ChannelPvuvipDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_flow_everyday_source", jdbcProp)
    }

    /**
      * 统计地区流量
      *
      * @param spark
      * @param jdbcProp
      */
    private def analyzeAddressFlow(spark: SparkSession, jdbcProp: Properties, beanRdd: RDD[ComFlewBean]): Unit = {
        //得到代地址信息的DF
        val schema = StructType(List(
            StructField("appid", StringType),
            StructField("uuid", StringType),
            StructField("ip", StringType),
            StructField("country", StringType),
            StructField("province", StringType),
            StructField("city", StringType)
        ))
        val rowRDD = beanRdd.map(bean => {
            val add = IPUtils.getAddress(bean.ip)
            Row(bean.appid, bean.uuid, bean.ip, add.country, add.province, add.city)
        })
        val addDf = spark.createDataFrame(rowRDD, schema)
        addDf.createOrReplaceTempView("add_logBase")
        //按地区统计PV UV IP
        val sql_AddPvUvIp =
            """select appid,
              | country,
              | province,
              | city,
              | count(*) as pv_num,
              | count(distinct uuid) as user_num,
              | count(distinct ip) as ip_num,
              | logDate() as day,
              | addTime() as addtime from add_logBase group by appid,country,province,city """.stripMargin

        val addPvuvipDf: DataFrame = spark.sql(sql_AddPvUvIp)
        addPvuvipDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_flow_everyday_district", jdbcProp)
    }


    override protected def job(args: Array[String]): Unit = {
        //平台参数
        val platform: String = this.getParameterOrElse("platform", "null")

        val beanRdd: RDD[ComFlewBean] = platform match {
            case Platform.PLATFORM_NAME_WEB => loadWeb(this.inPath)
            case Platform.PLATFORM_NAME_MOBILE => loadApp(this.inPath)
            case Platform.PLATFORM_NAME_WEIXIN => loadWeiXin(this.inPath)
            case _ => throw new RuntimeException("no define platform")
        }
        beanRdd.cache()
        beanRdd.checkpoint()

        val spark = SparkSession.builder().appName(this.jobName).getOrCreate()
        import spark.implicits._
        val beanDF = beanRdd.toDF
        beanDF.createOrReplaceTempView("t_commonLogBase")

        //设置公共方法
        spark.udf.register("logDate", () => new java.sql.Date(this.logDate.getTime))
        spark.udf.register("addTime", () => new Timestamp(System.currentTimeMillis()))
        spark.udf.register("_all", () => "all")
        spark.udf.register("getAddress", (ip: String) => IPUtils.getAddress(ip))

        //设置数据库配置
        val prop = new Properties
        prop.setProperty("user", JDBCBasicUtils.getProperty("username"))
        prop.setProperty("password", JDBCBasicUtils.getProperty("password"))

        //统计PV UV IP
        analyzeAppFlow(spark, prop)

//        if (platform == Platform.PLATFORM_NAME_WEB) {
//            analyzeChannelFlow_Web(spark, prop)
//        } else {
//            //统计渠道PV UV IP
//            analyzeChannelFlow(spark, prop)
//        }
//        //统计地区PV UV IP
//        analyzeAddressFlow(spark, prop, beanRdd)

        spark.stop()

    }

}

object ComJobFlow {
    def main(args: Array[String]) {
        val job = new ComJobFlow()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
