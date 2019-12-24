package com.wangjia.bigdata.core.job.common

import java.sql.Timestamp
import java.util.Properties

import com.wangjia.bigdata.core.bean.info.PageInfo
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{IPUtils, JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.{LogType, Platform}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * 统计各个平台的，各个渠道页面的信息
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --platform  平台:web,app，weixin
  *
  * Created by Cfb on 2018/2/26.
  */

case class ComPageBean(appid: String, uuid: String, pageRule: String, time: Long, stayTime: Long, ip: String, channel: String, visitIndex: Int, pageid: Int)

class ComJobPage extends EveryDaySparkJob {
    //页面规则
    private var pageRuleInfosBroadcast: Broadcast[mutable.Map[String, Array[PageInfo]]] = null

    //页面规则Map
    private var mapPageRuleInfosBroadcast: Broadcast[Map[Int, PageInfo]] = null

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx

        //加载广播页面规则
        val apps = JAInfoUtils.loadAppInfos
        val pageRuleInfos = JAInfoUtils.loadPageInfoByAppInfos(apps)
        pageRuleInfosBroadcast = sc.broadcast(pageRuleInfos)

        //加载广播页面规则Map
        val map = pageRuleInfos.flatMap(_._2).map(x => (x.id, x)).toMap
        mapPageRuleInfosBroadcast = sc.broadcast(map)
    }


    /**
      * 分析页面时间 按页面 统计
      *
      * @param spark
      * @param jdbcProp
      */
    private def analyzePageFlow(spark: SparkSession, jdbcProp: Properties): Unit = {
        //按渠道统计PV UV IP
        val sql_pagePvUvIp =
            """select appid,
              | pageid,
              | count(*) as pv_num,
              | count(distinct uuid) as user_num,
              | count(distinct ip) as ip_num,
              | logDate() as day,
              | addTime() as addtime from t_com_page_bean group by appid,pageid """.stripMargin

        val pagePvuvipDf: DataFrame = spark.sql(sql_pagePvUvIp)
        pagePvuvipDf.createOrReplaceTempView("t_page_pv_uv_ip_num")
        pagePvuvipDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_page_everyday_flow", jdbcProp)
    }

    /**
      * 分析页面时间 按页面 统计
      *
      * @param spark
      * @param jdbcProp
      */
    private def analyzePageTime(spark: SparkSession, jdbcProp: Properties): Unit = {
        //分析页面时间 按页面 统计
        val sql_pageTime =
            """select appid,
              | pageid,
              | sum(stayTime) as sum_time,
              | count(*) as num,
              | logDate() as day,
              | addTime() as addtime from t_com_page_bean group by appid,pageid """.stripMargin

        val pageTimeDf: DataFrame = spark.sql(sql_pageTime)
        pageTimeDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_page_everyday_time", jdbcProp)
    }

    /**
      * 分析页面流失 按页面 统计
      *
      * @param spark
      * @param jdbcProp
      * @param userVisitRdd
      */
    private def analyzePageRunOff(spark: SparkSession,
                                  jdbcProp: Properties,
                                  userVisitRdd: RDD[((String, String, Int), List[ComPageBean])]): Unit = {
        //流失RDD RDD[Row(APPID,PageId,该流失数,总流失数)]1552652842610,1,87  1552652869021
//      ((1100,5685bb48966c8681b5e5a38ecd273d48,1),
      // List(ComPageBean(1100,5685bb48966c8681b5e5a38ecd273d48,main_tab_case_list,1552652842610,26411,111.41.131.212,AppStore,1,87), ComPageBean(1100,5685bb48966c8681b5e5a38ecd273d48,main_tab_case_list,1552652869021,10061,111.41.131.212,AppStore,1,87), ComPageBean(1100,5685bb48966c8681b5e5a38ecd273d48,main_tab_photo_list,1552652879082,2447,111.41.131.212,AppStore,1,91), ComPageBean(1100,5685bb48966c8681b5e5a38ecd273d48,search_photo,1552652881529,3297,111.41.131.212,AppStore,1,92), ComPageBean(1100,5685bb48966c8681b5e5a38ecd273d48,search_case,1552652884826,984,111.41.131.212,AppStore,1,88), ComPageBean(1100,5685bb48966c8681b5e5a38ecd273d48,search_photo,1552652885810,9228,111.41.131.212,AppStore,1,92), ComPageBean(1100,5685bb48966c8681b5e5a38ecd273d48,main_tab_photo_list,1552652895038,365,111.41.131.212,AppStore,1,91)))

      val runOffRdd: RDD[Row] = userVisitRdd.map(_._2.last)
                .map(x => ((x.appid, x.pageid), 1))
                .reduceByKey(_ + _)
                .map(x => (x._1._1, (x._1._2, x._2))) //(APPID,(PageId,流失数))
                .groupByKey()
                .flatMap(x => {
                    //APPID,PageId,该流失数,总流失数
                    val beans = new ListBuffer[Row]
                    val appid: String = x._1
                    val offList = x._2.toList
                    val access_sum: Int = offList.map(_._2).sum
                    offList.foreach(b => beans += Row(appid, b._1, b._2, access_sum))
                    beans
                })

        val schema = StructType(List(
            StructField("appid", StringType),
            StructField("pageid", IntegerType),
            StructField("num", IntegerType),
            StructField("access_sum", IntegerType)
        ))
        val runOffDf = spark.createDataFrame(runOffRdd, schema)
        runOffDf.createOrReplaceTempView("t_page_runoff_bean")
        //分析页面流失 按页面 统计
        val sql_pageOff =
            """select
              | t_page_runoff_bean.appid as appid,
              | t_page_runoff_bean.pageid as pageid,
              | t_page_runoff_bean.num as num,
              | t_page_runoff_bean.access_sum as access_sum,
              | t_page_pv_uv_ip_num.pv_num as pv_num,
              | logDate() as day,
              | addTime() as addtime from
              | t_page_runoff_bean join t_page_pv_uv_ip_num
              | on
              | t_page_runoff_bean.appid = t_page_pv_uv_ip_num.appid
              | and
              | t_page_runoff_bean.pageid = t_page_pv_uv_ip_num.pageid""".stripMargin

        val pageOffDf: DataFrame = spark.sql(sql_pageOff)
        pageOffDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_page_everyday_runoff", jdbcProp)
    }

    /**
      * 分析页面跳转 按页面 统计
      *
      * @param spark
      * @param jdbcProp
      * @param userVisitRdd
      */
    private def analyzePageJump(spark: SparkSession,
                                jdbcProp: Properties,
                                userVisitRdd: RDD[((String, String, Int), List[ComPageBean])]): Unit = {
        //APPID,SourecePageId,SetpNum,PageId,NextPageId
        val pageJumpRdd: RDD[Row] = userVisitRdd.flatMap(x => {
            val appid: String = x._1._1
            val uuid: String = x._1._2
            val visitIndex: Int = x._1._3
            //APPID,SourecePageId,SetpNum,PageId,NextPageId
            val beans = new ListBuffer[Row]
            val sourcePageId: Int = x._2.head.pageid
            var pageId: Int = x._2.head.pageid
            var stepsNum: Int = 1
            //PageID去重
            val visitPageIds = new ListBuffer[Int]
            visitPageIds += x._2.head.pageid
            x._2.foreach(b => {
                if (b.pageid != visitPageIds.last) {
                    visitPageIds += b.pageid
                }
            })

            visitPageIds.foreach(b => {
                if (b != pageId) {
                    beans += Row(appid, sourcePageId, stepsNum, pageId, b)
                    stepsNum += 1
                    pageId = b
                }
            })
            beans += Row(appid, sourcePageId, stepsNum, pageId, 0)
            beans
        })

        val schema = StructType(List(
            StructField("appid", StringType),
            StructField("sourceid", IntegerType),
            StructField("steps_num", IntegerType),
            StructField("pageid", IntegerType),
            StructField("next_pageid", IntegerType)
        ))
        val pageJumpDf = spark.createDataFrame(pageJumpRdd, schema)
        pageJumpDf.createOrReplaceTempView("t_page_jump_bean")

        val sql_pageJumpSource =
            """select appid,
              | sourceid,
              | steps_num,
              | pageid,
              | next_pageid,
              | count(*) as num,
              | logDate() as day,
              | addTime() as addtime from t_page_jump_bean group by appid,sourceid,steps_num,pageid,next_pageid """.stripMargin

        val pageJumpSourceDf: DataFrame = spark.sql(sql_pageJumpSource)
        pageJumpSourceDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_page_everyday_jump_source", jdbcProp)

        val sql_pageJump =
            """select appid,
              | steps_num,
              | pageid,
              | next_pageid,
              | count(*) as num,
              | logDate() as day,
              | addTime() as addtime from t_page_jump_bean group by appid,steps_num,pageid,next_pageid """.stripMargin

        val rePageJumpDf: DataFrame = spark.sql(sql_pageJump)
        rePageJumpDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_page_everyday_jump", jdbcProp)
    }

    private def loadWeb(inpath: String): RDD[ComPageBean] = {
        val pageRdd = SparkExtend.ctx
                .textFile(inpath)
                .map(BeanHandler.toWebAcc)
                .filter(x => x != null && x.logtype == LogType.PAGE)
                .flatMap(x => {
                    val beans = new ListBuffer[ComPageBean]()
                    val pageInfo = BeanHandler.getPageInfo(pageRuleInfosBroadcast.value, x.appid, x.url)
                    beans += ComPageBean(x.appid, x.uuid, "", x.time, x.staytime, x.ip, "", x.visitindex, if (pageInfo != null) pageInfo.id else 0)
                    val ids = BeanHandler.getPageInfoLooseIds(pageRuleInfosBroadcast.value, x.appid, x.url)
                    ids.foreach(id => {
                        beans += ComPageBean(x.appid, x.uuid, "", x.time, x.staytime, x.ip, "", x.visitindex, id)
                    })
                    beans
                })
        pageRdd
    }

    private def loadApp(inpath: String): RDD[ComPageBean] = {
        val pageRdd = SparkExtend.ctx
                .textFile(inpath)
                .map(BeanHandler.toAppPage)
                .filter(_ != null)
                .flatMap(x => {
                    val beans = new ListBuffer[ComPageBean]()
                    val channel: String = x.dStrInfo("channel", "")
                    val pageInfo = BeanHandler.getPageInfo(pageRuleInfosBroadcast.value, x.appid, x.pageid)
                    beans += ComPageBean(x.appid, x.uuid, x.pageid, x.time, x.staytime, x.ip, channel, x.visitindex, if (pageInfo != null) pageInfo.id else 0)
                    val ids = BeanHandler.getPageInfoLooseIds(pageRuleInfosBroadcast.value, x.appid, x.pageid)
                    ids.foreach(id => {
                        beans += ComPageBean(x.appid, x.uuid, x.pageid, x.time, x.staytime, x.ip, channel, x.visitindex, id)
                    })
                    beans
                })
        pageRdd
    }

    private def loadWeiXin(inpath: String): RDD[ComPageBean] = {
        val pageRdd = SparkExtend.ctx
                .textFile(inpath)
                .map(BeanHandler.toWeiXin)
                .filter(x => x != null && x.logtype == LogType.PAGE)
                .flatMap(x => {
                    val beans = new ListBuffer[ComPageBean]()
                    val platform: String = x.dStrInfo("platform", "other")
                    val pageInfo = BeanHandler.getPageInfo(pageRuleInfosBroadcast.value, x.appid, x.pageid)
                    beans += ComPageBean(x.appid, x.uuid, x.pageid, x.time, x.staytime, x.ip, platform, x.visitindex, if (pageInfo != null) pageInfo.id else 0)
                    val ids = BeanHandler.getPageInfoLooseIds(pageRuleInfosBroadcast.value, x.appid, x.pageid)
                    ids.foreach(id => {
                        beans += ComPageBean(x.appid, x.uuid, x.pageid, x.time, x.staytime, x.ip, platform, x.visitindex, id)
                    })
                    beans
                })
        pageRdd
    }

    /**
      * 任务入口方法
      *
      * @param args
      */
    override protected def job(args: Array[String]): Unit = {
        //平台参数
        val platform = this.getParameterOrElse("platform", "app")
        val pageRdd: RDD[ComPageBean] = platform match {
            case Platform.PLATFORM_NAME_WEB => loadWeb(this.inPath)
            case Platform.PLATFORM_NAME_MOBILE => loadApp("G:\\LOG\\app\\clean\\")
            case Platform.PLATFORM_NAME_WEIXIN => loadWeiXin(this.inPath)
            case _ => throw new RuntimeException("no define platform")
        }

        pageRdd.cache()
        pageRdd.checkpoint()

        val spark = SparkSession.builder().appName(this.jobName).getOrCreate()
        import spark.implicits._

        val beanDF = pageRdd.toDF
        beanDF.createOrReplaceTempView("t_com_page_bean")

        //设置公共方法
        spark.udf.register("logDate", () => new java.sql.Date(this.logDate.getTime))
        spark.udf.register("addTime", () => new Timestamp(System.currentTimeMillis()))
        spark.udf.register("_all", () => "all")
        spark.udf.register("getAddress", (ip: String) => IPUtils.getAddress(ip))

        //设置数据库配置
        val prop = new Properties
        prop.setProperty("user", JDBCBasicUtils.getProperty("username"))
        prop.setProperty("password", JDBCBasicUtils.getProperty("password"))

        //分析页面流量
        analyzePageFlow(spark, prop)

        //分析页面时间
        analyzePageTime(spark, prop)

        //用户访问会话  RDD[((appid, uuid, visit), List[ComPageBean])]
        val userVisitRdd: RDD[((String, String, Int), List[ComPageBean])] = pageRdd
                .filter(x => {
                    val info = mapPageRuleInfosBroadcast.value.getOrElse(x.pageid, null)
                    if (info != null && info.loose == 1)
                        false
                    else
                        true
                })
                .groupBy(x => (x.appid, x.uuid, x.visitIndex))
                .mapValues(_.toList.sortWith(_.time < _.time))
      userVisitRdd.repartition(1).saveAsTextFile("G:\\LOG\\app\\test")


        //分析页面流失
        analyzePageRunOff(spark, prop, userVisitRdd)

        //分析页面跳转
        analyzePageJump(spark, prop, userVisitRdd)

        spark.stop()
    }
}

object ComJobPage {
    def main(args: Array[String]) {
        val job = new ComJobPage()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}

