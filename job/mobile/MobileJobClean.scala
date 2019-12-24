package com.wangjia.bigdata.core.job.mobile

import java.sql.Timestamp

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.gson.JsonObject
import com.wangjia.bigdata.core.bean.info.{AccountInfo, AppInfo}
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{BeanHandler, LogClearHandler, LogClearStatus}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.{LogEventId, LogType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * 清洗APP端日志
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --outPath   日志输出目录
  *
  * Created by Administrator on 2017/3/22.
  */
class MobileJobClean extends EveryDaySparkJob {
    //页面最大停留时间
    private val PAGE_MAX_STAY_TIME: Long = 10 * 60 * 1000

    //应用信息
    private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null
    private var mapIdentifierAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

    //帐号信息
    private var mapAccountInfosBroadcast: Broadcast[mutable.Map[Int, AccountInfo]] = null


    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx

        //加载广播帐号信息
        val mapAccountInfos = JAInfoUtils.loadAccountInfos
        mapAccountInfosBroadcast = sc.broadcast(mapAccountInfos)

        //加载广播应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos
        mapAppInfosBroadcast = sc.broadcast(mapAppInfos)

        val mapIdAppInfos = mutable.Map[String, AppInfo]()
        mapAppInfos.foreach(i => {
            if (i._2.identifier != "") {
                mapIdAppInfos.put(i._2.identifier, i._2)
            }
        })
        mapIdentifierAppInfosBroadcast = sc.broadcast(mapIdAppInfos)
    }

    private def countStayTimeVDef(beans: List[MobileLogMsg]): Unit = {
        val recentPageJson: JSONObject = new JSONObject()
        //计算停留时间
        var pageBean: MobileLogMsg = null
        beans.foreach(bean => {
            if (pageBean == null && bean.logtype == LogType.PAGE) {
                pageBean = bean
            } else if (pageBean != null) {
                recentPageJson.clear()
                recentPageJson.put("pageid", pageBean.pageid)
                recentPageJson.put("time", pageBean.time)
                recentPageJson.put("data", JSON.parseObject(pageBean.data))
                recentPageJson.put("title", pageBean.title)
                bean.recentPage = recentPageJson.toJSONString

                if (bean.logtype == LogType.PAGE) {
                    if (pageBean.staytime == 0)
                        pageBean.staytime = bean.time - pageBean.time
                    pageBean = bean
                } else if (bean.logtype == LogType.EVENT && bean.subtype == LogEventId.APP_ENTER_BACKGROUND) {
                    if (pageBean.staytime == 0)
                        pageBean.staytime = bean.time - pageBean.time
                    pageBean = null
                }
            }
        })
    }

    private def getNextPage(beans: List[MobileLogMsg], formIndex: Int): MobileLogMsg = {
        val max = beans.size
        var i = formIndex
        while (i < max) {
            if (beans(i).logtype == LogType.PAGE)
                return beans(i)
            i += 1
        }
        null
    }

    private def countStayTimeV2(beans: List[MobileLogMsg]): Unit = {
        val recentPageJson: JSONObject = new JSONObject()
        var pageBean: MobileLogMsg = null
        var upCloseTime: Long = beans.head.time
        var i: Int = 0
        val max: Int = beans.size

        while (i < max) {
            val bean = beans(i)
            if (bean.logtype == LogType.EVENT && bean.subtype == LogEventId.APP_ENTER_FOREGROUND)
                upCloseTime = bean.time
            if (bean.logtype == LogType.PAGE) {
                if (pageBean != null) {
                    recentPageJson.clear()
                    recentPageJson.put("pageid", pageBean.pageid)
                    recentPageJson.put("time", pageBean.time)
                    recentPageJson.put("data", JSON.parseObject(pageBean.data))
                    recentPageJson.put("title", pageBean.title)
                    bean.recentPage = recentPageJson.toJSONString
                }
                bean.staytime = bean.time - upCloseTime
                val closeTime = bean.time
                bean.time = upCloseTime
                upCloseTime = closeTime
                pageBean = bean
            } else if (bean.logtype == LogType.EVENT) {
                val rpage = getNextPage(beans, i + 1)
                if (rpage != null) {
                    recentPageJson.clear()
                    recentPageJson.put("pageid", rpage.pageid)
                    recentPageJson.put("time", rpage.time)
                    recentPageJson.put("data", JSON.parseObject(rpage.data))
                    recentPageJson.put("title", rpage.title)
                    bean.recentPage = recentPageJson.toJSONString
                }
            }
            i += 1
        }
    }

    override protected def job(args: Array[String]): Unit = {
        val sc = SparkExtend.ctx

        if (!Config.DEBUG) {
            val hadoopConf: Configuration = sc.hadoopConfiguration
            val hdfs: FileSystem = FileSystem.get(hadoopConf)
            val path: Path = new Path(this.outPath)
            if (hdfs.exists(path)) {
                hdfs.delete(path, true)
            }
        }

        //读取文件
        val linesRdd = sc.textFile("G:\\LOG\\app\\log\\2019\\03\\15")
      println(linesRdd.count())
        //清洗
        val clearBeanRdd = linesRdd.map(x => LogClearHandler.clearMobileLog(x, mapAppInfosBroadcast.value, mapIdentifierAppInfosBroadcast.value, mapAccountInfosBroadcast.value))
        clearBeanRdd.filter(_._1 == LogClearStatus.SUCCESS).
          flatMap(_._2.asInstanceOf[ListBuffer[MobileLogMsg]]).map(x=>(x.appid,1)).reduceByKey((x1,x2)=>x1+x2).repartition(1).saveAsTextFile("G:\\\\LOG\\\\app\\\\clean1")


        //得到日志对象
        val logBeanRdd: RDD[MobileLogMsg] = clearBeanRdd.filter(_._1 == LogClearStatus.SUCCESS)
                .flatMap(_._2.asInstanceOf[ListBuffer[MobileLogMsg]])
                .map(x => ((x.uuid, x.logtype, x.subtype, x.appid, x.localTime), x))
                .reduceByKey((x1, x2) => x1)
                .map(_._2)

        //非法UUID日志对象
        val eUUIDBeanRdd: RDD[MobileLogMsg] = logBeanRdd.filter(x => x.uuid == null || x.uuid.length != 32)

        //计算时间和会话
        val msgBeanRdd = logBeanRdd.filter(x => x.uuid != null && x.uuid.length == 32)
                .groupBy(x => (x.uuid, x.appid))
                .flatMap(x => {
                    val beans: List[MobileLogMsg] = x._2.toList.sortWith(_.time < _.time)
                    if (beans.head.ja_version.startsWith("2."))
                        countStayTimeV2(beans)
                    else
                        countStayTimeVDef(beans)

                    //计算会话次数，停留时间
                    var bSwitchover: Boolean = false
                    var visitindex: Int = 1
                    var upTime: Long = beans.head.time
                    beans.foreach(bean => {
                        if (bSwitchover && bean.logtype == LogType.PAGE) {
                            visitindex += 1
                            bSwitchover = false
                        } else if (bSwitchover && bean.logtype == LogType.EVENT && bean.subtype == LogEventId.APP_ENTER_FOREGROUND) {
                            visitindex += 1
                            bSwitchover = false
                        }
                        bean.visitindex = visitindex
                        if (bean.staytime >= PAGE_MAX_STAY_TIME) {
                            bean.staytime = 0
                            bSwitchover = true
                        } else if (bean.time - upTime >= PAGE_MAX_STAY_TIME) {
                            bSwitchover = true
                        }
                        upTime = bean.time
                    })
                    beans
                })

        //写入文件
//             msgBeanRdd.union(eUUIDBeanRdd)
        //                .map(BeanHandler.toString)
        //                .repartition(8)
        //                .saveAsTextFile("G:\\LOG\\app\\clean")

//        //写入记录
//        val mapClearStatusNum: Map[Int, Int] = clearBeanRdd.map(x => (x._1, 1)).reduceByKey(_ + _).collect().toMap
//        println(mapClearStatusNum)
//        val allLogNum: Int = mapClearStatusNum.toList.map(_._2).sum
//        val rightLogNum: Int = mapClearStatusNum.getOrElse(LogClearStatus.SUCCESS, 0)
//        val errorLogNum: Int = allLogNum - rightLogNum
//        val time = (System.currentTimeMillis() - jobStartTime) / 1000
//        JDBCBasicUtils.insert("sql_insert_log_job_clean", pstmt => {
//            pstmt.setLong(1, allLogNum)
//            pstmt.setLong(2, errorLogNum)
//            pstmt.setLong(3, rightLogNum)
//            pstmt.setString(4, inPath)
//            pstmt.setString(5, outPath)
//            pstmt.setString(6, jobName)
//            pstmt.setLong(7, time.toLong)
//            pstmt.setDate(8, new java.sql.Date(this.logDate.getTime))
//            pstmt.setTimestamp(9, new Timestamp(System.currentTimeMillis()))
//        })
//
//        val json = new JsonObject
//        mapClearStatusNum.foreach(b => json.addProperty(b._1.toString, b._2))
//        this.setRunLog(json.toString)

//        msgBeanRdd.filter(_.uuid=="59fdd706fa79ba41dd127d42fe705a4c")
//                .repartition(1)
//                .sortBy(_.time)
//                .foreach(println)
    }
}

object MobileJobClean {
    def main(args: Array[String]) {
        val job = new MobileJobClean()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}