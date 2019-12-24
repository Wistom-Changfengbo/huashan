package com.wangjia.bigdata.core.job.mobile

import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import com.wangjia.common.LogEventId
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer


/**
  * 分析APP占比
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  *
  * Created by Administrator on 2017/3/22.
  */
class MobileJobAppRatioAndList extends EveryDaySparkJob {

    /**
      * 加载APPLIST Rdd
      *
      * @param path 路径
      * @return
      */
    private def loadAppListRdd(path: String): RDD[MobileLogMsg] = {
        val sc = SparkExtend.ctx
        try {
            val linesRdd = sc.textFile(path)
            val appRdd = linesRdd.map(BeanHandler.toAppEvent(LogEventId.APP_APPLIST) _).filter(_ != null)

            val lastAppEventRdd = appRdd.map(x => (x.uuid, x))
                    .reduceByKey((x1, x2) => if (x1.time > x2.time) x1 else x2)
                    .map(_._2)
            lastAppEventRdd
        } catch {
            case e: Exception => e.printStackTrace(); null
        }
    }

    /**
      * 分析应用占比
      *
      * @param appListRdd
      */
    private def analyzeAppRatio(appListRdd: RDD[MobileLogMsg]): Unit = {
        //APP RDD[platform,uuid,packgename,appname]
        val appRdd = appListRdd.flatMap(bean => {
            //platform,uuid,packgename appname
            val appList = new ListBuffer[(String, String, String, String)]
            try {
                val app_josns = JSON.parseObject(bean.data).getJSONArray("app_list")
                var i = 0
                val maxapp = app_josns.size()
                val platform = bean.dStrInfo("platform")
                val uuid = bean.uuid
                while (i < maxapp) {
                    val appj = app_josns.getJSONObject(i)
                    val pkg_name = appj.getString("pkg_name")
                    val app_name = appj.getString("app_name")
                    if (app_name.length >= 1) {
                        appList += ((platform, uuid, pkg_name, app_name))
                    }
                    i += 1
                }
            } catch {
                case e: Exception => e.printStackTrace()
            }
            appList
        })
//        appRdd.take(10).foreach(println)
        //IOS总的设备数
        val sumIosDevice = appRdd.filter(_._1 == "ios").map(_._2).distinct().count() * 1.0
        //android总的设备数
        val sumAndroidDevice = appRdd.filter(_._1 == "android").map(_._2).distinct().count() * 1.0

        //过滤掉小于5/1000的APP
        val resultsRdd = appRdd.map(x => (x._1, x._3, x._4))
                //((platform,pkg_name),((platform,pkg_name, app_name),1)
                .map(x => ((x._1, x._2), (x, 1)))
                .reduceByKey((x1, x2) => (x1._1, x1._2 + x2._2))
                .filter(x => {
                    if (x._2._1._1 == "ios" && x._2._2 >= sumIosDevice * 0.001)
                        true
                    else if (x._2._1._1 == "android" && x._2._2 >= sumAndroidDevice * 0.001)
                        true
                    else
                        false
                })
        //写入数据库
        resultsRdd.foreachPartition(iterator => {
            var i = 0
            val date = new java.sql.Date(this.logDate.getTime)
            JDBCBasicUtils.insertBatch("sql_data_app_ratio", pstmt => {
                while (iterator.hasNext) {
                    val b = iterator.next()
                    try {
                        pstmt.setString(1, b._2._1._1)
                        pstmt.setString(2, b._2._1._3)
                        pstmt.setString(3, b._2._1._2)
                        pstmt.setLong(4, b._2._2)
                        if (b._1._1 == "ios")
                            pstmt.setDouble(5, b._2._2 / sumIosDevice)
                        else
                            pstmt.setDouble(5, b._2._2 / sumAndroidDevice)
                        pstmt.setDate(6, date)

                        pstmt.addBatch()
                        i += 1
                        if (i > 100) {
                            pstmt.executeBatch()
                            pstmt.clearBatch()
                            i = 0
                        }
                    } catch {
                        case e: Exception => e.printStackTrace(); pstmt.clearBatch(); i = 0; println(b)
                    }
                }
            })
        })
    }

    /**
      * 分析应用列表
      *
      * @param appListRdd
      */
    private def analyzeApList(appListRdd: RDD[MobileLogMsg]): Unit = {
        //APP RDD[platform,packgename,appname]
        val appRdd = appListRdd.flatMap(bean => {
            //platform packgename appname
            val appList = new ListBuffer[((String, String, String), Long)]
            try {
                val app_josns = JSON.parseObject(bean.data).getJSONArray("app_list")
                var i = 0
                val maxapp = app_josns.size()
                val platform = bean.dStrInfo("platform")
                while (i < maxapp) {
                    val appj = app_josns.getJSONObject(i)
                    val pkg_name = appj.getString("pkg_name")
                    val app_name = appj.getString("app_name")
                    if (app_name.length >= 1) {
                        appList += (((platform, pkg_name, app_name), bean.time))
                    }
                    i += 1
                }
            } catch {
                case e: Exception => e.printStackTrace()
            }
            appList
        })

        //去重取最小时间
        val appDistinctRdd = appRdd.reduceByKey((x1, x2) => if (x1 < x2) x1 else x2)

        //写入数据库
        appDistinctRdd.foreachPartition(iterator => {
            var i = 0
            val date = new java.sql.Date(this.logDate.getTime)
            val newDate = new Timestamp(System.currentTimeMillis())
            JDBCBasicUtils.insertBatch("sql_data_app_list", pstmt => {
                while (iterator.hasNext) {
                    val b = iterator.next()
                    try {
                        pstmt.setString(1, b._1._1)
                        pstmt.setString(2, b._1._2)
                        pstmt.setString(3, b._1._3)
                        pstmt.setTimestamp(4, new Timestamp(b._2))
                        pstmt.setDate(5, date)
                        pstmt.setTimestamp(6, newDate)
                        pstmt.addBatch()
                        i += 1
                        if (i > 100) {
                            pstmt.executeBatch()
                            pstmt.clearBatch()
                            i = 0
                        }
                    } catch {
                        case e: Exception => e.printStackTrace(); pstmt.clearBatch(); i = 0; println(b)
                    }
                }
            })
        })
    }

    override protected def job(args: Array[String]): Unit = {
        val appListRdd: RDD[MobileLogMsg] = loadAppListRdd(this.inPath)
        appListRdd.cache()
        appListRdd.checkpoint()

        //分析应用占比
        analyzeAppRatio(appListRdd)

        //分析应用列表
        analyzeApList(appListRdd)
    }
}


object MobileJobAppRatioAndList {
    def main(args: Array[String]) {
        val job = new MobileJobAppRatioAndList()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
