package com.wangjia.bigdata.core.job.common

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Properties

import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.{DeviceType, LogType, Platform}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 统计各个平台的信息，补全流量信息
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --platform  平台:web,app
  *
  * Created by Cfb on 2018/2/26.
  */

case class ComMakeUpFlew(appId: String, uuid: String, ip: String, timeZone: Long, time: Long)

class ComJobMakeUp extends EveryDaySparkJob {
    private val MINUTE_PERIOD = 60 * 1000
    private val HOUR_PERIOD = 60 * 60 * 1000
    private val DAY = 24 * 3600 * 1000
    private var appInfo: Broadcast[mutable.Map[String, AppInfo]] = null
    private var platform: String = null

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx
        appInfo = sc.broadcast(JAInfoUtils.loadAppInfos)
    }

    private def totalMinAndHour(sc: SparkContext, appInfo: Broadcast[mutable.Map[String, AppInfo]], allHour: RDD[((Long, String), ((Int, Int), Int))], allMinute: RDD[((Long, String), ((Int, Int), Int))]): (RDD[((Long, String), ((Int, Int), Int))], RDD[((Long, String), ((Int, Int), Int))]) = {
        val rangeMinute: Array[Long] = Array.range(0, 1440).map(x => x.toLong)
        val rangeHour: Array[Long] = Array.range(0, 24).map(x => x.toLong)
        var list: List[String] = Nil

        if (platform == Platform.PLATFORM_NAME_MOBILE) {
            //只把type==2，3的appid拿出来，是app的
            list = appInfo.value.filter((k) => {
                k._2.appType == DeviceType.DEVICE_PHONE_IOS || k._2.appType == DeviceType.DEVICE_PHONE_ANDROID
            }).map(_._1).toList
        } else if (platform == Platform.PLATFORM_NAME_WEB) {
            //只把type==1的appid拿出来，是web的
            list = appInfo.value.filter((k) => {
                k._2.appType == DeviceType.DEVICE_PC_BROWSER
            }).map(_._1).toList
        }

        //(0,1108) //把每个分钟跟appid拼到一起，并做成key，value形式去join
        val minuteRDD = sc.makeRDD(rangeMinute.flatMap(x => {
            val tuples = new ListBuffer[(Long, String)]
            for (appid: String <- list) {
                tuples += ((x, appid))
            }
            tuples
        })).map(x => (x, ((0, 0), 0)))
        //把每个小时跟appid拼到一起，并做成key，value形式去join
        val hourRDD = sc.makeRDD(rangeHour.flatMap(x => {
            val tuples = new ListBuffer[(Long, String)]
            for (appid: String <- list) {
                tuples += ((x, appid))
            }
            tuples
        })).map(x => (x, ((0, 0), 0)))

        //跟以前有值的数据补到一起并去重
        val totalMinute: RDD[((Long, String), ((Int, Int), Int))] = allMinute.union(minuteRDD).reduceByKey((x1, x2) => {
            ((x1._1._1 + x2._1._1, x1._1._2 + x2._1._2), x1._2 + x2._2)
        })

        //跟以前有值的数据补到一起并去重
        val totalHour: RDD[((Long, String), ((Int, Int), Int))] = allHour.union(hourRDD).reduceByKey((x1, x2) => {
            ((x1._1._1 + x2._1._1, x1._1._2 + x2._1._2), x1._2 + x2._2)
        })

        (totalHour, totalMinute)
    }

    private def loadWeb(inpath: String): (RDD[((Long, String), ((Int, Int), Int))], RDD[((Long, String), ((Int, Int), Int))]) = {
        val beanRdd = SparkExtend.ctx
                .textFile(inpath)
                .map(BeanHandler.toWebAll)
                .filter(x => x != null && x.logtype == LogType.PAGE)
        //把不是当天的数据清除掉  按小时统计
        val flewBeanH = beanRdd.map(x => {
            val dTime = x.time - this.logDate.getTime
            if (dTime >= 0 && dTime < DAY)
                ComMakeUpFlew(x.appid, x.uuid, x.ip, dTime / HOUR_PERIOD, x.time)
            else
                null
        }).filter(_ != null)
        //把不是当天的数据清除掉  按分钟统计
        val flewBeanM = beanRdd.map(x => {
            val dTime = x.time - this.logDate.getTime
            if (dTime >= 0 && dTime < DAY)
                ComMakeUpFlew(x.appid, x.uuid, x.ip, dTime / MINUTE_PERIOD, x.time)
            else
                null
        }).filter(_ != null)

        (periodCal(flewBeanH), periodCal(flewBeanM))
    }

    private def loadApp(inpath: String): (RDD[((Long, String), ((Int, Int), Int))], RDD[((Long, String), ((Int, Int), Int))]) = {

        val beanRdd = SparkExtend.ctx
                .textFile(inpath)
                .map(BeanHandler.toAppAll)
                .filter(x => x != null && x.logtype == LogType.PAGE)
        //把不是当天的数据清除掉  按小时统计
        val flewBeanH = beanRdd.map(x => {
            val dTime = x.time - this.logDate.getTime
            if (dTime >= 0 && dTime < DAY)
                ComMakeUpFlew(x.appid, x.uuid, x.ip, dTime / HOUR_PERIOD, x.time)
            else
                null
        }).filter(_ != null)
        //把不是当天的数据清除掉  按分钟统计
        val flewBeanM = beanRdd.map(x => {
            val dTime = x.time - this.logDate.getTime
            if (dTime >= 0 && dTime < DAY)
                ComMakeUpFlew(x.appid, x.uuid, x.ip, dTime / MINUTE_PERIOD, x.time)
            else
                null
        }).filter(_ != null)

        (periodCal(flewBeanH), periodCal(flewBeanM))
    }

    private def loadWeiXin(inpath: String): (RDD[((Long, String), ((Int, Int), Int))], RDD[((Long, String), ((Int, Int), Int))]) = {
        val beanRdd = SparkExtend.ctx
                .textFile(inpath)
                .map(BeanHandler.toWeiXin)
                .filter(x => x != null && x.logtype == LogType.PAGE)
        //把不是当天的数据清除掉  按小时统计
        val flewBeanH = beanRdd.map(x => {
            val dTime = x.time - this.logDate.getTime
            if (dTime >= 0 && dTime < DAY)
                ComMakeUpFlew(x.appid, x.uuid, x.ip, dTime / HOUR_PERIOD, x.time)
            else
                null
        }).filter(_ != null)
        //把不是当天的数据清除掉  按分钟统计
        val flewBeanM = beanRdd.map(x => {
            val dTime = x.time - this.logDate.getTime
            if (dTime >= 0 && dTime < DAY)
                ComMakeUpFlew(x.appid, x.uuid, x.ip, dTime / MINUTE_PERIOD, x.time)
            else
                null
        }).filter(_ != null)

        (periodCal(flewBeanH), periodCal(flewBeanM))
    }


    //计算出按小时或者按分钟的pv，uv，ip
    private def periodCal(flewBean: RDD[ComMakeUpFlew]): RDD[((Long, String), ((Int, Int), Int))] = {
        val pvs = flewBean.map(x => ((x.timeZone, x.appId), 1)).reduceByKey(_ + _)
        val uvs = flewBean.map(x => (x.timeZone, x.appId, x.uuid)).distinct().map(x => ((x._1, x._2), 1)).reduceByKey(_ + _)
        val ips = flewBean.map(x => (x.timeZone, x.appId, x.ip)).distinct().map(x => ((x._1, x._2), 1)).reduceByKey(_ + _)
        val cal: RDD[((Long, String), ((Int, Int), Int))] = pvs.join(uvs).join(ips)
        cal
    }

    /**
      * 任务入口方法
      *
      * @param args
      */
    override protected def job(args: Array[String]): Unit = {
        val sc = SparkExtend.ctx
        //平台参数
        platform = this.getParameterOrElse("platform", "null")
        val beanRdd = platform match {
            case Platform.PLATFORM_NAME_WEB => loadWeb(this.inPath)
            case Platform.PLATFORM_NAME_MOBILE => loadApp(this.inPath)
            case Platform.PLATFORM_NAME_WEIXIN => loadWeiXin(this.inPath)
            case _ => throw new RuntimeException("no define platform")
        }
        //allHour,allMinute
        val totalRDD = totalMinAndHour(sc, appInfo, beanRdd._1, beanRdd._2)

        //将按分钟，按小时的存入数据库
        saveToJdbc(totalRDD._1, totalRDD._2)
    }


    def saveToJdbc(totalHour: (RDD[((Long, String), ((Int, Int), Int))]), totalMinute: (RDD[((Long, String), ((Int, Int), Int))])): Unit = {
        val sdf = new SimpleDateFormat("yyyyMM")
        val format: String = sdf.format(this.logDate)

        val url = JDBCBasicUtils.getProperty("makeup.url")
        val username = JDBCBasicUtils.getProperty("makeup.username")
        val password = JDBCBasicUtils.getProperty("makeup.password")

        val prop: Properties = new Properties()
        prop.setProperty("url", url)
        prop.setProperty("name", username)
        prop.setProperty("password", password)

        val conn: Connection = JDBCBasicUtils.getConn(url, username, password)
        val tableNameMinute = s"ja_data_flew_minute_everyday_$format"
        val createSqlMinute =
            s"""
               |CREATE TABLE IF NOT EXISTS `$tableNameMinute`(
               |`id` int(11) NOT NULL AUTO_INCREMENT,
               |`minute` bigint NOT NULL,
               |`appid` varchar(20) NOT NULL,
               |`pv` bigint NOT NULL,
               |`uv` bigint NOT NULL,
               |`ip` bigint NOT NULL,
               |`day` date,
               |`addtime` date,
               |PRIMARY KEY (`id`),
               |index `idx_time`(`minute`,`appid`,`day`))
               |ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
            """.stripMargin
        //一个月创建一次表
        val psMinute: PreparedStatement = conn.prepareStatement(createSqlMinute)
        psMinute.executeUpdate(createSqlMinute)
        psMinute.close()
        conn.close()

        val sql = JDBCBasicUtils.getProperty("ja_data_flew_minute_everyday").replace("${tableName}", tableNameMinute)
        totalMinute.foreachPartition(iter => {
            val url = JDBCBasicUtils.getProperty("makeup.url")
            val username = JDBCBasicUtils.getProperty("makeup.username")
            val password = JDBCBasicUtils.getProperty("makeup.password")
            val conn: Connection = JDBCBasicUtils.getConn(url, username, password)

            val date = new java.sql.Date(logDate.getTime)
            val addTime = new Timestamp(System.currentTimeMillis())
            JDBCBasicUtils.insertBatchByIteratorBySql[((Long, String), ((Int, Int), Int))](conn)(sql, tableNameMinute, iter, (pstmt, bean) => {
                pstmt.setLong(1, bean._1._1 * 60 * 1000 + logDate.getTime)
                pstmt.setString(2, bean._1._2)
                pstmt.setInt(3, bean._2._1._1)
                pstmt.setInt(4, bean._2._1._2)
                pstmt.setInt(5, bean._2._2)
                pstmt.setDate(6, date)
                pstmt.setTimestamp(7, addTime)
            })
        })
        totalHour.foreachPartition(iter => {
            val date = new java.sql.Date(logDate.getTime)
            val addTime = new Timestamp(System.currentTimeMillis())
            JDBCBasicUtils.insertBatchByIterator[((Long, String), ((Int, Int), Int))]("ja_data_flew_hour_everyday", iter, (pstmt, bean) => {
                pstmt.setLong(1, bean._1._1 * 60 * 60 * 1000 + logDate.getTime)
                pstmt.setString(2, bean._1._2)
                pstmt.setInt(3, bean._2._1._1)
                pstmt.setInt(4, bean._2._1._2)
                pstmt.setInt(5, bean._2._2)
                pstmt.setDate(6, date)
                pstmt.setTimestamp(7, addTime)
            })
        })
    }
}

object ComJobMakeUp {
    def main(args: Array[String]) {
        val job = new ComJobMakeUp()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}