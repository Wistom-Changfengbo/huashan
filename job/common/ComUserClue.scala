package com.wangjia.bigdata.core.job.common

import java.sql.Timestamp

import com.wangjia.bigdata.core.bean.common.UserClue
import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.{DeviceType, LogEventId, LogType, Platform}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 统计各个平台的线索
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --platform  平台:web,app
  *
  * Created by Administrator on 2017/5/23.
  */
class ComUserClue extends EveryDaySparkJob {

    //应用信息
    private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx

        //加载广播应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos
        mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
    }

    /**
      * 加载Web端线索
      *
      * @param mapAppInfos
      * @return
      */
    private def loadWebClue(mapAppInfos: mutable.Map[String, AppInfo]): RDD[UserClue] = {
        //删除实时数据
        val day = new java.sql.Date(this.logDate.getTime)
        val conn = JDBCBasicUtils.getConn()
        val sql = "delete from ja_data_user_clue where day=? and appid=?"
        val stmt = conn.prepareStatement(sql)

        for ((k, v) <- mapAppInfos) {
            if (v.appType == DeviceType.DEVICE_PC_BROWSER) {
                stmt.setDate(1, day)
                stmt.setString(2, v.appId)
                stmt.addBatch()
            }
        }
        stmt.executeBatch()
        conn.close()

        val linesRdd = SparkExtend.ctx.textFile(this.inPath)
        val beansRdd = linesRdd.map(BeanHandler.toWebAll)
                .filter(x => x != null && x.logtype == LogType.EVENT && x.subtype == LogEventId.USER_COMMIT_CLUE)
        beansRdd.map(BeanHandler.toUserClue).filter(_ != null)
    }

    /**
      * 加载APP端线索
      *
      * @param mapAppInfos
      * @return
      */
    private def loadAppClue(mapAppInfos: mutable.Map[String, AppInfo]): RDD[UserClue] = {
        //删除实时数据
        val day = new java.sql.Date(this.logDate.getTime)
        val conn = JDBCBasicUtils.getConn()
        val sql = "delete from ja_data_user_clue where day=? and appid=?"
        val stmt = conn.prepareStatement(sql)

        for ((k, v) <- mapAppInfos) {
            if (v.appType == DeviceType.DEVICE_PHONE_IOS || v.appType == DeviceType.DEVICE_PHONE_ANDROID) {
                stmt.setDate(1, day)
                stmt.setString(2, v.appId)
                stmt.addBatch()
            }
        }
        stmt.executeBatch()
        conn.close()

        val linesRdd = SparkExtend.ctx.textFile(this.inPath)
        val beansRdd = linesRdd.map(BeanHandler.toAppAll)
                .filter(x => x != null && x.logtype == LogType.EVENT && x.subtype == LogEventId.USER_COMMIT_CLUE)
        beansRdd.map(BeanHandler.toUserClue).filter(_ != null)
    }

    override protected def job(args: Array[String]): Unit = {
        //加载应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos

        val platform: String = this.getParameterOrElse("platform", "null")

        val clueRdd: RDD[UserClue] = platform match {
            case Platform.PLATFORM_NAME_WEB => loadWebClue(mapAppInfos)
            case Platform.PLATFORM_NAME_MOBILE => loadAppClue(mapAppInfos)
            case _ => throw new RuntimeException("no define platform")
        }
        //val rdd = this.action().filter(x => x.phone != "" && x.phone.matches("^\\d+$"))
        //写入数据库
        clueRdd.foreachPartition(iterator => {
            val date = new java.sql.Date(logDate.getTime)
            val newDate = new Timestamp(System.currentTimeMillis())

            JDBCBasicUtils.insertBatchByIterator[UserClue]("sql_user_clue_everyday", iterator, (pstmt, b) => {
                pstmt.setString(1, b.clueId)
                pstmt.setString(2, b.uuid)
                pstmt.setString(3, b.ip)
                pstmt.setString(4, b.appId)
                pstmt.setString(5, b.cookieId)
                pstmt.setString(6, b.deviceId)
                pstmt.setString(7, b.deviceName)
                pstmt.setString(8, b.version)
                pstmt.setTimestamp(9, new Timestamp(b.time))
                pstmt.setString(10, b.name)
                pstmt.setString(11, b.phone)
                pstmt.setString(12, b.cityId)
                pstmt.setString(13, b.houseArea)
                pstmt.setString(14, b.style)
                pstmt.setString(15, b.flatsName)
                pstmt.setDate(16, date)
                pstmt.setTimestamp(17, newDate)
            })
        })
    }
}

object ComUserClue {
    def main(args: Array[String]) {
        val job = new ComUserClue()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}