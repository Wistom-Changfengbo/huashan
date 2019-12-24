package com.wangjia.bigdata.core.job.stream

import com.wangjia.bigdata.core.bean.info.{AccountInfo, AppInfo}
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler._
import com.wangjia.bigdata.core.job.common.JobStreamFlew
import com.wangjia.bigdata.core.utils.{JAInfoUtils, StreamUtils}
import com.wangjia.common.LogType
import com.wangjia.utils.{JavaUtils, RedisUtils}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * APP端实时流量统计 按分钟、按小时
  *
  * Created by Administrator on 2017/7/5.
  */
class AppStreamFlew extends JobStreamFlew {

    //一个批次时间(S)
    BATCH_DURATION_SECONDS = 30L

    //加载广播应用信息
    private val mapAppInfos = new LoadDataHandler[mutable.Map[String, AppInfo]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadAppInfos _)

    //加载应用信息
    private val mapIdAppInfos = new LoadDataHandler[mutable.Map[String, AppInfo]](Config.UPDATE_INFO_INTERVAL_TIME, () => {
        val _mapAppInfos = JAInfoUtils.loadAppInfos
        val _mapIdAppInfos = mutable.Map[String, AppInfo]()
        _mapAppInfos.foreach(i => {
            if (i._2.identifier != "")
                _mapIdAppInfos.put(i._2.identifier, i._2)
        })
        _mapIdAppInfos
    })

    //加载广播帐号信息
    private val mapAccountInfos = new LoadDataHandler[mutable.Map[Int, AccountInfo]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadAccountInfos _)

    private def filterBean(bean: MobileLogMsg): Boolean = {
        if (bean.uuid.length != 32)
            return false
        if (bean.logtype != LogType.PAGE)
            return false
        if (bean.subtype != "1")
            return false
        val timeMillis = System.currentTimeMillis()
        if (bean.time > timeMillis)
            return false
        if (!JavaUtils.isSameDate(bean.time, timeMillis))
            return false
        true
    }

    override protected def job(args: Array[String]): Unit = {
        val kafkaParams = Map[String, String](
            "session.timeout.ms" -> "40000",
            "request.timeout.ms" -> "50000",
            "group.id" -> "wj_app_flew_20180611"
        )

        val topics = Array("app20180611")
        val lines = readLineByKafka(ssc, topics, kafkaParams)

        val beanDS = lines.map(line => LogClearHandler.clearMobileLog(line, mapAppInfos, mapIdAppInfos, mapAccountInfos))
                .filter(_._1 == LogClearStatus.SUCCESS)
                .flatMap(_._2.asInstanceOf[ListBuffer[MobileLogMsg]])

        StreamUtils.saveLogLastTime(beanDS.map(_.time),"MONITOR:APP:LOG:LASTTIME")

        //DEBUG
        beanDS.foreachRDD(_.foreachPartition(iterator => {
            val conn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, 6)
            updateDebugMarks(conn)
            if (debugMarks.nonEmpty) {
                val dayStr = JavaUtils.timeMillis2DayNum(System.currentTimeMillis())
                val keyHead = "DEBUG:LOG:" + dayStr + ":"
                iterator.foreach(bean => {
                    debugMarks.foreach(mark => {
                        if (bean.appid == mark._2._1 && bean.dStrInfo("deviceid") == mark._2._2) {
                            conn.rpush(keyHead + mark._1, bean.toString)
                        }
                    })
                })
            }
            RedisUtils.closeConn(conn)
        }))

        val pageDS = beanDS.filter(filterBean).map(x => {
            val key = (x.uuid, x.logtype, x.subtype, x.appid, x.time)
            (key, x)
        }).reduceByKey((x1, x2) => x1).map(_._2)

        val flewDS: DStream[(String, Flew)] = pageDS.map(x => (x.appid, x))
                .groupByKey()
                .map(x => {
                    val appid = x._1
                    val deviceIds = mutable.Map[String, Long]()
                    val ips = mutable.Map[String, Long]()
                    val pvs = mutable.ListBuffer[Long]()
                    var maxTime: Long = 0
                    x._2.foreach(b => {
                        deviceIds += ((b.uuid, b.time))
                        ips += ((b.ip, b.time))
                        pvs += b.time
                        if (maxTime < b.time)
                            maxTime = b.time
                    })
                    (appid, Flew(appid, deviceIds, ips, pvs, maxTime, 0))
                })

        flewJob(ssc, flewDS, 60L, "MINUTE")
        flewJob(ssc, flewDS, 3600L, "HOUR")
    }
}

object AppStreamFlew {
    def main(args: Array[String]) {
        val job = new AppStreamFlew
        job.run(args)
    }
}