package com.wangjia.bigdata.core.job.common

import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.job.SparkStreamJob

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.wangjia.bigdata.core.utils.ExMap.map2ExMap
import com.wangjia.bigdata.core.utils.ExListBuffer.map2ExListBuffer
import com.wangjia.utils.{JavaUtils, RedisUtils}
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * Created by Administrator on 2017/9/8.
  */
trait JobStreamFlew extends SparkStreamJob {

    /**
      * 调试标记 (debugId,(appId,deviceId))
      */
    val debugMarks: mutable.Map[String, (String, String)] = mutable.Map[String, (String, String)]()

    /**
      * 更新测试标记
      *
      * @param conn
      */
    protected def updateDebugMarks(conn: Jedis): Unit = {
        import scala.collection.JavaConversions._
        val marks: mutable.Map[String, String] = conn.hgetAll("DEBUG:MARKS")
        debugMarks.clear()
        marks.foreach(m => {
            val debugId = m._1
            val index = m._2.indexOf('#')
            if (index > 0 && index < m._2.length / 2) {
                val appid = m._2.substring(0, index)
                val deviceid = m._2.substring(index + 1)
                debugMarks.put(debugId, (appid, deviceid))
            }
        })
    }


    /**
      *
      * @param appid
      * @param deviceIds
      * @param ips
      * @param pvs
      * @param maxTime
      * @param endTime
      */
    case class Flew(appid: String,
                    deviceIds: mutable.Map[String, Long],
                    ips: mutable.Map[String, Long],
                    pvs: ListBuffer[Long],
                    var maxTime: Long,
                    var endTime: Long)

    /**
      * 聚合两个Flew
      *
      * @param f1
      * @param f2
      * @param pastTime
      * @return
      */
    protected def reduceFlew(f1: Flew, f2: Flew, pastTime: Long): Flew = {

        f1.deviceIds.addValue(f2.deviceIds, (t1, t2) => t1 < t2)
        f1.ips.addValue(f2.ips, (t1, t2) => t1 < t2)

        f1.pvs ++= f2.pvs
        if (f1.maxTime < f2.maxTime)
            f1.maxTime = f2.maxTime

        if (f1.maxTime % pastTime == 0)
            f1.endTime = f1.maxTime
        else
            f1.endTime = (f1.maxTime / pastTime + 1) * pastTime
        f1
    }

    /**
      * 聚合新老数据
      *
      * @param appid
      * @param lnew
      * @param of
      * @param pastTime
      * @return
      */
    protected def reduceFunc(appid: String, lnew: Seq[Flew], of: Option[Flew], pastTime: Long): Flew = {
        var f = of.getOrElse(Flew(appid, mutable.Map[String, Long](), mutable.Map[String, Long](), mutable.ListBuffer[Long](), 0, 0))
        lnew.foreach(b => {
            f = reduceFlew(f, b, pastTime)
        })
        //去掉过时数据
        val minTime = f.endTime - pastTime

        f.deviceIds.remove((k: String, v: Long) => v < minTime)
        f.ips.remove((k: String, v: Long) => v < minTime)
        f.pvs.remove((x: Long) => x < minTime)

        f
    }


    /**
      * 更新历史数据
      *
      * @param pastTime 过期时间
      * @param iter
      * @return
      */
    protected def updateFunc(pastTime: Long)(iter: Iterator[(String, Seq[Flew], Option[Flew])]): Iterator[(String, Flew)] = {
        val list = mutable.ListBuffer[(String, Flew)]()
        while (iter.hasNext) {
            val b = iter.next()
            val appid = b._1
            list += ((appid, reduceFunc(appid, b._2, b._3, pastTime)))
        }

        list.toIterator
    }


    /**
      * 按时间段统计一个流量任务
      *
      * @param ssc
      * @param flewDS
      * @param time 多久统计一次(s)
      * @param tag  存入数据库Key的标识
      */
    protected def flewJob(ssc: StreamingContext, flewDS: DStream[(String, Flew)], time: Long, tag: String = ""): Unit = {

        val results = flewDS.updateStateByKey(updateFunc(time * 1000) _, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

        var initRunTimes: Int = 0
        results.foreachRDD(rdd => {
            initRunTimes += 1
            if (initRunTimes % 2 == 0) {
                rdd.foreachPartition(iterator => {
                    val conn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, Config.REDIS_DBINDEX)
                    while (iterator.hasNext) {
                        val b = iterator.next()._2
                        val appid = b.appid
                        val pv = b.pvs.size
                        val uv = b.deviceIds.size
                        val ip = b.ips.size
                        val timeKey = b.endTime.toString
                        val dayStr = JavaUtils.timeMillis2DayNum(b.endTime)
                        val keyHead = "FLEW:" + dayStr + ":" + appid + ":" + tag

                        conn.hset(keyHead + ":PV", timeKey, pv.toString)
                        conn.hset(keyHead + ":UV", timeKey, uv.toString)
                        conn.hset(keyHead + ":IP", timeKey, ip.toString)
                        conn.hset(keyHead + ":TIME", System.currentTimeMillis().toString, b.maxTime.toString)
                    }
                    RedisUtils.closeConn(conn)
                })
            }
        })
    }
}
