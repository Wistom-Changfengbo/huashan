package com.wangjia.bigdata.core.job.stream

import com.wangjia.bigdata.core.bean.info._
import com.wangjia.bigdata.core.bean.mobile.MobileLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler._
import com.wangjia.bigdata.core.job.common.JobStreamUserTrack
import com.wangjia.bigdata.core.utils.JAInfoUtils
import com.wangjia.common.{LogEventId, LogType}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.{JavaUtils, RedisUtils}
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * APP端实时推送
  *
  * Created by Administrator on 2018/3/5.
  */
class AppStreamPush extends JobStreamUserTrack {
    //一个批次时间(S)

    BATCH_DURATION_SECONDS = 60L

    //下线后20M推送
    private val OFF_LINE_TIME_MILLIS: Long = 10 * 60 * 1000

    private val CACHE_SAVE_TIME: Int = 3600

    //最小推送小时
    private val MIN_PUSH_HOUR: Int = 8

    //最大推送小时
    private val MAX_PUSH_HOUR: Int = 20


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
        if (bean == null)
            return false
        val timeMillis = System.currentTimeMillis()
        if (bean.time > timeMillis)
            return false
        if (!JavaUtils.isSameDate(bean.time, timeMillis))
            return false
        //        if (!bean.deviceId.contains("864368033676681"))
        //            return false
        bean.logtype match {
            case LogType.PAGE => return true
            case LogType.EVENT => {
                //页面点击事件 定位事件 应用列表事件 页面点击事件
                if (bean.subtype == LogEventId.UI_CLICK_POINT
                        || bean.subtype == LogEventId.APP_LOCATION
                        || bean.subtype == LogEventId.APP_APPLIST
                        || bean.subtype == "page_action")
                    return false
                return true
            }
        }
        false
    }

    /**
      * 得到用户最早时间
      *
      * @param user (UUID,APPID)
      * @return
      */
    private def getUserFistTime(user: (String, String), conn: Jedis): Long = {
        val key = s"""PUSH:RECENT:${user._1}@${user._2}"""
        if (conn.exists(key)) {
            val t = conn.get(key).toLong
            conn.setex(key, CACHE_SAVE_TIME, t.toString)
            t
        } else
            0L
    }

    /**
      * 添加用户
      *
      * @param user (UUID,APPID)
      * @param time 用户第一次时间
      * @return
      */
    private def addUserFistTime(user: (String, String), time: Long, conn: Jedis): Unit = {
        val key = s"""PUSH:RECENT:${user._1}@${user._2}"""
        conn.setex(key, CACHE_SAVE_TIME, time.toString)
    }


    protected def updateFunc(iter: Iterator[((String, String, String), Seq[Long], Option[Long])]): Iterator[((String, String, String), Long)] = {
        val list = mutable.ListBuffer[((String, String, String), Long)]()
        val conn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, Config.REDIS_DBINDEX)
        val timeMillis: Long = System.currentTimeMillis()
        val dayStr = JavaUtils.timeMillis2DayNum(timeMillis)
        val hour = JavaUtils.getHour(timeMillis)
        val minTime = timeMillis - OFF_LINE_TIME_MILLIS

        iter.foreach(x => {
            val max = Math.max(if (x._2.nonEmpty) x._2.toList.max else 0L, x._3.getOrElse(0L))
            if (max > minTime)
                list += ((x._1, max))
            else if (hour >= MIN_PUSH_HOUR && hour <= MAX_PUSH_HOUR && max != 0L) {
                val key: String = s"""PUSH:$dayStr:${x._1._1}@${x._1._2}"""
                val bClue: Boolean = conn.hget(key, "bClue").toBoolean
                val pushT: Long = conn.hget(key, "pushTime").toLong
                if (!bClue && pushT == 0L) {
                    conn.hset(key, "pushTime", timeMillis.toString)
                    val deviceid = x._1._3.replace("#", "")
                    //log
                    conn.hset(s"""PUSH:$dayStr:LOG""", s"""${x._1._1}@${x._1._2}@$deviceid@$timeMillis""", timeMillis.toString)
                    try {
                        val url = s"""http://api.jiajuol.com/api/jiajuol/api/push?uuid=${x._1._1}&appid=${x._1._2}&device_token=$deviceid"""
                        val httpClient = HttpClientBuilder.create().build()
                        val get = new HttpGet(url)
                        val response: CloseableHttpResponse = httpClient.execute(get)
                        //response.getStatusLine().getStatusCode() == HttpStatus.SC_OK
                    } catch {
                        case e: Exception => e.printStackTrace(); -1
                    }
                }
            }
        })
        conn.close()
        list.toIterator
    }

    case class StreamHabit(pageNume: Int,
                           stayTime: Long,
                           bClue: Boolean,
                           minTime: Long,
                           maxTime: Long,
                           ip: String,
                           deviceid: String)

    override protected def job(args: Array[String]): Unit = {
        val kafkaParams = Map[String, Object](
            "session.timeout.ms" -> "70000",
            "request.timeout.ms" -> "80000",
            "group.id" -> "wj_app_user_push_20180611"
        )

        val topics = Array("app20180611")

        val lines = readLineByKafka(ssc, topics, kafkaParams)
        val beanDs: DStream[MobileLogMsg] = lines.map(x => LogClearHandler.clearMobileLog(x, mapAppInfos, mapIdAppInfos, mapAccountInfos))
                .filter(_._1 == LogClearStatus.SUCCESS)
                .flatMap(_._2.asInstanceOf[ListBuffer[MobileLogMsg]])
                .filter(filterBean)
                .filter(x => x.uuid != null && x.uuid.length == 32)

        //得到新增用户最后一次时间[((UUID, APPID, DEVICEID), LASTTIME)]
        val userLastTimeDf: DStream[((String, String, String), Long)] = beanDs.map(x => ((x.uuid, x.appid), x))
                .groupByKey()
                .map { case ((uuid, appid), beans) =>
                    var pageNum: Int = 0
                    val ip: String = beans.head.ip
                    var minTime: Long = beans.head.time
                    var maxTime: Long = beans.head.time
                    val deviceid: String = beans.head.dStrInfo("deviceid")
                    var bClue: Boolean = false

                    beans.foreach(b => {
                        if (b.logtype == LogType.PAGE)
                            pageNum += 1
                        else if (b.subtype == LogEventId.USER_COMMIT_CLUE)
                            bClue = true
                        if (minTime > b.time) minTime = b.time
                        if (maxTime < b.time) maxTime = b.time
                    })

                    ((uuid, appid), StreamHabit(pageNum, maxTime - minTime, bClue, minTime, maxTime, ip, deviceid))
                }
                .mapPartitions(iterator => {
                    //获取新增用户
                    val list = new ListBuffer[((String, String), StreamHabit)]
                    initHBase()
                    val jConn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, Config.REDIS_DBINDEX)
                    val hbConn = new ExTBConnection
                    val tb: Table = hbConn.getTable(HBaseTableName.UUID_HABIT).getTable
                    val gets = new ListBuffer[Get]
                    val datas = new ListBuffer[((String, String), StreamHabit)]

                    var i: Int = 0
                    import scala.collection.JavaConversions._
                    iterator.foreach(b => {
                        val t = getUserFistTime(b._1, jConn)
                        if (t == 0L) {
                            gets += new Get(Bytes.toBytes(b._1._1))
                            datas += b
                        } else if (JavaUtils.isSameDate(t, System.currentTimeMillis())) {
                            list += b
                        }
                        if (datas.length > Config.BATCH_SIZE) {
                            val results: Array[Result] = tb.get(gets)
                            val size = results.length
                            while (i < size) {
                                val rs = results(i)
                                val data = datas(i)
                                val key = Bytes.toBytes(data._1._2 + ":minTime")
                                val time = if (rs.containsColumn(HBaseConst.BYTES_CF1, key)) Bytes.toLong(rs.getValue(HBaseConst.BYTES_CF1, key)) else System.currentTimeMillis()
                                if (JavaUtils.isSameDate(time, System.currentTimeMillis()))
                                    list += data
                                addUserFistTime(data._1, time, jConn)
                                i += 1
                            }
                        }
                    })
                    if (datas.nonEmpty) {
                        val results: Array[Result] = tb.get(gets)
                        val size = results.length
                        while (i < size) {
                            val rs = results(i)
                            val data = datas(i)
                            val key = Bytes.toBytes(data._1._2 + ":minTime")
                            val time = if (rs.containsColumn(HBaseConst.BYTES_CF1, key)) Bytes.toLong(rs.getValue(HBaseConst.BYTES_CF1, key)) else System.currentTimeMillis()
                            if (JavaUtils.isSameDate(time, System.currentTimeMillis()))
                                list += data
                            addUserFistTime(data._1, time, jConn)
                            i += 1
                        }
                    }
                    jConn.close()
                    hbConn.close()
                    list.toIterator
                })
                .mapPartitions(iterator => {
                    val list = new ListBuffer[((String, String, String), Long)]
                    val conn: Jedis = RedisUtils.getPConn(Config.REDIS_URL, Config.REDIS_PORT, Config.REDIS_DBINDEX)
                    val timeMillis: Long = System.currentTimeMillis()
                    val dayStr = JavaUtils.timeMillis2DayNum(timeMillis)
                    iterator.foreach(x => {
                        list += (((x._1._1, x._1._2, x._2.deviceid), x._2.maxTime))
                        val key = s"""PUSH:$dayStr:${x._1._1}@${x._1._2}"""
                        val bUpdate = conn.hexists(key, "bClue")
                        val pageNume: Int = if (bUpdate) conn.hget(key, "pageNume").toInt + x._2.pageNume else x._2.pageNume
                        val stayTime: Long = if (bUpdate) conn.hget(key, "stayTime").toLong + x._2.stayTime else x._2.stayTime
                        val bClue: Boolean = if (bUpdate) conn.hget(key, "bClue").toBoolean || x._2.bClue else x._2.bClue
                        conn.hset(key, "pageNume", pageNume.toString)
                        conn.hset(key, "stayTime", stayTime.toString)
                        conn.hset(key, "bClue", bClue.toString)
                        conn.hset(key, "maxTime", x._2.maxTime.toString)
                        conn.hset(key, "ip", x._2.ip)
                        if (!bUpdate) {
                            conn.hset(key, "deviceid", x._2.deviceid)
                            conn.hset(key, "minTime", x._2.minTime.toString)
                            conn.hset(key, "pushTime", "0")
                        }
                    })
                    conn.close()
                    list.toIterator
                })


        val results: DStream[((String, String, String), Long)] = userLastTimeDf.updateStateByKey(updateFunc _, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

        results.foreachRDD(_.count())
    }
}

object AppStreamPush {
    def main(args: Array[String]) {
        val job = new AppStreamPush
        job.run(args)
    }
}