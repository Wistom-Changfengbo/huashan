package com.wangjia.bigdata.core.job.mobile

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.common.{LogEventId, LogType}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.{HBaseUtils, JavaUtils}
import org.apache.hadoop.hbase.client.{Get, Put, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 分析设备信息
  *
  * --inPath    日志输入目录
  * * --date      日志日期(yyyy-MM-dd)
  *
  * Created by Administrator on 2017/4/27.
  */
class MobileJobUserHabit extends EveryDaySparkJob {

    /**
      *
      * @param uuid     UUID
      * @param appid    APPID
      * @param platform 平台
      * @param visit    会话次数
      * @param page     页面数
      * @param stayTime 总停留时间
      * @param bUser    是否注册
      * @param bClue    是否提交线索
      * @param events   事件详情
      * @param times    活跃时间
      * @param ips      常用IP
      */
    case class Habit(uuid: String,
                     appid: String,
                     platform: String,
                     visit: Int,
                     page: Int,
                     stayTime: Long,
                     bUser: Boolean,
                     bClue: Boolean,
                     events: Map[String, Int],
                     times: Map[String, Int],
                     ips: Map[String, Int])

    private def habit2Json(habit: Habit): JSONObject = {
        val json: JSONObject = new JSONObject()
        json.put("uuid", habit.uuid)
        json.put("appid", habit.appid)
        json.put("platform", habit.platform)
        json.put("visit", habit.visit)
        json.put("page", habit.page)
        json.put("stayTime", habit.stayTime)
        json.put("bUser", habit.bUser)
        json.put("bClue", habit.bClue)
        val events = new JSONObject()
        habit.events.foreach(x => events.put(x._1, x._2))
        json.put("events", events)
        val times = new JSONObject()
        habit.times.foreach(x => times.put(x._1.toString, x._2))
        json.put("times", times)
        val ips = new JSONObject()
        habit.ips.foreach(x => ips.put(x._1.toString, x._2))
        json.put("ips", ips)
        json
    }

    private def json2Habit(jsonStr: String): Habit = {
        try {
            val json: JSONObject = JSON.parseObject(jsonStr)
            val uuid: String = json.getString("uuid")
            val appid: String = json.getString("appid")
            val platform: String = json.getString("platform")
            val visit: Int = json.getIntValue("visit")
            val page: Int = json.getIntValue("page")
            val stayTime: Long = json.getLongValue("stayTime")
            val bUser: Boolean = json.getBooleanValue("bUser")
            val bClue: Boolean = json.getBooleanValue("bClue")

            val jEvents = json.getJSONObject("events")
            val events = new mutable.HashMap[String, Int]
            jEvents.keys.foreach(key => events.put(key, jEvents.getIntValue(key)))

            val jTimes = json.getJSONObject("times")
            val times = new mutable.HashMap[String, Int]
            jTimes.keys.foreach(key => times.put(key, jTimes.getIntValue(key)))

            val jIps = json.getJSONObject("ips")
            val ips = new mutable.HashMap[String, Int]
            jIps.keys.foreach(key => ips.put(key, jIps.getIntValue(key)))

            Habit(uuid, appid, platform, visit, page, stayTime, bUser, bClue, events.toMap, times.toMap, ips.toMap)
        } catch {
            case e: Exception => e.printStackTrace(); null
        }
    }

    private def habitAdd(h1: Habit, h2: Habit): Habit = {
        if (h2 == null)
            return h1
        if (h1 == null)
            return h2

        val uuid: String = h1.uuid
        val appid: String = h1.appid
        val visit: Int = h1.visit + h2.visit
        val page: Int = h1.page + h2.page
        val stayTime: Long = h1.stayTime + h2.stayTime
        val bUser: Boolean = h1.bUser | h2.bUser
        val bClue: Boolean = h1.bClue | h2.bClue

        val events = new mutable.HashMap[String, Int]
        h1.events.foreach { case (k, v) => events.put(k, events.getOrDefault(k, 0) + v) }
        h2.events.foreach { case (k, v) => events.put(k, events.getOrDefault(k, 0) + v) }

        val times = new mutable.HashMap[String, Int]
        h1.times.foreach { case (k, v) => times.put(k, times.getOrDefault(k, 0) + v) }
        h2.times.foreach { case (k, v) => times.put(k, times.getOrDefault(k, 0) + v) }

        val ips = new mutable.HashMap[String, Int]
        h1.ips.foreach { case (k, v) => ips.put(k, ips.getOrDefault(k, 0) + v) }
        h2.ips.foreach { case (k, v) => ips.put(k, ips.getOrDefault(k, 0) + v) }

        Habit(uuid, appid, h1.platform, visit, page, stayTime, bUser, bClue, events.toMap, times.toMap, ips.toMap)
    }

    private def updataPut(minTime: Long, maxTime: Long, habit: Habit, result: Result): Put = {
        val appid: String = habit.appid
        val put = new Put(Bytes.toBytes(habit.uuid))

        val minTimeKey = Bytes.toBytes(appid + ":minTime")
        val maxTimeKey = Bytes.toBytes(appid + ":maxTime")
        if (!result.containsColumn(HBaseConst.BYTES_CF1, minTimeKey)
                || Bytes.toLong(result.getValue(HBaseConst.BYTES_CF1, minTimeKey)) > minTime)
            put.addColumn(HBaseConst.BYTES_CF1, minTimeKey, Bytes.toBytes(minTime))

        if (!result.containsColumn(HBaseConst.BYTES_CF1, maxTimeKey)
                || Bytes.toLong(result.getValue(HBaseConst.BYTES_CF1, maxTimeKey)) < maxTime)
            put.addColumn(HBaseConst.BYTES_CF1, maxTimeKey, Bytes.toBytes(maxTime))

        if (!result.containsColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("platform")))
            put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes("platform"), Bytes.toBytes(habit.platform))

        val dayNum = JavaUtils.timeMillis2DayNum(maxTime)

        val msgKey = Bytes.toBytes(appid + ":" + dayNum + ":msg")
        put.addColumn(HBaseConst.BYTES_CF1, msgKey, Bytes.toBytes(habit2Json(habit).toJSONString))

        var activeDayNum: Int = 0
        var i: Int = 1
        var recentHabit: Habit = habit
        while (i < 60) {
            val msgKey = Bytes.toBytes(appid + ":" + (dayNum - i) + ":msg")
            if (result.containsColumn(HBaseConst.BYTES_CF1, msgKey)) {
                recentHabit = habitAdd(recentHabit, json2Habit(Bytes.toString(result.getValue(HBaseConst.BYTES_CF1, msgKey))))
                activeDayNum += 1
            }
            i += 1
        }
        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(appid + ":habit:msg"), Bytes.toBytes(habit2Json(recentHabit).toJSONString))

        val activeTime: Int = recentHabit.times.max._1.toInt

        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(appid + ":activeDayNum"), Bytes.toBytes(activeDayNum))
        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(appid + ":activeTime"), Bytes.toBytes(activeTime))
        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(appid + ":visit"), Bytes.toBytes(recentHabit.visit))
        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(appid + ":page"), Bytes.toBytes(recentHabit.page))
        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(appid + ":stayTime"), Bytes.toBytes(recentHabit.stayTime))
        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(appid + ":bUser"), Bytes.toBytes(recentHabit.bUser))
        put.addColumn(HBaseConst.BYTES_CF1, Bytes.toBytes(appid + ":bClue"), Bytes.toBytes(recentHabit.bClue))

        put
    }

    override protected def job(args: Array[String]): Unit = {
        HBaseUtils.createTable(HBaseTableName.UUID_HABIT)

        val sc = SparkExtend.ctx
        val linesRdd = sc.textFile(this.inPath)

        val beansRdd = linesRdd.map(BeanHandler.toAppAcc).filter(_ != null)
        beansRdd.cache()
        beansRdd.checkpoint()

        //((UUID,APPID),(minTime,maxTime,Habit))
        val habitRdd: RDD[((String, String), (Long, Long, Habit))] = beansRdd.map(x => ((x.uuid, x.appid), x))
                .groupByKey()
                .map { case ((uuid, appid), beans) =>
                    val events: Map[String, Int] = beans.filter(_.logtype == LogType.EVENT)
                            .groupBy(_.subtype)
                            .map(x => (x._1, x._2.size))
                    val bClue: Boolean = events.contains(LogEventId.USER_COMMIT_CLUE)

                    var minTime: Long = beans.head.time
                    var maxTime: Long = beans.head.time
                    val platform: String = beans.head.dStrInfo("platform", "-")
                    var bUser: Boolean = false

                    var countVisit: Int = 0
                    var sumPage: Int = 0
                    var sumStayTime: Long = 0L

                    val times: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
                    val ips: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()

                    beans.foreach(b => {
                        if (minTime > b.time) minTime = b.time
                        if (maxTime < b.time) maxTime = b.time
                        if (!bUser && !b.userid.isEmpty) bUser = true
                        if (countVisit < b.visitindex) countVisit = b.visitindex
                        if (b.logtype == LogType.PAGE) sumPage += 1
                        ips.put(b.ip, ips.getOrDefault(b.ip, 0) + 1)
                        sumStayTime += b.staytime

                        val index = (((b.time + 28800000) % 86400000) / 1800000).toString
                        times.put(index, times.getOrDefault(index, 0) + 1)
                    })

                    ((uuid, appid), (minTime, maxTime, Habit(uuid, appid, platform, countVisit, sumPage, sumStayTime, bUser, bClue, events, times.toMap, ips.toMap)))
                }

        habitRdd.foreachPartition { case iterator =>
            initHBase()
            val habits = new ListBuffer[(Long, Long, Habit)]
            val gets = new ListBuffer[Get]
            val conn = new ExTBConnection
            val exTb = conn.getTable(HBaseTableName.UUID_HABIT)
            val tb: Table = exTb.getTable

            iterator.foreach(x => {
                habits += x._2
                gets += new Get(Bytes.toBytes(x._1._1))
                if (habits.size > Config.BATCH_SIZE) {
                    val results: Array[Result] = tb.get(gets)
                    var i = 0
                    val max = results.length
                    while (i < max) {
                        val h = habits(i)
                        exTb.addPut(updataPut(h._1, h._2, h._3, results(i)))
                        i += 1
                    }
                    habits.clear()
                    gets.clear()
                }
            })
            if (habits.nonEmpty) {
                val results: Array[Result] = tb.get(gets)
                var i = 0
                val max = results.length
                while (i < max) {
                    val h = habits(i)
                    exTb.addPut(updataPut(h._1, h._2, h._3, results(i)))
                    i += 1
                }
                habits.clear()
                gets.clear()
            }

            conn.close()
        }
    }
}

object MobileJobUserHabit {
    def main(args: Array[String]) {
        val job = new MobileJobUserHabit()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}