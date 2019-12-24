package com.wangjia.bigdata.core.job.common

import com.wangjia.bigdata.core.bean.common.UserHabit
import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.{LogType, Platform}
import com.wangjia.hbase.conn.ComTBConnection
import com.wangjia.hbase.{HBaseConst, HBaseTableName}
import com.wangjia.utils.JavaUtils
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import sun.util.resources.cldr.uk.CalendarData_uk_UA

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 统计各个平台的,用户的信息，新老用户，用户习惯
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --platform  平台:web,app，weixin
  *
  * Created by Cfb on 2018/2/26.
  */

case class ComUserBean(appid: String, uuid: String, pageid: String, time: Long, stayTime: Long, visitindex: Int, ip: String)

class ComJobUser extends EveryDaySparkJob {
  //应用信息
  private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

  override protected def init(): Unit = {
    super.init()
    val sc = SparkExtend.ctx

    //加载广播应用信息
    val mapAppInfos = JAInfoUtils.loadAppInfos
    mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
  }

  private def loadApp(inpath: String): RDD[ComUserBean] = {
    val pageRdd = SparkExtend.ctx
      .textFile(inpath)
      .map(BeanHandler.toAppPage)
      .filter(_ != null)
      .map(x => ComUserBean(x.appid, x.uuid, x.pageid, x.time, x.staytime, x.visitindex, x.ip))
    pageRdd
  }


  private def loadWeb(inpath: String): RDD[ComUserBean] = {
    val pageRdd = SparkExtend.ctx
      .textFile(inpath)
      .map(BeanHandler.toWebAcc)
      .filter(x => x != null && x.logtype == LogType.PAGE)
      .map(x => ComUserBean(x.appid, x.uuid, "", x.time, x.staytime, x.visitindex, x.ip))
    pageRdd
  }

  private def loadWeiXin(inpath: String): RDD[ComUserBean] = {
    val beansRdd = SparkExtend.ctx
      .textFile(inpath)
      .map(BeanHandler.toWeiXin)
      .filter(x => x != null && x.logtype == LogType.PAGE)
      .map(x => ComUserBean(x.appid, x.uuid, x.pageid, x.time, x.staytime, x.visitindex, x.ip))
    beansRdd
  }


  /**
    * 分析用户习惯
    *
    * @param beansRdd
    */
  private def analyzeUserHabit(beansRdd: RDD[ComUserBean]): Unit = {
    //(uuid,bean) RDD
    val uuid2beanRdd = beansRdd.groupBy(x => (x.appid, x.uuid))

    //得到每个APPID总的访问时间和总的会话次数
    val appid2Habit = uuid2beanRdd.map(x => {
      val appid = x._1._1
      val uuid = x._1._2
      val sumUser: Long = 1
      val beans = x._2.toList
      val sumTime: Long = beans.map(_.stayTime).sum
      val sumAccess: Long = beans.map(_.visitindex).max
      val sumPage: Long = beans.length
      (appid, (sumUser, sumTime, sumAccess, sumPage))
    }).reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4))
    appid2Habit.cache()
    appid2Habit.checkpoint()

    //分平台写入数据库
    appid2Habit.map(x => {
      val appid = x._1
      val platform = ""
      val sum_page = x._2._4
      val sum_user = x._2._1
      val sum_time = x._2._2
      val sum_access = x._2._3
      UserHabit(appid, platform, sum_time, sum_user, sum_access, sum_page)
    })
      .foreachPartition(iterator => {
        val date = new java.sql.Date(logDate.getTime)
        val newDate = new java.sql.Date(System.currentTimeMillis())
        JDBCBasicUtils.insertBatch("sql_user_everyday_habit", pstmt => {
          while (iterator.hasNext) {
            val b = iterator.next()
            pstmt.setString(1, b.appid)
            pstmt.setLong(2, 0)
            pstmt.setLong(3, b.sum_time)
            pstmt.setLong(4, b.sum_user)
            pstmt.setLong(5, b.sum_access)
            pstmt.setLong(6, b.sum_page)
            pstmt.setDate(7, date)
            pstmt.setDate(8, newDate)
            pstmt.addBatch()
          }
        })
      })
  }

  /**
    * 写入Mysql 用户数据-用户留存
    *
    * @param iterator
    */
  private def saveUser(iterator: Iterator[((String, Int), Long)]): Unit = {
    val date = new java.sql.Date(logDate.getTime)
    val newDate = new java.sql.Date(System.currentTimeMillis())
    var i = 0
    JDBCBasicUtils.insertBatch("sql_user_everyday", pstmt => {
      while (iterator.hasNext) {
        val b = iterator.next()

        pstmt.setString(1, b._1._1)
        pstmt.setLong(2, 0)
        pstmt.setLong(3, b._1._2)
        pstmt.setLong(4, b._2)
        pstmt.setDate(5, date)
        pstmt.setDate(6, newDate)

        pstmt.addBatch()

        i += 1
        if (i > 1000) {
          pstmt.executeBatch()
          pstmt.clearBatch()
          i = 0
        }
      }
    })
  }

  /**
    * 得到设备第一次时间
    *
    * @param tbUUID2DeviceMsg
    * @param uuids (APPID,UUID)
    * @return (APPID,UUID,FristTime)
    */
  private def getUUIDFirstTime(tbUUID2DeviceMsg: Table, uuids: ListBuffer[(String, String)]): ListBuffer[(String, String, Long)] = {
    val gets = uuids.map(x => new Get(Bytes.toBytes(x._2)))
    val results: Array[Result] = tbUUID2DeviceMsg.get(gets)
    val list = new ListBuffer[(String, String, Long)]()
    var i = 0
    val max = results.length
    while (i < max) {
      val bean = uuids(i)
      val rs = results(i)
      val key = Bytes.toBytes("_appid#" + bean._1)
      if (rs.containsColumn(HBaseConst.BYTES_CF1, key)) {
        val t = Bytes.toLong(rs.getValue(HBaseConst.BYTES_CF1, key))
        list += ((bean._1, bean._2, t))
      }
      i += 1
    }
    list
  }

  /**
    * 分析新老用户
    *
    * @param beansRdd
    */
  private def analyzeUserNewOrOld(beansRdd: RDD[ComUserBean]): Unit = {
    //        appid: String, uuid: String, pageid: String, time: Long, stayTime: Long, visitindex: Int, ip: String
    //把app和uuid去重，保持每个app中只有一份uuid
    val appidAndUUIDRdd = beansRdd.map(x => (x.appid, x.uuid)).distinct()

    appidAndUUIDRdd.mapPartitions(iterator => {
      initHBase()
      //((APPID,DAY),NUM)
      val map = mutable.Map[(String, Int), Long]()
      val addMap = (x: (String, Int)) => {
        val b = map.getOrDefault(x, 0)
        map.put(x, b + 1)
      }
      val newDay = JavaUtils.timeMillis2DayNum(this.logDate.getTime)
      var dDay = 0
      val conn = new ComTBConnection()
      val tbUUID2DeviceMsg = conn.getTable(HBaseTableName.UUID_DEVICE_MSG)
      //(APPID,UUID)
      val cache = new ListBuffer[(String, String)]()
      while (iterator.hasNext) {
        cache += iterator.next()
        if (cache.size > Config.BATCH_SIZE) {
          getUUIDFirstTime(tbUUID2DeviceMsg, cache).foreach(x => {
            dDay = JavaUtils.timeMillis2DayNum(x._3) - newDay
            if (dDay != 0) {
              dDay = 1
            }
            addMap((x._1, dDay))
          })
          cache.clear()
        }
      }
      if (cache.nonEmpty) {
        getUUIDFirstTime(tbUUID2DeviceMsg, cache).foreach(x => {
          dDay = JavaUtils.timeMillis2DayNum(x._3) - newDay
          if (dDay != 0) {
            dDay = 1
          }
          addMap((x._1, dDay))
        })
        cache.clear()
      }
      conn.close()
      map.toIterator
    }).repartition(1).saveAsTextFile("G:\\LOG\\app\\test01\\")


    val dayRdd = appidAndUUIDRdd.mapPartitions(iterator => {
      initHBase()
      //((APPID,DAY),NUM)
      val map = mutable.Map[(String, Int), Long]()
      val addMap = (x: (String, Int)) => {
        val b = map.getOrDefault(x, 0)
        map.put(x, b + 1)
      }
      val newDay = JavaUtils.timeMillis2DayNum(this.logDate.getTime)
      var dDay = 0
      val conn = new ComTBConnection()
      val tbUUID2DeviceMsg = conn.getTable(HBaseTableName.UUID_DEVICE_MSG)
      //(APPID,UUID)
      val cache = new ListBuffer[(String, String)]()
      while (iterator.hasNext) {
        cache += iterator.next()
        if (cache.size > Config.BATCH_SIZE) {
          getUUIDFirstTime(tbUUID2DeviceMsg, cache).foreach(x => {
            dDay = JavaUtils.timeMillis2DayNum(x._3) - newDay
            if (dDay != 0) {
              dDay = 1
            }
            addMap((x._1, dDay))
          })
          cache.clear()
        }
      }
      if (cache.nonEmpty) {
        getUUIDFirstTime(tbUUID2DeviceMsg, cache).foreach(x => {
          dDay = JavaUtils.timeMillis2DayNum(x._3) - newDay
          if (dDay != 0) {
            dDay = 1
          }
          addMap((x._1, dDay))
        })
        cache.clear()
      }
      conn.close()
      map.toIterator
    }).reduceByKey(_ + _)

    dayRdd.repartition(1).saveAsTextFile("G:\\LOG\\app\\test02\\")



//    println(dayRdd.count())
//    dayRdd.foreachPartition(saveUser)
  }

  /**
    * 任务入口方法
    *
    * @param args
    */
  override protected def job(args: Array[String]): Unit = {
    //平台参数
    val platform = this.getParameterOrElse("platform", "null")

//    val userRdd: RDD[ComUserBean] = platform match {
//      case Platform.PLATFORM_NAME_WEB => loadWeb(this.inPath)
//      case Platform.PLATFORM_NAME_MOBILE => loadApp("G:\\LOG\\app\\clean\\")
//      case Platform.PLATFORM_NAME_WEIXIN => loadWeiXin(this.inPath)
//      case _ => throw new RuntimeException("no define platform")
//    }

    val userRdd: RDD[ComUserBean] = loadApp("G:\\LOG\\app\\clean\\")
    userRdd.cache()
    userRdd.checkpoint()

    //分析新老用户
    analyzeUserNewOrOld(userRdd)

    //分析用户习惯
//    analyzeUserHabit(userRdd)
  }
}

object ComJobUser {
  def main(args: Array[String]) {
    val job = new ComJobUser()
    job.run(args)
    println(System.currentTimeMillis() - job.jobStartTime)
  }
}