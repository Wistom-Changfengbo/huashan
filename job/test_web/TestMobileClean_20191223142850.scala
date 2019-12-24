package com.wangjia.bigdata.core.job.test_web

import java.util.Calendar

import com.wangjia.bigdata.core.bean.info.{AccountInfo, AppInfo}
import com.wangjia.bigdata.core.handler.LogClearStatus
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JAInfoUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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


class TestMobileClean extends EveryDaySparkJob {
  //页面最大停留时间
  private val PAGE_MAX_STAY_TIME: Long = 10 * 60 * 1000
  private var logPeriod = 30
  private val oneDayTime: Long = 1000 * 60 * 60 * 24
  private var addDayTime: Long = 0
  private var firstStartTime: Long = 0
  //应用信息
  private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null
  private var mapIdentifierAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

  //帐号信息
  private var mapAccountInfosBroadcast: Broadcast[mutable.Map[Int, AccountInfo]] = null


  override protected def init(): Unit = {
    super.init()
    val sc = SparkExtend.ctx
//
//    //加载广播帐号信息
//    val mapAccountInfos = JAInfoUtils.loadAccountInfos
//    mapAccountInfosBroadcast = sc.broadcast(mapAccountInfos)
//
//    //加载广播应用信息
//    val mapAppInfos = JAInfoUtils.loadAppInfos
//    mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
//
//    val mapIdAppInfos = mutable.Map[String, AppInfo]()
//    mapAppInfos.foreach(i => {
//      if (i._2.identifier != "") {
//        mapIdAppInfos.put(i._2.identifier, i._2)
//      }
//    })
//    mapIdentifierAppInfosBroadcast = sc.broadcast(mapIdAppInfos)
    addDayTime = this.logDate.getTime + oneDayTime
  }

  //获取日志路径
  private def getLogPath(cal: Calendar): String = {
    val year = cal.get(Calendar.YEAR).toString
    var month = (cal.get(Calendar.MONTH) + 1).toString
    if (month.size == 1) {
      month = s"0$month"
    }
    var day = cal.get(Calendar.DAY_OF_MONTH).toString
    if (day.size == 1) {
      day = s"0$day"
    }
    val dataPath = s"$year/$month/$day"
    dataPath
  }

  //获取92天的日志
  def getTotalRDD(sc: SparkContext): RDD[String] = {
    val callendar = Calendar.getInstance()
    callendar.setTimeInMillis(addDayTime)
    var totalRdd: RDD[String] = null
    //往后移动92天
    var total = logPeriod
    val sb = new StringBuilder
    while (total != 0) {
      total = total - 1
      callendar.add(Calendar.DATE, -1)
      val dataPath = getLogPath(callendar)
      sb.append(this.inPath.concat(dataPath))
      if (total != 0) {
        sb.append(",")
      }
    }
//    println(sb.toString())
        if (totalRdd == null) {
          totalRdd = sc.textFile(sb.toString())
        }
    firstStartTime = callendar.getTimeInMillis
    totalRdd
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
    logPeriod =this.getParameterOrElse("num","1").toInt
    val linesRdd = getTotalRDD(sc)
//    //        val linesRdd = sc.textFile("G:\\LOG\\app\\log\\2019\\01\\19\\ja-app-2019-01-19.log")
//    //        val linesRdd = sc.textFile("G:\\LOG\\app\\log\\2019\\01\\19\\ja-app-2019-01-19.log")
//    //
//    //
    val clearBeanRdd = linesRdd.map(x => LogClearTest.clearMobileLog(x, null, null, null))
      .filter(x => x._1 == LogClearStatus.SUCCESS && x._2 != null && (x._2.appid == "1203" || x._2.appid == "1200"||x._2.appid == "1201") )


    if (!Config.DEBUG) {
      val hadoopConf: Configuration = sc.hadoopConfiguration
      val hdfs: FileSystem = FileSystem.get(hadoopConf)
      val path: Path = new Path(this.outPath)
      if (hdfs.exists(path)) {
        hdfs.delete(path, true)
      }
    }
    clearBeanRdd.repartition(5).saveAsTextFile(this.outPath)


    //    val totalRdd = getTotalRDD(sc)


    //      .flatMap(_._2.asInstanceOf[ListBuffer[MobileLogMsg]]).map(x=>(x.appid,1)).reduceByKey((x1,x2)=>x1+x2).repartition(1).saveAsTextFile("G:\\\\LOG\\\\app\\\\clean1")


  }
}

object TestMobileClean {
  def main(args: Array[String]) {
    val job = new TestMobileClean()
    job.run(args)
    println(System.currentTimeMillis() - job.jobStartTime)
  }
}

