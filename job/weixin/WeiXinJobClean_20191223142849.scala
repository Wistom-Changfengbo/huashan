package com.wangjia.bigdata.core.job.weixin

import java.sql.Timestamp

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.gson.JsonObject
import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.bean.weixin.WeixinLogMsg
import com.wangjia.bigdata.core.handler._
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.{LogEventId, LogType}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable


/**
  * 清洗WEB端日志
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --outPath   日志输出目录
  *
  */
class WeiXinJobClean extends EveryDaySparkJob {
  //页面最大停留时间
  private val PAGE_MAX_STAY_TIME: Long = 10 * 60 * 1000

  //应用信息
  private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null
  private var mapIdentifierAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

  override protected def init(): Unit = {
    super.init()
    val sc = SparkExtend.ctx

    //加载广播应用信息
    val mapAppInfos = JAInfoUtils.loadAppInfos

    mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
    val mapIdAppInfos = mutable.Map[String, AppInfo]()
    for ((k, v) <- mapAppInfos) {
      if (v.identifier != "")
        mapIdAppInfos.put(v.identifier, v)
    }
    mapIdentifierAppInfosBroadcast = sc.broadcast(mapIdAppInfos)
  }


  override protected def job(args: Array[String]): Unit = {
    val sc = SparkExtend.ctx
    //读取文件
    val linesRdd = sc.textFile(this.inPath)
    //清洗
    val clearBeanRdd = linesRdd.map(x => LogClearHandler.clearWeiXinLog(x, mapAppInfosBroadcast.value, mapIdentifierAppInfosBroadcast.value))
    //得到日志对象
    val logBeanRdd = clearBeanRdd.filter(_._1 == LogClearStatus.SUCCESS).map(_._2.asInstanceOf[WeixinLogMsg])
    //计算时间和会话
    val msgBeanRdd = logBeanRdd.groupBy(_.uuid)
      .flatMap(x => {
        val recentPageJson: JSONObject = new JSONObject()
        val beans: List[WeixinLogMsg] = x._2.toList.sortWith(_.time < _.time)
        //计算停留时间
        var pageBean: WeixinLogMsg = null
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
              pageBean.staytime = bean.time - pageBean.time
              pageBean = bean
            } else if (bean.logtype == LogType.EVENT && bean.subtype == LogEventId.APP_ENTER_BACKGROUND) {
              pageBean.staytime = bean.time - pageBean.time
              pageBean = null
            }
          }
        })
        //计算会话次数，停留时间
        var bSwitchover: Boolean = false
        var visitindex: Int = 1
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
          }
        })
        beans
      })
    /*
    clearBeanRdd.filter(_._1 == LogClearStatus.ERROR_NOT_IDENTIFIER).foreach(println)
    clearBeanRdd.map(x => (x._1, 1)).reduceByKey(_ + _).foreach(println)
    msgBeanRdd.map(x => ((x.appid, x.ja_version), 1)).reduceByKey(_ + _).foreach(println)
    msgBeanRdd.map(x=>((x.logtype,x.subtype),1)).reduceByKey(_+_).foreach(println)
    msgBeanRdd.filter(_.subtype=="__sys_user_login").foreach(println)
    */

    //写入文件
    msgBeanRdd.map(BeanHandler.toString).repartition(1).saveAsTextFile(this.outPath)

    //写入记录
    val mapClearStatusNum: Map[Int, Int] = clearBeanRdd.map(x => (x._1, 1)).reduceByKey(_ + _).collect().toMap
    println(mapClearStatusNum)
    val allLogNum: Int = mapClearStatusNum.toList.map(_._2).sum
    val rightLogNum: Int = mapClearStatusNum.getOrElse(LogClearStatus.SUCCESS, 0)
    val errorLogNum: Int = allLogNum - rightLogNum
    val time = (System.currentTimeMillis() - jobStartTime) / 1000
    JDBCBasicUtils.insert("sql_insert_log_job_clean", pstmt => {
      pstmt.setLong(1, allLogNum)
      pstmt.setLong(2, errorLogNum)
      pstmt.setLong(3, rightLogNum)
      pstmt.setString(4, inPath)
      pstmt.setString(5, outPath)
      pstmt.setString(6, jobName)
      pstmt.setLong(7, time.toLong)
      pstmt.setDate(8, new java.sql.Date(this.logDate.getTime))
      pstmt.setTimestamp(9, new Timestamp(System.currentTimeMillis()))
    })

    val json = new JsonObject
    mapClearStatusNum.foreach(b => json.addProperty(b._1.toString, b._2))
    this.setRunLog(json.toString)
  }
}

object WeiXinJobClean {

  def main(args: Array[String]) {
    val job = new WeiXinJobClean()
    job.run(args)
    println(System.currentTimeMillis() - job.jobStartTime)
  }

}
