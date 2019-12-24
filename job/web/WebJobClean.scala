package com.wangjia.bigdata.core.job.web

import java.sql.Timestamp

import com.google.gson.JsonObject
import com.wangjia.bigdata.core.bean.info.{AccountInfo, AppInfo}
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{BeanHandler, LogClearHandler, LogClearStatus}
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.{JAInfoUtils, JDBCBasicUtils}
import com.wangjia.common.LogType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
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
class WebJobClean extends EveryDaySparkJob {

    //页面最大停留时间
    private val PAGE_MAX_STAY_TIME: Long = 10 * 60 * 1000

    //应用信息
    private var mapAppInfosBroadcast: Broadcast[mutable.Map[String, AppInfo]] = null

    //帐号信息
    private var mapAccountInfosBroadcast: Broadcast[mutable.Map[Int, AccountInfo]] = null


    override protected def init(): Unit = {
        super.init()
        val sc = SparkExtend.ctx


        //加载广播帐号信息
        val mapAccountInfos = JAInfoUtils.loadAccountInfos
        mapAccountInfosBroadcast = sc.broadcast(mapAccountInfos)

        //加载广播应用信息
        val mapAppInfos = JAInfoUtils.loadAppInfos
        mapAppInfosBroadcast = sc.broadcast(mapAppInfos)
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

        //读取文件
//        val linesRdd = sc.textFile(this.inPath)
        val linesRdd = sc.textFile("G:\\LOG\\web\\log\\2018\\11\\01")
        val str = linesRdd.first()
        val any =LogClearHandler.clearWebLog(str, mapAppInfosBroadcast.value, mapAccountInfosBroadcast.value)
        //清洗
        val clearBeanRdd = linesRdd.map(x => LogClearHandler.clearWebLog(x, mapAppInfosBroadcast.value, mapAccountInfosBroadcast.value))
        clearBeanRdd
        //得到日志对象
        val logBeanRdd = clearBeanRdd.filter(_._1 == LogClearStatus.SUCCESS).map(_._2.asInstanceOf[WebLogMsg])
        //计算时间和会话
        val msgBeanRdd = logBeanRdd.groupBy(_.uuid)
                .flatMap(x => {
                    val uuid = x._1
                    val beans: List[WebLogMsg] = x._2.toList.sortWith(_.time < _.time)
                    //计算停留时间
                    var pageBean: WebLogMsg = null
                    beans.foreach(bean => {
                        if (pageBean == null && bean.logtype == LogType.PAGE) {
                            pageBean = bean
                        } else if (pageBean != null && bean.logtype == LogType.PAGE) {
                            pageBean.staytime = bean.time - pageBean.time
                            pageBean = bean
                        }
                    })
                    //计算会话次数，停留时间
                    var visitindex: Int = 1
                    beans.foreach(bean => {
                        bean.visitindex = visitindex
                        if (bean.staytime >= PAGE_MAX_STAY_TIME) {
                            bean.staytime = 0
                            visitindex += 1
                        }
                    })
                    beans
                })

        //写入文件
//        msgBeanRdd.map(BeanHandler.toString)
//                .repartition(1)
//                .saveAsTextFile(this.outPath)
        msgBeanRdd.map(BeanHandler.toString)
                .repartition(1)
                .saveAsTextFile("G:\\LOG\\web\\out")

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

object WebJobClean {
    def main(args: Array[String]) {
        val job = new WebJobClean()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
