package com.wangjia.bigdata.core.job.stream

import com.wangjia.bigdata.core.bean.info.{AccountInfo, AppInfo}
import com.wangjia.bigdata.core.bean.web.WebLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{LoadDataHandler, LogClearHandler, LogClearStatus}
import com.wangjia.bigdata.core.job.common.JobStreamFlew
import com.wangjia.bigdata.core.utils.{JAInfoUtils, StreamUtils}
import com.wangjia.common.LogType
import com.wangjia.utils.JavaUtils
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
  * WEB端实时流量统计 按分钟、按小时
  *
  * Created by Administrator on 2017/7/5.
  */
class WebStreamFlew extends JobStreamFlew {
    //一个批次时间(S)
    BATCH_DURATION_SECONDS = 30L

    //加载广播帐号信息
    private val mapAccountInfos = new LoadDataHandler[mutable.Map[Int, AccountInfo]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadAccountInfos _)

    //加载广播应用信息
    private val mapAppInfos = new LoadDataHandler[mutable.Map[String, AppInfo]](Config.UPDATE_INFO_INTERVAL_TIME, JAInfoUtils.loadAppInfos _)

    private def filterBean(bean: WebLogMsg): Boolean = {
        if (bean == null)
            return false
        if (bean.logtype != LogType.PAGE)
            return false
        val timeMillis = System.currentTimeMillis()
        if (bean.time > timeMillis)
            return false
        if (!JavaUtils.isSameDate(bean.time, timeMillis))
            return false
        true
    }

    override protected def job(args: Array[String]): Unit = {
        val kafkaParams = Map[String, Object](
            "session.timeout.ms" -> "40000",
            "request.timeout.ms" -> "50000",
            "group.id" -> "wj_web_flew_20180611"
        )
        val topics = Array("web20180611")
        val lines = readLineByKafka(ssc, topics, kafkaParams)

        val beanDS: DStream[WebLogMsg] = lines.map(str => LogClearHandler.clearWebLog(str, mapAppInfos, mapAccountInfos))
                .filter(_._1 == LogClearStatus.SUCCESS)
                .map(_._2.asInstanceOf[WebLogMsg])
                .filter(filterBean)

        StreamUtils.saveLogLastTime(beanDS.map(_.time),"MONITOR:WEB:LOG:LASTTIME")

        val flewDS: DStream[(String, Flew)] = beanDS.map(x => (x.appid, x))
                .groupByKey()
                .map(x => {
                    val appid = x._1
                    val uuids = mutable.Map[String, Long]()
                    val ips = mutable.Map[String, Long]()
                    val pvs = mutable.ListBuffer[Long]()
                    var maxTime: Long = 0
                    x._2.foreach(b => {
                        uuids += ((b.uuid, b.time))
                        ips += ((b.ip, b.time))
                        pvs += b.time
                        if (maxTime < b.time)
                            maxTime = b.time
                    })
                    (appid, Flew(appid, uuids, ips, pvs, maxTime, 0))
                })

        flewJob(ssc, flewDS, 60L, "MINUTE")
        flewJob(ssc, flewDS, 3600L, "HOUR")
    }
}


object WebStreamFlew {
    def main(args: Array[String]) {
        val job = new WebStreamFlew
        job.run(args)
    }
}