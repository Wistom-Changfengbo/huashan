package com.wangjia.bigdata.core.job.stream

import com.wangjia.bigdata.core.bean.info.AppInfo
import com.wangjia.bigdata.core.bean.weixin.WeixinLogMsg
import com.wangjia.bigdata.core.common.Config
import com.wangjia.bigdata.core.handler.{LoadDataHandler, LogClearHandler, LogClearStatus}
import com.wangjia.bigdata.core.job.common.JobStreamFlew
import com.wangjia.bigdata.core.utils.{JAInfoUtils, StreamUtils}
import com.wangjia.common.LogType
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
  * Created by Cfb on 2018/5/4.
  */
class WeixinStreamFlew extends JobStreamFlew {
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

    override protected def job(args: Array[String]): Unit = {
        val kafkaParams = Map[String, Object](
            "session.timeout.ms" -> "40000",
            "request.timeout.ms" -> "50000",
            "group.id" -> "wj_weixin_flew_20180611"
        )
        val topics = Array("wx20180611")
        ssc.sparkContext.setLogLevel("ERROR")

        val lineDs: DStream[String] = readLineByKafka(ssc, topics, kafkaParams)

        val beanDS: DStream[WeixinLogMsg] = lineDs.map(x => LogClearHandler.clearWeiXinLog(x, mapAppInfos, mapIdAppInfos))
                .filter(_._1 == LogClearStatus.SUCCESS)
                .map(_._2.asInstanceOf[WeixinLogMsg])
                .filter(x => x != null && x.logtype == LogType.PAGE)

        StreamUtils.saveLogLastTime(beanDS.map(_.time),"MONITOR:WEIXIN:LOG:LASTTIME")

        val flewDS = beanDS.map(x => (x.appid, x)).groupByKey().map(x => {
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

object WeixinStreamFlew {
    def main(args: Array[String]) {
        val job = new WeixinStreamFlew
        job.run(args)
    }
}
