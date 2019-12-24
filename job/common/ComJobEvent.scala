package com.wangjia.bigdata.core.job.common

import java.sql.Timestamp
import java.util.Properties

import com.wangjia.bigdata.core.handler.BeanHandler
import com.wangjia.bigdata.core.job.{EveryDaySparkJob, SparkExtend}
import com.wangjia.bigdata.core.utils.JDBCBasicUtils
import com.wangjia.common.{LogEventId, LogType, Platform}
import com.wangjia.hbase.conn.ExTBConnection
import com.wangjia.hbase.{ExHbase, HBaseConst, HBaseTableName}
import com.wangjia.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, _}


/**
  * 统计各个平台的事件信息
  *
  * --inPath    日志输入目录
  * --date      日志日期(yyyy-MM-dd)
  * --platform  平台:web,app，weixin
  *
  * Created by Cfb on 2018/2/26.
  */
case class ComEventBean(appid: String, logtype: String, subtype: String, time: Long, data: String, deviceid: String, uuid: String, ip: String, userid: String, url: String, ref: String, unionid: String)

class ComJobEvent extends EveryDaySparkJob {
    private var platform: String = ""

    private def loadWeb(inpath: String): RDD[ComEventBean] = {
        val beanRdd = SparkExtend.ctx.textFile(inpath).map(BeanHandler.toWebAcc).filter(x => {
            x != null && x.logtype == LogType.EVENT
        }).map(x => ComEventBean(x.appid, x.logtype, x.subtype, x.time, x.data, x.dStrInfo("ja_uuid"), x.uuid, x.ip, x.userid, x.url, x.ref, ""))
        beanRdd
    }

    private def loadApp(inpath: String): RDD[ComEventBean] = {
        val beanRdd = SparkExtend.ctx.textFile(inpath).map(BeanHandler.toAppAcc).filter(x => {
            if (x != null
                    && x.logtype == LogType.EVENT
                    && x.subtype != LogEventId.APP_LOCATION
                    && x.subtype != LogEventId.UI_CLICK_POINT
                    && x.subtype != LogEventId.APP_ENTER_BACKGROUND
                    && x.subtype != LogEventId.APP_ENTER_FOREGROUND)
                true
            else
                false
        }).map(x => ComEventBean(x.appid, x.logtype, x.subtype, x.time, x.data, x.dStrInfo("deviceid"), x.uuid, x.ip, x.userid, "", "", ""))
        beanRdd
    }

    private def loadWeiXin(inpath: String): RDD[ComEventBean] = {
        val beanRdd = SparkExtend.ctx.textFile(inpath).map(BeanHandler.toWeiXin).filter(x => {
            x != null && x.logtype == LogType.EVENT && x.logtype != LogEventId.APP_ENTER_BACKGROUND && x.logtype != LogEventId.APP_ENTER_FOREGROUND
        }).map(x => ComEventBean(x.appid, x.logtype, x.subtype, x.time, x.data, x.dStrInfo("ja_uuid"), x.uuid, x.ip, x.userid, "", "", x.dStrInfo("unionid")))
        beanRdd
    }

    /**
      * 任务入口方法
      *
      * @param args
      */
    override protected def job(args: Array[String]): Unit = {
        //平台参数
        platform = this.getParameterOrElse("platform", "app")
        val beanRdd = platform match {
            case Platform.PLATFORM_NAME_WEB => loadWeb(this.inPath)
            case Platform.PLATFORM_NAME_MOBILE => loadApp("G:\\LOG\\app\\clean\\")
            case Platform.PLATFORM_NAME_WEIXIN => loadWeiXin(this.inPath)
            case _ => throw new RuntimeException("no define platform")
        }
        beanRdd.take(100).foreach(x=>{
            println("logtype=="+x.logtype+"==subtype=="+x.subtype)
        })

        //保存详情到HBASE
        HBaseUtils.createTable(HBaseTableName.EVENT_DES)
//        beanRdd.foreachPartition(iterator => {
//            initHBase()
//            val conn = new ExTBConnection
//            val tb = conn.getTable(HBaseTableName.EVENT_DES)
//            while (iterator.hasNext) {
//                val bean = iterator.next()
//                val put = new Put(Bytes.toBytes(ExHbase.getEventDesKey(bean.appid, bean.logtype, bean.subtype, bean.time)))
//                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_TIME, Bytes.toBytes(bean.time))
//                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_MSG, Bytes.toBytes(bean.data))
//                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_DEVICEID, Bytes.toBytes(bean.deviceid))
//                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UUID, Bytes.toBytes(bean.uuid))
//                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_IP, Bytes.toBytes(bean.ip))
//                put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_USERID, Bytes.toBytes(bean.userid))
//                if (platform == Platform.PLATFORM_NAME_WEB) {
//                    put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_URL, Bytes.toBytes(bean.url))
//                    put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_REF, Bytes.toBytes(bean.ref))
//                } else if (platform == Platform.PLATFORM_NAME_WEIXIN) {
//                    put.addColumn(HBaseConst.BYTES_CF1, HBaseConst.BYTES_UNIONID, Bytes.toBytes(bean.unionid))
//                }
//
//                tb.addPut(put)
//            }
//            conn.close()
//        })

        val spark = SparkSession.builder().appName(this.jobName).getOrCreate()
        import spark.implicits._
        val beanDF = beanRdd.toDF
        beanDF.createOrReplaceTempView("t_commonLogSelf")

        //设置公共方法
        spark.udf.register("logDate", () => new java.sql.Date(this.logDate.getTime))
        spark.udf.register("addTime", () => new Timestamp(System.currentTimeMillis()))

        //按渠道统计PV UV IP
        val sql_event =
            """select appid,
              | logtype as etype,
              | subtype as sub_type,
              | count(*) as event_num,
              | count(distinct uuid) as user_num,
              | count(distinct ip) as ip_num,
              | logDate() as day,
              | addTime() as addtime from t_commonLogSelf group by appid,logtype,subtype """.stripMargin

        val eventDf: DataFrame = spark.sql(sql_event)

        //设置数据库配置
        val prop = new Properties
//        prop.setProperty("user", JDBCBasicUtils.getProperty("username"))
//        prop.setProperty("password", JDBCBasicUtils.getProperty("password"))

//        eventDf.write.mode(SaveMode.Append).jdbc(JDBCBasicUtils.getProperty("url"), "ja_data_event_everyday", prop)

        spark.stop()
    }
}

object ComJobEvent {
    def main(args: Array[String]) {
        val job = new ComJobEvent()
        job.run(args)
        println(System.currentTimeMillis() - job.jobStartTime)
    }
}
